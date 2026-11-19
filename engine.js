/* 모바일 모의매매 체결 엔진 (브라우저 안에서 돈다).
 *
 * 데스크탑용 engine.py와 같은 계산을 시장가 주문에 한해 옮긴 것이다.
 * - 격리 마진, 단방향 포지션. 매수/매도를 누르면 그 순간의 실제 호가를 먹어 들어간 평균가로 체결한다.
 * - 반대 방향 주문은 포지션을 줄이고, 더 크면 뒤집는다.
 * - 시장평가가가 청산가에 닿으면 격리 증거금을 전부 잃는다. 펀딩 시각이 지나면 펀딩을 주고받는다.
 * - 배율표가 있으면 종목별 최대 배율, 배율별 포지션 한도, 실제 유지증거금률을 쓴다.
 */
(function (root) {
  'use strict';
  const EPS = 1e-12, FEED_STALE_MS = 5000, KEEP = 3000, DEFAULT_LEVERAGE = 20, MAX_LEVERAGE = 125;
  const DEFAULTS = { taker_fee: 0.0005, mmr: 0.01, tp_pct: 0, sl_pct: 0 };

  class OrderError extends Error {}

  function decimals(unit) {
    const s = String(unit);
    if (s.includes('e-')) return +s.split('e-')[1];
    const i = s.indexOf('.');
    return i < 0 ? 0 : Math.max(0, s.replace(/0+$/, '').length - i - 1);
  }
  function floorStep(x, step) {
    const n = Math.floor(x / +step + 1e-9);
    return +(n * +step).toFixed(decimals(step));
  }
  const money = x => x.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function newState(balance, settings, leverage) {
    return {
      version: 1, start_balance: +balance, wallet: +balance,
      settings: Object.assign({}, DEFAULTS, settings || {}), leverage: Object.assign({}, leverage || {}),
      positions: {}, fills: [], trades: [], next_id: 1,
    };
  }

  class Engine {
    constructor(state, clock) {
      this.clock = clock || (() => Date.now());
      this.state = state || newState(10000);
      this.state.settings = Object.assign({}, DEFAULTS, this.state.settings || {});
      this.specs = {};      // symbol -> {tick, step, min_qty, min_notional, market_max_qty}
      this.books = {};      // symbol -> {bids, asks}  각 호가는 [가격, 수량]
      this.last = {};
      this.mark = {};
      this.funding = {};    // symbol -> {rate, next}
      this.brackets = {};   // symbol -> [[배율, 포지션 한도, 유지증거금률, 공제액], ...] 작은 구간부터
      this.feedMs = 0;
      this.events = [];
      this.dirty = false;
    }

    /* ---------------------------------------------------------------- 조회 */
    maxLeverage(symbol) {
      const t = this.brackets[symbol];
      return t ? Math.max(...t.map(x => x[0])) : MAX_LEVERAGE;
    }
    notionalCap(symbol, leverage) {
      const t = this.brackets[symbol];
      if (!t) return null;
      return Math.max(0, ...t.filter(x => x[0] >= leverage).map(x => x[1]));
    }
    tier(symbol, notional) {
      const t = this.brackets[symbol];
      if (!t) return [this.state.settings.mmr, 0];
      const hit = t.find(x => notional <= x[1]) || t[t.length - 1];
      return [hit[2], hit[3]];
    }
    leverage(symbol) {
      return Math.min(Math.trunc(this.state.leverage[symbol] || DEFAULT_LEVERAGE), this.maxLeverage(symbol));
    }
    available() {
      let used = 0;
      for (const s in this.state.positions) used += this.state.positions[s].margin;
      return this.state.wallet - used;
    }
    liqPrice(symbol, pos) {
      const q = Math.abs(pos.qty), [mmr, cum] = this.tier(symbol, pos.entry * q);
      if (pos.qty > 0) return Math.max(0, (pos.entry * q - pos.margin - cum) / (q * (1 - mmr)));
      return (pos.entry * q + pos.margin + cum) / (q * (1 + mmr));
    }
    /** 수수료(들어갈 때 + 나올 때)를 내고도 손해가 아닌 가격 */
    breakeven(pos) {
      const f = this.state.settings.taker_fee;
      return pos.qty > 0 ? pos.entry * (1 + f) / (1 - f) : pos.entry * (1 - f) / (1 + f);
    }
    /** 자동 익절·손절 가격. 진입가에서 설정한 비율만큼 움직인 가격이며, 꺼져 있으면 null */
    triggers(pos) {
      const s = this.state.settings, dir = pos.qty > 0 ? 1 : -1;
      return { tp: s.tp_pct > 0 ? pos.entry * (1 + dir * s.tp_pct) : null, sl: s.sl_pct > 0 ? pos.entry * (1 - dir * s.sl_pct) : null };
    }
    upnl(symbol, pos) {
      const price = this.mark[symbol] || this.last[symbol] || pos.entry;
      return (price - pos.entry) * pos.qty;
    }
    snapshot() {
      const s = this.state, positions = [];
      let total = 0;
      for (const symbol in s.positions) {
        const p = s.positions[symbol], u = this.upnl(symbol, p), q = Math.abs(p.qty), f = s.settings.taker_fee, t = this.triggers(p);
        const now = this.mark[symbol] || this.last[symbol] || p.entry;
        total += u;
        positions.push({
          symbol, side: p.side, qty: q, entry: p.entry, mark: this.mark[symbol], margin: p.margin,
          leverage: p.leverage, upnl: u, roe: p.margin ? u / p.margin : 0, liq: this.liqPrice(symbol, p), opened_ms: p.opened_ms,
          breakeven: this.breakeven(p), net: u - q * p.entry * f - q * now * f, tp: t.tp, sl: t.sl,
        });
      }
      return { wallet: s.wallet, equity: s.wallet + total, available: this.available(), upnl: total,
        start_balance: s.start_balance, settings: s.settings, positions };
    }

    /* ---------------------------------------------------------------- 내부 */
    spec(symbol) {
      const sp = this.specs[symbol];
      if (!sp) throw new OrderError(`${symbol}: 거래 가능한 종목 정보가 없습니다`);
      return sp;
    }
    book(symbol) {
      const b = this.books[symbol];
      if (!b || !b.bids.length || !b.asks.length || this.clock() - this.feedMs > FEED_STALE_MS)
        throw new OrderError(`${symbol}: 실시간 호가를 받지 못하고 있어 체결할 수 없습니다`);
      return b;
    }
    id() { return this.state.next_id++; }
    toast(level, msg) { this.events.push({ type: 'toast', level, msg }); }
    closingQty(symbol, side) {
      const pos = this.state.positions[symbol], sign = side === 'BUY' ? 1 : -1;
      return pos && pos.qty * sign < 0 ? Math.abs(pos.qty) : 0;
    }
    checkMargin(symbol, side, qty, price, feeRate) {
      const closing = Math.min(qty, this.closingQty(symbol, side)), opening = qty - closing;
      if (opening <= EPS) return;
      const pos = this.state.positions[symbol];
      let released = 0, realized = 0;
      if (closing) {
        released = pos.margin * closing / Math.abs(pos.qty);
        realized = (price - pos.entry) * closing * (pos.qty > 0 ? 1 : -1);
      }
      const lev = this.leverage(symbol), cap = this.notionalCap(symbol, lev);
      const held = pos && !closing ? Math.abs(pos.qty) : 0;
      if (cap != null && (held + opening) * price > cap + 1e-6)
        throw new OrderError(`${symbol} ${lev}배에서는 포지션을 ${Math.round(cap).toLocaleString('en-US')} USDT까지만 가질 수 있습니다`);
      const need = opening * price / lev + qty * price * feeRate;
      const have = this.available() + released + realized;
      if (need > have + 1e-9) throw new OrderError(`증거금 부족: 필요 ${money(need)} USDT, 사용 가능 ${money(Math.max(have, 0))} USDT`);
    }

    applyFill(symbol, side, qty, price, feeRate, reason) {
      const s = this.state, now = this.clock(), sp = this.spec(symbol), sign = side === 'BUY' ? 1 : -1;
      const fee = qty * price * feeRate;
      s.wallet -= fee;
      let pos = s.positions[symbol], realized = 0, closing = 0, closedTrade = null;
      if (pos && pos.qty * sign < 0) {
        const size = Math.abs(pos.qty);
        closing = Math.min(qty, size);
        realized = (price - pos.entry) * closing * (pos.qty > 0 ? 1 : -1);
        s.wallet += realized;
        pos.margin *= 1 - closing / size;
        pos.qty = +(pos.qty + sign * closing).toFixed(12);
        pos.realized += realized;
        pos.fees += fee * closing / qty;
        pos.exit_qty += closing;
        pos.exit_notional += closing * price;
        if (Math.abs(pos.qty) < +sp.step / 2) closedTrade = this.finish(symbol, pos, reason, now);
      }
      const opening = qty - closing;
      if (opening > EPS) {
        const lev = this.leverage(symbol);
        pos = s.positions[symbol];
        if (pos) {
          const size = Math.abs(pos.qty);
          pos.entry = (pos.entry * size + price * opening) / (size + opening);
          pos.qty = +(pos.qty + sign * opening).toFixed(12);
          pos.margin += opening * price / lev;
          pos.fees += fee * opening / qty;
        } else {
          pos = s.positions[symbol] = {
            side: sign > 0 ? 'LONG' : 'SHORT', qty: +(sign * opening).toFixed(12), entry: price, margin: opening * price / lev,
            leverage: lev, opened_ms: now, realized: 0, fees: fee * opening / qty, funding: 0, peak_margin: 0,
            exit_qty: 0, exit_notional: 0, entry_qty: 0, entry_notional: 0,
          };
        }
        pos.entry_qty += opening;
        pos.entry_notional += opening * price;
        pos.peak_margin = Math.max(pos.peak_margin, pos.margin);
      }
      const kind = closing && opening > EPS ? 'FLIP' : closing ? 'CLOSE' : 'OPEN';
      const fill = { id: this.id(), ms: now, symbol, side, qty, price, fee, realized, reason, kind };
      this.recordFill(fill);
      if (closedTrade) this.events.push({ type: 'trade', trade: closedTrade });
      return fill;
    }
    recordFill(fill) {
      const fills = this.state.fills;
      fills.push(fill);
      if (fills.length > KEEP) fills.splice(0, fills.length - KEEP);
      this.events.push({ type: 'fill', fill });
      this.dirty = true;
    }
    finish(symbol, pos, reason, now) {
      const s = this.state;
      delete s.positions[symbol];
      const net = pos.realized - pos.fees - pos.funding;
      const trade = {
        id: this.id(), symbol, side: pos.side, leverage: pos.leverage, qty: pos.entry_qty,
        entry: pos.entry_notional / pos.entry_qty, exit: pos.exit_notional / pos.exit_qty,
        gross: pos.realized, fees: pos.fees, funding: pos.funding, net,
        roe: pos.peak_margin ? net / pos.peak_margin : 0, opened_ms: pos.opened_ms, closed_ms: now, reason,
      };
      s.trades.push(trade);
      if (s.trades.length > KEEP) s.trades.splice(0, s.trades.length - KEEP);
      return trade;
    }

    /* ---------------------------------------------------------------- 주문 */
    /** 시장가 주문. size는 {margin} (증거금, 넣을 수 있는 만큼으로 줄여 넣음) 또는 {qty} (코인 수량 그대로). */
    market(symbol, side, size, reduceOnly, reason) {
      const sp = this.spec(symbol), s = this.state;
      if (side !== 'BUY' && side !== 'SELL') throw new OrderError('주문 형식이 잘못되었습니다');
      const book = this.book(symbol), levels = side === 'BUY' ? book.asks : book.bids, best = levels[0][0];
      const lev = this.leverage(symbol), taker = s.settings.taker_fee, closing = this.closingQty(symbol, side);
      let qty;
      if (reduceOnly) {
        if (!closing) throw new OrderError('정리할 포지션이 없습니다');
        qty = size && size.qty != null && size.qty < closing - EPS ? floorStep(size.qty, sp.step) : closing;
      } else if (size && size.qty != null) {
        qty = floorStep(size.qty, sp.step);
      } else {
        const margin = size && size.margin;
        if (!(margin > 0)) throw new OrderError('주문 크기를 정하세요');
        let affordable = Math.max(this.available(), 0) / (1 / lev + taker) * 0.995;
        const cap = this.notionalCap(symbol, lev), pos = s.positions[symbol];
        if (cap != null && !closing) affordable = Math.min(affordable, Math.max(0, cap * 0.995 - (pos ? Math.abs(pos.qty) * best : 0)));
        // 반대 포지션이 있으면 그만큼은 증거금 없이 줄일 수 있다
        qty = floorStep(Math.min(margin * lev, affordable + closing * best) / best, sp.step);
        if (sp.market_max_qty) qty = Math.min(qty, +sp.market_max_qty);
      }
      if (sp.market_max_qty && qty > +sp.market_max_qty)
        throw new OrderError(`${symbol} 시장가 주문은 한 번에 ${+sp.market_max_qty}개까지 넣을 수 있습니다`);
      if (qty <= 0 || (!reduceOnly && qty < +sp.min_qty)) throw new OrderError(`주문 수량이 최소 수량(${sp.min_qty})보다 작습니다`);
      if (!reduceOnly && qty * best < +sp.min_notional) throw new OrderError(`주문 금액이 최소 주문 금액(${+sp.min_notional} USDT)보다 작습니다`);
      let filled = 0, cost = 0;
      for (const [price, amount] of levels) {
        const take = Math.min(qty - filled, amount);
        filled += take;
        cost += take * price;
        if (qty - filled <= EPS) break;
      }
      let thin = false;
      if (filled < qty - EPS) { cost += (qty - filled) * levels[levels.length - 1][0]; filled = qty; thin = true; }
      const avg = cost / qty;
      this.checkMargin(symbol, side, qty, avg, taker);
      if (thin) this.toast('warn', `${symbol}: 주문이 받아 둔 호가보다 커서 나머지는 가장 불리한 호가로 체결했습니다`);
      return this.applyFill(symbol, side, qty, avg, taker, reason || (side === 'BUY' ? '매수' : '매도'));
    }
    /** 한 종목 정리. pct를 주면 그 비율만(예: 50) 닫는다. */
    close(symbol, reason, pct) {
      const pos = this.state.positions[symbol];
      if (!pos) throw new OrderError('정리할 포지션이 없습니다');
      let size = null;
      if (pct && pct < 100) {
        const part = floorStep(Math.abs(pos.qty) * pct / 100, this.spec(symbol).step);
        if (part <= 0) throw new OrderError('포지션이 작아 나눠서 정리할 수 없습니다');
        size = { qty: part };
      }
      return this.market(symbol, pos.qty > 0 ? 'SELL' : 'BUY', size, true, reason || '정리');
    }
    /** 일괄 정리: 열린 포지션을 전부 시장가로 닫는다. 호가가 없는 종목은 남기고 알려 준다. */
    closeAll() {
      const done = [], failed = [];
      for (const symbol of Object.keys(this.state.positions)) {
        try { done.push(this.close(symbol, '일괄 정리')); } catch (e) { failed.push(symbol); }
      }
      return { done, failed };
    }
    setLeverage(symbol, leverage) {
      this.spec(symbol);
      leverage = Math.trunc(leverage);
      const top = this.maxLeverage(symbol);
      if (!(leverage >= 1 && leverage <= top)) throw new OrderError(`${symbol}의 배율은 1~${top}배까지 가능합니다`);
      if (this.state.positions[symbol]) throw new OrderError('포지션이 있는 동안에는 배율을 바꿀 수 없습니다');
      this.state.leverage[symbol] = leverage;
      this.dirty = true;
    }
    updateSettings(values) {
      const limits = { taker_fee: [0, 0.01, '수수료는 0~1% 사이여야 합니다'], tp_pct: [0, 0.5, '익절은 0~50% 사이여야 합니다'], sl_pct: [0, 0.5, '손절은 0~50% 사이여야 합니다'] };
      for (const key in limits) {
        if (values[key] == null) continue;
        const v = +values[key], [lo, hi, why] = limits[key];
        if (!(v >= lo && v <= hi)) throw new OrderError(why);
        this.state.settings[key] = v;
      }
      this.dirty = true;
    }
    reset(balance) {
      balance = +balance;
      if (!(balance >= 10 && balance <= 1e9)) throw new OrderError('시작 잔고는 10 USDT 이상이어야 합니다');
      this.state = newState(balance, this.state.settings, this.state.leverage);
      this.dirty = true;
    }

    /* ---------------------------------------------------------------- 시세 입력 */
    onDepth(symbol, bids, asks) { this.books[symbol] = { bids, asks }; this.feedMs = this.clock(); }
    clearBooks() { this.books = {}; }
    onTrade(symbol, price) {
      this.last[symbol] = price;
      const pos = this.state.positions[symbol];
      if (!pos) return;
      const t = this.triggers(pos), long = pos.qty > 0;
      let hit = null;
      if (t.sl != null && (long ? price <= t.sl : price >= t.sl)) hit = '손절';
      else if (t.tp != null && (long ? price >= t.tp : price <= t.tp)) hit = '익절';
      if (hit) { try { this.close(symbol, hit); } catch (e) { /* 호가가 끊긴 동안에는 다음 체결에서 다시 시도 */ } }
    }
    onMark(symbol, mark, rate, nextMs) {
      this.mark[symbol] = mark;
      const pos = this.state.positions[symbol], prev = this.funding[symbol];
      if (pos && prev && nextMs && prev.next && nextMs > prev.next && prev.rate) this.payFunding(symbol, prev.rate, mark);
      if (nextMs) this.funding[symbol] = { rate: rate || 0, next: nextMs };
      const now = this.state.positions[symbol];
      if (!now) return;
      const liq = this.liqPrice(symbol, now);
      if ((now.qty > 0 && mark <= liq) || (now.qty < 0 && mark >= liq)) this.liquidate(symbol, now, liq);
    }
    payFunding(symbol, rate, mark) {
      const pos = this.state.positions[symbol];
      if (!pos || !rate) return;
      const pay = Math.abs(pos.qty) * mark * rate * (pos.qty > 0 ? 1 : -1);
      this.state.wallet -= pay;
      pos.funding += pay;
      this.recordFill({ id: this.id(), ms: this.clock(), symbol, side: pos.qty > 0 ? 'BUY' : 'SELL', qty: Math.abs(pos.qty),
        price: mark, fee: 0, realized: -pay, reason: '펀딩', kind: 'FUNDING' });
    }
    /** 앱을 닫아 둔 동안의 최저가·최고가로 강제청산 여부를 뒤늦게 판정한다. */
    settleGap(symbol, low, high) {
      const pos = this.state.positions[symbol];
      if (!pos) return false;
      const liq = this.liqPrice(symbol, pos);
      if ((pos.qty > 0 && low <= liq) || (pos.qty < 0 && high >= liq)) { this.liquidate(symbol, pos, liq); return true; }
      // 자동 손절·익절도 뒤늦게 판정한다. 둘 다 닿았으면 순서를 알 수 없으므로 손절로 본다
      const t = this.triggers(pos), long = pos.qty > 0, side = long ? 'SELL' : 'BUY';
      let hit = null;
      if (t.sl != null && (long ? low <= t.sl : high >= t.sl)) hit = ['손절', t.sl];
      else if (t.tp != null && (long ? high >= t.tp : low <= t.tp)) hit = ['익절', t.tp];
      if (hit) { this.applyFill(symbol, side, Math.abs(pos.qty), hit[1], this.state.settings.taker_fee, hit[0]); return true; }
      return false;
    }
    liquidate(symbol, pos, liq) {
      const s = this.state, now = this.clock(), size = Math.abs(pos.qty), loss = pos.margin;
      s.wallet -= loss;
      pos.realized -= loss;
      pos.exit_qty += size;
      pos.exit_notional += size * liq;
      const trade = this.finish(symbol, pos, '강제청산', now);
      this.recordFill({ id: this.id(), ms: now, symbol, side: pos.qty > 0 ? 'SELL' : 'BUY', qty: size, price: liq,
        fee: 0, realized: -loss, reason: '강제청산', kind: 'LIQ' });
      this.events.push({ type: 'trade', trade });
      this.toast('error', `${symbol} 포지션이 강제청산되었습니다 (증거금 ${money(loss)} USDT 손실)`);
    }
  }

  const api = { Engine, OrderError, newState, decimals, floorStep, DEFAULT_LEVERAGE };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.MP = api;
})(typeof window !== 'undefined' ? window : globalThis);
