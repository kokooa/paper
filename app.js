'use strict';
/* 원터치 모의매매 (아이폰용).
 * 서버 없이 이 화면이 바이낸스 선물 공개 시세를 직접 받아, 화면 안의 엔진(engine.js)으로 가상 계좌에 체결한다.
 * 계좌는 이 기기의 브라우저 저장소에 남는다. API 키는 쓰지 않는다. */

const { Engine, OrderError, decimals } = MP;
const $ = id => document.getElementById(id);
const store = {
  get(key, fallback) { try { const v = localStorage.getItem(key); return v == null ? fallback : JSON.parse(v); } catch (e) { return fallback; } },
  set(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); } catch (e) { /* 저장이 안 돼도 화면은 쓴다 */ } },
};
const dev = new URLSearchParams(location.search).get('dev');   // 개발용: 가짜 시세 서버 주소
const REST = dev ? `http://${dev}` : 'https://fapi.binance.com';
const WS_PUBLIC = dev ? `ws://${dev}/public/stream` : 'wss://fstream.binance.com/public/stream';
const WS_MARKET = dev ? `ws://${dev}/market/stream` : 'wss://fstream.binance.com/market/stream';
const IVS = { '1m': 60e3, '5m': 300e3, '15m': 900e3, '1h': 3600e3 };
const IV_KO = { '1m': '1분', '5m': '5분', '15m': '15분', '1h': '1시간' };
const TZ = -new Date().getTimezoneOffset() * 60;
const AMOUNTS = [5, 10, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000, 10000];
const C = { buy: '#0ecb81', sell: '#f6465d', warn: '#e0a23a', text: '#c9ced6' };

const engine = new Engine(store.get('mpm.state', null));
if (window.MP_BRACKETS) engine.brackets = window.MP_BRACKETS.t;

const S = {
  symbol: store.get('mpm.symbol', 'BTCUSDT'), iv: store.get('mpm.iv', '1m'), amount: store.get('mpm.amount', 100),
  fav: new Set(store.get('mpm.fav', [])), ready: false, bar: null, klKey: '', tick24: null, tickers: {}, tickersAt: 0,
  symTab: 'all', menuTab: 'sum', sound: store.get('mpm.sound', true), study: store.get('mpm.study', true), lines: [], lineSig: '', dirty: {}, hiddenAt: 0, busy: false, resetArmed: 0,
};
if (!IVS[S.iv]) S.iv = '1m';

/* ------------------------------------------------------------------ 표시 */
const spec = sym => engine.specs[sym] || { tick: '0.01', step: '0.001' };
const num = (x, d) => x == null || !isFinite(x) ? '–' : x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const price = (x, sym) => num(x, decimals(spec(sym || S.symbol).tick));
const qty = (x, sym) => x == null ? '–' : x.toLocaleString('en-US', { maximumFractionDigits: decimals(spec(sym || S.symbol).step) });
const usd = x => num(x, 2);
const signed = x => (x > 0 ? '+' : '') + usd(x);
const pct = x => (x > 0 ? '+' : '') + num(x * 100, 2) + '%';
const cls = x => x > 0 ? 'up' : x < 0 ? 'down' : '';
const coin = sym => sym.replace(/USDT$/, '');
const stamp = ms => { const d = new Date(ms), p = n => String(n).padStart(2, '0'); return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`; };
function toast(level, msg) {
  const el = document.createElement('div');
  el.className = 'toast ' + level;
  el.textContent = msg;
  const box = $('toasts');
  box.appendChild(el);
  while (box.children.length > 3) box.firstChild.remove();
  setTimeout(() => el.remove(), level === 'error' ? 5000 : 2200);
}
/* 체결 소리: 매수는 높은 음, 매도는 낮은 음, 정리는 두 음. 아이폰은 화면을 누른 뒤에만 소리를 낼 수 있다 */
let audio = null;
function beep(kind) {
  if (!S.sound) return;
  try {
    audio = audio || new (window.AudioContext || window.webkitAudioContext)();
    if (audio.state === 'suspended') audio.resume();
    const notes = { buy: [880], sell: [440], close: [660, 990], liq: [330, 220, 165], error: [180] }[kind] || [600];
    notes.forEach((freq, i) => {
      const osc = audio.createOscillator(), gain = audio.createGain(), t = audio.currentTime + i * 0.09;
      osc.type = kind === 'error' ? 'square' : 'sine';
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.0001, t);
      gain.gain.exponentialRampToValueAtTime(0.3, t + 0.012);
      gain.gain.exponentialRampToValueAtTime(0.0001, t + 0.085);
      osc.connect(gain); gain.connect(audio.destination);
      osc.start(t); osc.stop(t + 0.1);
    });
  } catch (e) { /* 소리를 못 내도 매매는 된다 */ }
}
async function getJSON(path) {
  const response = await fetch(REST + path);
  if (!response.ok) throw new Error('HTTP ' + response.status);
  return response.json();
}

/* ------------------------------------------------------------------ 바이낸스 실시간 연결 */
class Feed {
  constructor(url, onData) {
    this.url = url; this.onData = onData; this.want = new Set(); this.sub = new Set(); this.alive = false; this.n = 0; this.lastMsg = 0;
    this.connect();
  }
  connect() {
    clearTimeout(this.timer);
    const ws = this.ws = new WebSocket(this.url);
    ws.onopen = () => { if (this.ws !== ws) return; this.alive = true; this.sub = new Set(); this.lastMsg = Date.now(); this.sync(); status(); };
    ws.onmessage = e => { this.lastMsg = Date.now(); const m = JSON.parse(e.data); if (m.data) this.onData(m.data, m.stream || ''); };
    ws.onclose = () => { if (this.ws !== ws) return; this.alive = false; status(); this.timer = setTimeout(() => this.connect(), 1000); };
  }
  set(streams) { this.want = new Set(streams); this.sync(); }
  sync() {
    if (!this.alive || this.ws.readyState !== 1) return;
    const add = [...this.want].filter(x => !this.sub.has(x)), remove = [...this.sub].filter(x => !this.want.has(x));
    this.sub = new Set(this.want);
    if (remove.length) this.ws.send(JSON.stringify({ method: 'UNSUBSCRIBE', params: remove, id: ++this.n }));
    if (add.length) this.ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: add, id: ++this.n }));
  }
  restart() { const ws = this.ws; this.ws = null; this.alive = false; try { ws.close(); } catch (e) { /* 이미 닫힘 */ } this.connect(); }
  stale() { return this.alive && this.want.size > 0 && Date.now() - this.lastMsg > 8000; }
}
const pub = new Feed(WS_PUBLIC, onPublic);
const mkt = new Feed(WS_MARKET, onMarket);

function onPublic(d) {
  if (d.e !== 'depthUpdate') return;
  engine.onDepth(d.s, d.b.map(r => [+r[0], +r[1]]), d.a.map(r => [+r[0], +r[1]]));
  if (d.s === S.symbol) S.dirty.quote = true;
}
function onMarket(d) {
  if (d.e === 'aggTrade') {
    engine.onTrade(d.s, +d.p);
    if (d.s === S.symbol) { onBarTrade(+d.p, d.T, +d.q || 0); S.dirty.quote = true; }
  } else if (d.e === 'markPriceUpdate') {
    engine.onMark(d.s, +d.p, +d.r || 0, +d.T || 0);
    after();
  } else if (d.e === '24hrMiniTicker' && d.s === S.symbol) {
    S.tick24 = [+d.c, +d.o];
    S.dirty.quote = true;
  }
}
function subscribe() {
  if (!S.ready) return;
  const held = Object.keys(engine.state.positions), all = new Set([S.symbol, ...held]);
  pub.set([...all].map(s => s.toLowerCase() + '@depth20@100ms'));
  const low = S.symbol.toLowerCase();
  mkt.set([low + '@aggTrade', low + '@miniTicker', ...[...all].map(s => s.toLowerCase() + '@markPrice@1s')]);
}
function status() {
  let text = '';
  if (!S.ready) text = S.specError ? '바이낸스에서 종목 정보를 받지 못했습니다. 인터넷 연결을 확인하세요. 다시 시도하는 중…' : '';
  else if (!pub.alive || !mkt.alive) text = '실시간 시세 연결이 끊겨 다시 연결하는 중입니다. 그동안은 체결되지 않습니다.';
  $('banner').hidden = !text;
  $('banner').textContent = text;
}
setInterval(() => { if (pub.stale()) pub.restart(); if (mkt.stale()) mkt.restart(); }, 3000);   // 아이폰에서 조용히 죽은 연결 되살리기

/* ------------------------------------------------------------------ 종목 정보 */
function applySpecs(table) {
  engine.specs = {};
  for (const sym in table) { const r = table[sym]; engine.specs[sym] = { tick: r[0], step: r[1], min_qty: r[2], min_notional: r[3], market_max_qty: r[4] }; }
}
async function loadSpecs() {
  const cached = store.get('mpm.specs', null);
  if (cached && Date.now() - cached.t < 12 * 3600e3) return applySpecs(cached.d);
  try {
    const info = await getJSON('/fapi/v1/exchangeInfo'), table = {};
    for (const it of info.symbols) {
      if (it.contractType !== 'PERPETUAL' || it.status !== 'TRADING' || it.quoteAsset !== 'USDT') continue;
      const f = {};
      for (const x of it.filters) f[x.filterType] = x;
      table[it.symbol] = [f.PRICE_FILTER.tickSize, f.LOT_SIZE.stepSize, f.LOT_SIZE.minQty, (f.MIN_NOTIONAL || {}).notional || '5', (f.MARKET_LOT_SIZE || {}).maxQty || null];
    }
    store.set('mpm.specs', { t: Date.now(), d: table });
    applySpecs(table);
  } catch (e) {
    if (!cached) throw e;
    applySpecs(cached.d);   // 새로 받지 못하면 전에 받아 둔 것으로
  }
}
async function loadTickers() {
  if (Date.now() - S.tickersAt < 30000) return;
  const rows = await getJSON('/fapi/v1/ticker/24hr');
  S.tickers = {};
  for (const r of rows) if (engine.specs[r.symbol]) S.tickers[r.symbol] = [+r.lastPrice, +r.openPrice, +r.quoteVolume];
  S.tickersAt = Date.now();
}

/* ------------------------------------------------------------------ 차트 */
const chart = LightweightCharts.createChart($('chart'), {
  autoSize: true,
  layout: { background: { color: '#161a21' }, textColor: '#8b94a3', fontSize: 11 },
  grid: { vertLines: { color: '#1d222b' }, horzLines: { color: '#1d222b' } },
  rightPriceScale: { borderColor: '#252b35' },
  timeScale: { borderColor: '#252b35', timeVisible: true, secondsVisible: false, rightOffset: 4 },
  crosshair: { mode: 0 },
  localization: { locale: 'ko-KR' },
});
const candles = chart.addCandlestickSeries({ upColor: C.buy, downColor: C.sell, borderVisible: false, wickUpColor: C.buy, wickDownColor: C.sell });
candles.priceScale().applyOptions({ scaleMargins: { top: 0.06, bottom: 0.2 } });
const volume = chart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: 'vol', lastValueVisible: false, priceLineVisible: false });
chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.84, bottom: 0 } });
const MAS = [[7, '#e0a23a'], [25, '#c069e0'], [99, '#3fbf9f']].map(([n, color]) => ({
  n, line: chart.addLineSeries({ color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }),
}));
const volColor = b => (b.close >= b.open ? 'rgba(14,203,129,.45)' : 'rgba(246,70,93,.45)');
function showStudy() {
  volume.applyOptions({ visible: S.study });
  MAS.forEach(m => m.line.applyOptions({ visible: S.study }));
  candles.priceScale().applyOptions({ scaleMargins: { top: 0.06, bottom: S.study ? 0.2 : 0.06 } });
}
function average(n) { const c = S.closes, k = c.length; if (k < n) return null; let sum = 0; for (let i = k - n; i < k; i++) sum += c[i]; return sum / n; }
showStudy();
async function loadKlines() {
  const key = S.symbol + '|' + S.iv;
  S.klKey = key; S.bar = null;
  try {
    const rows = await getJSON(`/fapi/v1/klines?symbol=${S.symbol}&interval=${S.iv}&limit=400`);
    if (S.klKey !== key || !rows.length) return;
    const sp = spec(S.symbol);
    candles.applyOptions({ priceFormat: { type: 'price', precision: decimals(sp.tick), minMove: +sp.tick } });
    candles.setData(rows.map(r => ({ time: r[0] / 1000 + TZ, open: +r[1], high: +r[2], low: +r[3], close: +r[4] })));
    const bars = rows.map(r => ({ time: r[0] / 1000 + TZ, open: +r[1], close: +r[4], value: +r[5] }));
    volume.setData(bars.map(b => ({ time: b.time, value: b.value, color: volColor(b) })));
    for (const m of MAS) {
      const out = [];
      let sum = 0;
      bars.forEach((b, i) => { sum += b.close; if (i >= m.n) sum -= bars[i - m.n].close; if (i >= m.n - 1) out.push({ time: b.time, value: sum / m.n }); });
      m.line.setData(out);
    }
    S.closes = bars.map(b => b.close);
    const r = rows[rows.length - 1];
    S.bar = { t: r[0], open: +r[1], high: +r[2], low: +r[3], close: +r[4], v: +r[5] };
    S.firstBar = rows[0][0];
    chart.timeScale().scrollToRealTime();
    drawMarkers();
  } catch (e) { toast('error', '캔들을 불러오지 못했습니다'); }
}
function onBarTrade(p, T, q) {
  const bar = S.bar;
  if (!bar) return;
  const ms = IVS[S.iv], t = Math.floor(T / ms) * ms;
  if (t > bar.t) { S.bar = { t, open: p, high: p, low: p, close: p, v: q }; S.closes.push(p); if (S.closes.length > 600) S.closes.shift(); }
  else if (t === bar.t) { bar.close = p; bar.v += q; if (p > bar.high) bar.high = p; if (p < bar.low) bar.low = p; S.closes[S.closes.length - 1] = p; }
  else return;
  S.dirty.bar = true;
}
/* 내가 누른 매수·매도를 차트에 화살표로 남긴다 */
function drawMarkers() {
  if (!S.bar) return;
  const ms = IVS[S.iv], marks = [];
  for (const f of engine.state.fills) {
    if (f.symbol !== S.symbol || f.kind === 'FUNDING' || f.ms < S.firstBar) continue;
    const buy = f.side === 'BUY';
    marks.push({ time: Math.floor(f.ms / ms) * ms / 1000 + TZ, position: buy ? 'belowBar' : 'aboveBar', color: buy ? C.buy : C.sell, shape: buy ? 'arrowUp' : 'arrowDown' });
  }
  candles.setMarkers(marks);
}
function drawLines() {
  const p = engine.state.positions[S.symbol], want = [];
  if (p) {
    want.push([p.entry, p.side === 'LONG' ? '매수 진입' : '매도 진입', p.side === 'LONG' ? C.buy : C.sell]);
    want.push([engine.liqPrice(S.symbol, p), '청산가', C.warn]);
  }
  const sig = S.symbol + JSON.stringify(want);
  if (sig === S.lineSig) return;
  S.lineSig = sig;
  S.lines.forEach(l => candles.removePriceLine(l));
  S.lines = want.map(([p0, title, color]) => candles.createPriceLine({ price: p0, title, color, lineWidth: 1, lineStyle: 0, axisLabelVisible: true }));
}
$('ivs').innerHTML = Object.keys(IVS).map(iv => `<button data-iv="${iv}">${IV_KO[iv]}</button>`).join('');
function markIv() { for (const b of $('ivs').children) b.classList.toggle('on', b.dataset.iv === S.iv); }
$('ivs').addEventListener('click', e => {
  const iv = e.target.dataset.iv;
  if (!iv || iv === S.iv) return;
  S.iv = iv; store.set('mpm.iv', iv); markIv(); loadKlines();
});
markIv();

/* ------------------------------------------------------------------ 주문 */
function after() {
  if (engine.events.length) {
    const events = engine.events; engine.events = [];
    let sound = null;
    for (const ev of events) {
      if (ev.type === 'toast') toast(ev.level, ev.msg);
      else if (ev.type === 'trade') scheduleSync();
      else if (ev.type === 'fill') {
        const f = ev.fill, pnl = f.kind === 'OPEN' ? '' : ` · ${signed(f.realized)}`;
        toast(f.kind === 'LIQ' ? 'error' : f.side === 'BUY' ? 'b' : 's', `${coin(f.symbol)} ${f.reason} ${qty(f.qty, f.symbol)} @ ${price(f.price, f.symbol)}${pnl}`);
        if (f.kind === 'LIQ') sound = 'liq';
        else if (f.kind !== 'FUNDING' && sound !== 'liq') sound = /정리/.test(f.reason) ? 'close' : sound === 'close' ? 'close' : f.side === 'BUY' ? 'buy' : 'sell';
      }
    }
    if (sound) beep(sound);
    drawMarkers();
  }
  if (engine.dirty) {
    engine.dirty = false;
    store.set('mpm.state', engine.state);
    subscribe();
    S.dirty.account = true;
  }
}
/* 주문이 받아 둔 호가 20단계보다 크면 체결 직전에 실제 호가 1,000단계를 받아 온다 */
async function deepen(symbol, side, estQty) {
  const book = engine.books[symbol];
  if (!book) return;
  const levels = side === 'BUY' ? book.asks : book.bids;
  if (estQty <= levels.reduce((sum, l) => sum + l[1], 0) * 0.95) return;
  try {
    const deep = await getJSON(`/fapi/v1/depth?symbol=${symbol}&limit=1000`);
    engine.onDepth(symbol, deep.bids.map(r => [+r[0], +r[1]]), deep.asks.map(r => [+r[0], +r[1]]));
  } catch (e) { /* 못 받으면 20단계로 체결 */ }
}
async function tap(side) {
  if (S.busy || !S.ready) return;
  S.busy = true;
  const button = $(side === 'BUY' ? 'btnBuy' : 'btnSell');
  try {
    const book = engine.books[S.symbol];
    if (book && book.asks.length) await deepen(S.symbol, side, S.amount * engine.leverage(S.symbol) / book.asks[0][0]);
    engine.market(S.symbol, side, { margin: S.amount });
    button.classList.remove('flash'); void button.offsetWidth; button.classList.add('flash');
  } catch (e) {
    toast('error', e instanceof OrderError ? e.message : '주문을 처리하지 못했습니다');
    beep('error');
  }
  S.busy = false;
  after();
}
$('btnBuy').addEventListener('click', () => tap('BUY'));
$('btnSell').addEventListener('click', () => tap('SELL'));
async function closeOne(pct) {
  const p = engine.state.positions[S.symbol];
  if (S.busy || !p) return;
  S.busy = true;
  try {
    await deepen(S.symbol, p.qty > 0 ? 'SELL' : 'BUY', Math.abs(p.qty) * (pct || 100) / 100);
    engine.close(S.symbol, pct ? '절반 정리' : '정리', pct);
  } catch (e) { toast('error', e instanceof OrderError ? e.message : '정리하지 못했습니다'); beep('error'); }
  S.busy = false;
  after();
}
$('btnHalf').addEventListener('click', () => closeOne(50));
$('btnCloseOne').addEventListener('click', () => closeOne());
$('btnCloseAll').addEventListener('click', async () => {
  if (S.busy) return;
  S.busy = true;
  for (const sym of Object.keys(engine.state.positions)) {
    const p = engine.state.positions[sym];
    await deepen(sym, p.qty > 0 ? 'SELL' : 'BUY', Math.abs(p.qty));
  }
  const result = engine.closeAll();
  if (result.failed.length) toast('error', `${result.failed.map(coin).join(', ')}: 호가를 받지 못해 정리하지 못했습니다. 다시 눌러 주세요`);
  S.busy = false;
  after();
});

/* ------------------------------------------------------------------ 금액 · 배율 */
function stepAmount(dir) {
  const next = dir > 0 ? AMOUNTS.find(a => a > S.amount) : [...AMOUNTS].reverse().find(a => a < S.amount);
  if (next == null) return;
  S.amount = next; store.set('mpm.amount', next); S.dirty.account = true;
}
$('amtMinus').addEventListener('click', () => stepAmount(-1));
$('amtPlus').addEventListener('click', () => stepAmount(1));
$('amtBtn').addEventListener('click', () => {
  const top = engine.maxLeverage(S.symbol), lev = engine.leverage(S.symbol);
  $('amtInput').value = S.amount;
  $('levRange').max = top; $('levRange').value = lev; $('levNow').textContent = lev;
  $('levMax').textContent = `${coin(S.symbol)} 최대 ${top}배`;
  $('amtChips').innerHTML = [10, 50, 100, 500, 1000].map(a => `<button data-a="${a}">${a}</button>`).join('');
  $('levChips').innerHTML = [1, 5, 10, 20, 50, 75, 100, 125].filter(l => l <= top).map(l => `<button data-l="${l}">${l}배</button>`).join('');
  amtNote();
  openSheet('sheetAmt');
});
function amtNote() {
  const a = parseFloat($('amtInput').value) || 0, lev = +$('levRange').value, cap = engine.notionalCap(S.symbol, lev);
  $('amtNote').textContent = `한 번 누르면 약 ${usd(a * lev)} USDT어치를 삽니다.` + (cap != null ? ` 이 배율의 포지션 한도는 ${num(cap, 0)} USDT입니다.` : '') +
    (engine.state.positions[S.symbol] ? ' 포지션이 있는 동안에는 배율을 바꿀 수 없습니다.' : '');
}
$('amtInput').addEventListener('input', amtNote);
$('levRange').addEventListener('input', e => { $('levNow').textContent = e.target.value; amtNote(); });
$('amtChips').addEventListener('click', e => { if (e.target.dataset.a) { $('amtInput').value = e.target.dataset.a; amtNote(); } });
$('levChips').addEventListener('click', e => { if (e.target.dataset.l) { $('levRange').value = e.target.dataset.l; $('levNow').textContent = e.target.dataset.l; amtNote(); } });
$('amtOk').addEventListener('click', () => {
  const a = parseFloat($('amtInput').value), lev = +$('levRange').value;
  if (!(a > 0)) return toast('error', '증거금을 입력하세요');
  try {
    if (lev !== engine.leverage(S.symbol)) engine.setLeverage(S.symbol, lev);
  } catch (e) { toast('error', e.message); }
  S.amount = a; store.set('mpm.amount', a);
  S.lineSig = '';
  closeSheets(); after(); S.dirty.account = true;
});

/* ------------------------------------------------------------------ 시트 */
function openSheet(id) { closeSheets(); $(id).hidden = false; $('backdrop').hidden = false; }
function closeSheets() { for (const s of document.querySelectorAll('.sheet')) s.hidden = true; $('backdrop').hidden = true; document.activeElement && document.activeElement.blur(); }
$('backdrop').addEventListener('click', closeSheets);
for (const b of document.querySelectorAll('[data-close]')) b.addEventListener('click', closeSheets);

/* 종목 고르기 */
function setSymbol(sym) {
  S.symbol = sym; store.set('mpm.symbol', sym);
  $('sym').textContent = coin(sym);
  S.tick24 = S.tickers[sym] ? S.tickers[sym].slice(0, 2) : null;
  S.lineSig = '';
  subscribe(); loadKlines();
  S.dirty.quote = S.dirty.account = true;
}
function renderSymbols() {
  const q = $('search').value.trim().toUpperCase(), held = engine.state.positions;
  let syms = Object.keys(engine.specs);
  if (S.symTab === 'fav') syms = syms.filter(s => S.fav.has(s));
  if (S.symTab === 'pos') syms = syms.filter(s => held[s]);
  if (q) syms = syms.filter(s => s.includes(q));
  const vol = s => (S.tickers[s] ? S.tickers[s][2] : 0);
  syms.sort((a, b) => vol(b) - vol(a) || (a < b ? -1 : 1));
  if (q) syms.sort((a, b) => b.startsWith(q) - a.startsWith(q));
  $('symList').innerHTML = syms.slice(0, 300).map(s => {
    const t = S.tickers[s], c = t && t[1] ? t[0] / t[1] - 1 : null;
    return `<div class="row${s === S.symbol ? ' on' : ''}" data-s="${s}"><button class="star${S.fav.has(s) ? ' fav' : ''}">★</button>` +
      `<span class="nm">${coin(s)}<small>USDT</small>${held[s] ? ' ●' : ''}</span><span class="pr">${t ? price(t[0], s) : ''}</span><span class="ch ${cls(c)}">${c == null ? '' : pct(c)}</span></div>`;
  }).join('') || `<p class="empty">${S.symTab === 'fav' && !q ? '★를 눌러 즐겨찾기에 추가하세요' : S.symTab === 'pos' && !q ? '보유 중인 종목이 없습니다' : '해당하는 종목이 없습니다'}</p>`;
  for (const b of $('symTabs').children) b.classList.toggle('on', b.dataset.t === S.symTab);
}
$('btnSym').addEventListener('click', () => {
  $('search').value = '';
  renderSymbols();
  openSheet('sheetSym');
  loadTickers().then(renderSymbols).catch(() => {});
});
$('search').addEventListener('input', renderSymbols);
$('symTabs').addEventListener('click', e => { if (e.target.dataset.t) { S.symTab = e.target.dataset.t; renderSymbols(); } });
$('symList').addEventListener('click', e => {
  const row = e.target.closest('.row');
  if (!row) return;
  const sym = row.dataset.s;
  if (e.target.classList.contains('star')) {
    S.fav.has(sym) ? S.fav.delete(sym) : S.fav.add(sym);
    store.set('mpm.fav', [...S.fav]);
    if (S.symTab === 'fav') renderSymbols(); else e.target.classList.toggle('fav');
  } else { closeSheets(); if (sym !== S.symbol) setSymbol(sym); }
});

/* 날짜별 손익 요약. 하루는 이 기기의 현지 날짜 기준이고, 거래는 닫힌 날에 넣는다 */
const dayKey = ms => { const d = new Date(ms), p = n => String(n).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
function daily() {
  const days = {};
  for (const t of engine.state.trades) {
    const d = days[dayKey(t.closed_ms)] = days[dayKey(t.closed_ms)] || { n: 0, wins: 0, gross: 0, fees: 0, funding: 0, net: 0, best: null, worst: null, by: {} };
    d.n++; if (t.net > 0) d.wins++;
    d.gross += t.gross; d.fees += t.fees; d.funding += t.funding; d.net += t.net;
    if (!d.best || t.net > d.best.net) d.best = t;
    if (!d.worst || t.net < d.worst.net) d.worst = t;
    const s = d.by[t.symbol] = d.by[t.symbol] || { n: 0, net: 0 };
    s.n++; s.net += t.net;
  }
  return days;
}
function todayLine() {
  const d = daily()[dayKey(Date.now())];
  return d ? `오늘 ${d.n}회 · 순손익 <b class="${cls(d.net)}">${signed(d.net)}</b> USDT` : '포지션 없음';
}
function renderSummary(snap) {
  const days = daily(), key = dayKey(Date.now()), d = days[key], total = snap.equity - snap.start_balance;
  let html = `<div class="big"><label>오늘 순손익 (종료된 거래)</label><b class="${cls(d ? d.net : 0)}">${d ? signed(d.net) : '0.00'}</b><span>USDT · 수수료와 펀딩을 뺀 금액</span></div>`;
  if (d) {
    html += `<div class="kv"><div><label>거래</label><b>${d.n}회</b></div><div><label>승률</label><b>${num(d.wins / d.n * 100, 1)}%</b></div><div><label>평균</label><b class="${cls(d.net)}">${signed(d.net / d.n)}</b></div>` +
      `<div><label>매매 손익</label><b class="${cls(d.gross)}">${signed(d.gross)}</b></div><div><label>수수료</label><b>-${usd(d.fees)}</b></div><div><label>펀딩</label><b>${signed(-d.funding)}</b></div>` +
      `<div><label>가장 좋은 거래</label><b class="${cls(d.best.net)}">${signed(d.best.net)}</b></div><div><label>가장 나쁜 거래</label><b class="${cls(d.worst.net)}">${signed(d.worst.net)}</b></div><div><label>미실현</label><b class="${cls(snap.upnl)}">${signed(snap.upnl)}</b></div></div>`;
    html += '<div class="sec">오늘 종목별</div>' + Object.entries(d.by).sort((a, b) => b[1].net - a[1].net)
      .map(([sym, s]) => `<div class="line"><b>${coin(sym)}</b><span>${s.n}회</span><b class="${cls(s.net)}">${signed(s.net)}</b></div>`).join('');
  } else html += '<p class="empty">오늘 종료된 거래가 없습니다</p>';
  const keys = Object.keys(days).sort().reverse().slice(0, 30);
  if (keys.length) html += '<div class="sec">날짜별</div>' + keys.map(k => `<div class="line"><b>${k.slice(5)}${k === key ? ' (오늘)' : ''}</b><span>${days[k].n}회 · 승률 ${num(days[k].wins / days[k].n * 100, 0)}%</span><b class="${cls(days[k].net)}">${signed(days[k].net)}</b></div>`).join('');
  html += `<div class="stats">시작 잔고 ${usd(snap.start_balance)} → 총자산 ${usd(snap.equity)} · <b class="${cls(total)}">${signed(total)} (${pct(total / snap.start_balance)})</b></div>`;
  $('menuSum').innerHTML = html;
}

/* 메뉴: 요약 · 포지션 · 기록 · 설정 */
function renderMenu() {
  for (const b of $('menuTabs').children) b.classList.toggle('on', b.dataset.t === S.menuTab);
  $('menuSum').hidden = S.menuTab !== 'sum'; $('menuPos').hidden = S.menuTab !== 'pos'; $('menuHist').hidden = S.menuTab !== 'hist'; $('menuSet').hidden = S.menuTab !== 'set';
  const snap = engine.snapshot();
  if (S.menuTab === 'sum') renderSummary(snap);
  else if (S.menuTab === 'pos') {
    $('menuPos').innerHTML = snap.positions.map(p => `<div class="card" data-s="${p.symbol}">
      <div class="t">${coin(p.symbol)}<small class="${p.side === 'LONG' ? 'L' : 'S'}">${p.side === 'LONG' ? '매수' : '매도'} ${p.leverage}배</small></div>
      <div class="r"><b class="${cls(p.upnl)}">${signed(p.upnl)}</b><span class="${cls(p.upnl)}">${pct(p.roe)}</span><br><button data-close-pos>정리</button></div>
      <div class="d">${qty(p.qty, p.symbol)}개 · 진입 ${price(p.entry, p.symbol)} · 청산가 ${price(p.liq, p.symbol)} · 증거금 ${usd(p.margin)}</div></div>`).join('') ||
      '<p class="empty">열린 포지션이 없습니다</p>';
  } else if (S.menuTab === 'hist') {
    const trades = engine.state.trades, wins = trades.filter(t => t.net > 0).length, net = trades.reduce((sum, t) => sum + t.net, 0);
    $('menuHist').innerHTML = (trades.length ? `<div class="stats">종료 ${trades.length}회 · 승률 ${num(wins / trades.length * 100, 1)}% · 순손익 <b class="${cls(net)}">${signed(net)}</b> USDT (수수료·펀딩 포함)</div>` : '') +
      (trades.slice().reverse().map(t => `<div class="card"><div class="t">${coin(t.symbol)}<small class="${t.side === 'LONG' ? 'L' : 'S'}">${t.side === 'LONG' ? '매수' : '매도'} ${t.leverage}배</small></div>
      <div class="r"><b class="${cls(t.net)}">${signed(t.net)}</b><span class="${cls(t.net)}">${pct(t.roe)}</span></div>
      <div class="d">${stamp(t.closed_ms)} · ${price(t.entry, t.symbol)} → ${price(t.exit, t.symbol)} · ${t.reason}</div></div>`).join('') || '<p class="empty">종료된 거래가 없습니다</p>');
  } else {
    $('setFee').value = +(snap.settings.taker_fee * 100).toFixed(4);
    $('setBalance').value = snap.start_balance;
    $('setSound').checked = S.sound; $('setStudy').checked = S.study;
    $('ghRepo').value = G.repo; $('ghToken').value = G.token;
    ghStatus();
    armReset(false);
    const b = window.MP_BRACKETS;
    $('about').textContent = b ? `종목별 최대 배율·유지증거금률: 바이낸스 배율표 ${new Date(b.updated).toLocaleDateString('ko-KR')} 기준 (${Object.keys(b.t).length}개 종목)` : '배율표가 없어 최대 125배, 유지증거금률 1%로 계산합니다.';
  }
}
$('btnMenu').addEventListener('click', () => { renderMenu(); openSheet('sheetMenu'); });
$('info').addEventListener('click', () => { S.menuTab = Object.keys(engine.state.positions).length ? 'pos' : 'sum'; renderMenu(); openSheet('sheetMenu'); });
$('menuTabs').addEventListener('click', e => { if (e.target.dataset.t) { S.menuTab = e.target.dataset.t; renderMenu(); } });
$('menuPos').addEventListener('click', async e => {
  const card = e.target.closest('.card');
  if (!card) return;
  const sym = card.dataset.s;
  if (e.target.hasAttribute('data-close-pos')) {
    const p = engine.state.positions[sym];
    try { if (p) { await deepen(sym, p.qty > 0 ? 'SELL' : 'BUY', Math.abs(p.qty)); engine.close(sym); } } catch (err) { toast('error', err.message); }
    after(); renderMenu();
  } else { closeSheets(); if (sym !== S.symbol) setSymbol(sym); }
});
$('setOk').addEventListener('click', () => {
  try { engine.updateSettings({ taker_fee: parseFloat($('setFee').value) / 100 }); toast('', '저장했습니다'); } catch (e) { toast('error', e.message); }
  S.sound = $('setSound').checked; store.set('mpm.sound', S.sound);
  S.study = $('setStudy').checked; store.set('mpm.study', S.study);
  showStudy(); if (S.study) loadKlines();
  beep('buy');
  after();
});

/* 기록 보관: 파일 내보내기·가져오기 */
const csvCell = v => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
/* 기록의 시각은 데스크탑 기록과 같게 이 기기의 현지 시각으로 적는다 */
const iso = ms => { const d = new Date(ms), p = (n, w) => String(n).padStart(w || 2, '0'); return `${dayKey(ms)} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`; };
function toCsv(rows, columns) { return '\ufeff' + columns.join(',') + '\n' + rows.map(r => columns.map(c => csvCell(r[c])).join(',')).join('\n') + '\n'; }
const monthOf = ms => dayKey(ms).slice(0, 7);
function tradesCsv(month) {
  return toCsv(engine.state.trades.filter(t => monthOf(t.closed_ms) === month).map(t => Object.assign({}, t, { closed: iso(t.closed_ms), opened: iso(t.opened_ms) })),
    ['closed', 'opened', 'id', 'symbol', 'side', 'leverage', 'qty', 'entry', 'exit', 'gross', 'fees', 'funding', 'net', 'roe', 'reason']);
}
function fillsCsv(month) {
  return toCsv(engine.state.fills.filter(f => monthOf(f.ms) === month).map(f => Object.assign({}, f, { time: iso(f.ms) })),
    ['time', 'id', 'symbol', 'side', 'kind', 'reason', 'qty', 'price', 'fee', 'realized']);
}
function restore(state) {
  if (!state || typeof state.wallet !== 'number' || !state.positions || !Array.isArray(state.trades)) throw new Error('모의매매 계좌 파일이 아닙니다');
  engine.state = state;
  engine.state.settings = Object.assign({ taker_fee: 0.0005, mmr: 0.01 }, state.settings || {});
  engine.dirty = true; S.lineSig = '';
  after(); drawMarkers();
}
$('btnExport').addEventListener('click', async () => {
  const name = `paper-${dayKey(Date.now())}.json`, text = JSON.stringify(engine.state);
  try {
    const file = new File([text], name, { type: 'application/json' });
    if (navigator.canShare && navigator.canShare({ files: [file] })) return await navigator.share({ files: [file] });
  } catch (e) { if (e && e.name === 'AbortError') return; }
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([text], { type: 'application/json' }));
  a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 5000);
});
$('btnImport').addEventListener('click', () => $('importFile').click());
$('importFile').addEventListener('change', async e => {
  const file = e.target.files[0];
  e.target.value = '';
  if (!file) return;
  try { restore(JSON.parse(await file.text())); toast('', '파일에서 계좌를 가져왔습니다'); closeSheets(); } catch (err) { toast('error', '가져오지 못했습니다: ' + err.message); }
});

/* GitHub에 자동 저장: 사용자가 넣은 토큰으로 그 저장소의 paper/ 폴더에 올린다. 토큰은 이 기기에만 둔다 */
const G = Object.assign({ repo: '', token: '', at: 0, error: '' }, store.get('mpm.gh', {}), { sha: {}, busy: false, timer: 0 });
const b64 = text => { const bytes = new TextEncoder().encode(text); let bin = ''; for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000)); return btoa(bin); };
function ghFetch(path, options) {
  return fetch(`https://api.github.com/repos/${G.repo}/contents/${path}`, Object.assign({ cache: 'no-store',
    headers: { Authorization: 'Bearer ' + G.token, Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' } }, options));
}
const ghWhy = status => ({ 401: '토큰이 올바르지 않거나 만료되었습니다', 403: '토큰에 이 저장소의 쓰기 권한(Contents)이 없습니다', 404: '저장소를 찾을 수 없습니다. 이름과 토큰의 저장소 권한을 확인하세요' }[status] || 'GitHub 오류 ' + status);
async function ghPut(path, text) {
  let sha = G.sha[path];
  if (sha === undefined) {
    const r = await ghFetch(path);
    if (r.status === 200) sha = (await r.json()).sha;
    else if (r.status === 404) sha = null;   // 아직 없는 파일
    else throw new Error(ghWhy(r.status));
  }
  const body = { message: '모의매매 기록 ' + iso(Date.now()), content: b64(text) };
  if (sha) body.sha = sha;
  const r = await ghFetch(path, { method: 'PUT', body: JSON.stringify(body) });
  if (!r.ok) { delete G.sha[path]; throw new Error(ghWhy(r.status)); }
  G.sha[path] = (await r.json()).content.sha;
}
function ghRemember() { store.set('mpm.gh', { repo: G.repo, token: G.token, at: G.at, error: G.error, month: G.month }); }
function ghStatus() {
  $('ghStatus').textContent = !G.repo || !G.token ? '연결되어 있지 않습니다.' : G.busy ? '저장하는 중…' : G.error ? '저장 실패: ' + G.error : G.at ? `마지막 저장 ${stamp(G.at)} · ${G.repo}/paper` : '연결됨. 아직 저장한 적이 없습니다.';
}
async function ghSync() {
  clearTimeout(G.timer); G.timer = 0;
  if (!G.repo || !G.token || G.busy) return false;
  G.busy = true; ghStatus();
  try {
    // 기록은 달마다 다른 파일에 올린다. 지난달 파일은 그대로 남으므로 이 기기에서 오래된 기록이 밀려나도 GitHub에는 남는다
    const month = monthOf(Date.now()), months = G.month && G.month !== month ? [G.month, month] : [month];
    await ghPut('paper/state.json', JSON.stringify(engine.state));
    for (const m of months) {
      await ghPut(`paper/trades-${m}.csv`, tradesCsv(m));
      await ghPut(`paper/fills-${m}.csv`, fillsCsv(m));
    }
    G.month = month; G.at = Date.now(); G.error = '';
  } catch (e) { G.error = e.message || '연결하지 못했습니다'; }
  G.busy = false; ghRemember(); ghStatus();
  return !G.error;
}
function scheduleSync() { if (!G.repo || !G.token) return; clearTimeout(G.timer); G.timer = setTimeout(ghSync, 5000); }
function ghRead() {
  G.repo = $('ghRepo').value.trim().replace(/^https:\/\/github\.com\//, '').replace(/\.git$/, '').replace(/\/$/, '');
  G.token = $('ghToken').value.trim();
  G.sha = {};
  if (!/^[\w.-]+\/[\w.-]+$/.test(G.repo) || !G.token) { toast('error', '저장소(아이디/저장소)와 토큰을 입력하세요'); return false; }
  ghRemember();
  return true;
}
$('ghSave').addEventListener('click', async () => { if (ghRead()) toast(await ghSync() ? '' : 'error', G.error ? '저장 실패: ' + G.error : 'GitHub에 저장했습니다'); });
$('ghLoad').addEventListener('click', async () => {
  if (!ghRead()) return;
  if (!$('ghLoad').dataset.armed) {
    $('ghLoad').dataset.armed = '1'; $('ghLoad').textContent = '한 번 더: 지금 계좌를 덮어씁니다';
    setTimeout(() => { $('ghLoad').dataset.armed = ''; $('ghLoad').textContent = 'GitHub에서 가져오기'; }, 4000);
    return;
  }
  try {
    const r = await ghFetch('paper/state.json', { headers: { Authorization: 'Bearer ' + G.token, Accept: 'application/vnd.github.raw+json', 'X-GitHub-Api-Version': '2022-11-28' } });
    if (!r.ok) throw new Error(r.status === 404 ? '저장된 기록이 없습니다' : ghWhy(r.status));
    restore(JSON.parse(await r.text()));
    toast('', 'GitHub에서 계좌를 가져왔습니다'); closeSheets();
  } catch (e) { toast('error', '가져오지 못했습니다: ' + e.message); }
});
function armReset(on) {
  clearTimeout(S.resetArmed);
  $('btnReset').dataset.armed = on ? '1' : '';
  $('btnReset').textContent = on ? '한 번 더 누르면 초기화됩니다' : '계좌 초기화';
  if (on) S.resetArmed = setTimeout(() => armReset(false), 4000);
}
$('btnReset').addEventListener('click', () => {
  if (!$('btnReset').dataset.armed) return armReset(true);
  try { engine.reset(parseFloat($('setBalance').value)); toast('', '계좌를 초기화했습니다'); closeSheets(); } catch (e) { toast('error', e.message); }
  armReset(false); S.lineSig = ''; after(); drawMarkers();
});

/* ------------------------------------------------------------------ 그리기 */
function renderQuote() {
  const book = engine.books[S.symbol], last = engine.last[S.symbol];
  $('bid').textContent = book && book.bids.length ? price(book.bids[0][0]) : '–';
  $('ask').textContent = book && book.asks.length ? price(book.asks[0][0]) : '–';
  const shown = last != null ? last : S.tick24 ? S.tick24[0] : null;
  $('last').textContent = price(shown);
  const c = S.tick24 && S.tick24[1] && shown != null ? shown / S.tick24[1] - 1 : null;
  $('chg').textContent = c == null ? '' : pct(c) + ' (24시간)';
  $('chg').className = cls(c);
  $('last').className = cls(c);
}
function renderAccount() {
  const a = engine.snapshot(), lev = engine.leverage(S.symbol);
  $('equity').textContent = usd(a.equity);
  $('avail').textContent = usd(a.available);
  $('upnl').textContent = signed(a.upnl); $('upnl').className = cls(a.upnl);
  $('amt').textContent = S.amount.toLocaleString('en-US');
  $('amtSub').textContent = `USDT ×${lev}배`;
  const p = a.positions.find(x => x.symbol === S.symbol), others = a.positions.length - (p ? 1 : 0);
  $('posline').innerHTML = p
    ? `<b class="${p.side === 'LONG' ? 'up' : 'down'}">${p.side === 'LONG' ? '매수' : '매도'} ${qty(p.qty)}</b> · 진입 <b>${price(p.entry)}</b> · 청산가 ${price(p.liq)} · <b class="${cls(p.upnl)}">${signed(p.upnl)} (${pct(p.roe)})</b>${others ? ` · 외 ${others}종목` : ''}`
    : others ? `다른 종목에 포지션 ${others}개 · 여기를 눌러 보기` : todayLine();
  const n = a.positions.length;
  $('btnCloseAll').disabled = !n;
  $('closeInfo').textContent = n ? `${n}종목 · ${signed(a.upnl)}` : '';
  $('btnHalf').disabled = $('btnCloseOne').disabled = !p;
  drawLines();
}
let lastAccount = 0;
function frame(now) {
  const d = S.dirty;
  if (d.quote) { d.quote = false; renderQuote(); }
  if (d.bar && S.bar) {
    d.bar = false;
    const b = S.bar, time = b.t / 1000 + TZ;
    candles.update({ time, open: b.open, high: b.high, low: b.low, close: b.close });
    if (S.study) {
      volume.update({ time, value: b.v, color: volColor(b) });
      for (const m of MAS) { const v = average(m.n); if (v != null) m.line.update({ time, value: v }); }
    }
  }
  if (d.account || now - lastAccount > 250) { d.account = false; lastAccount = now; renderAccount(); }
  requestAnimationFrame(frame);
}

/* ------------------------------------------------------------------ 닫아 둔 동안 정산 */
async function catchUp() {
  const since = store.get('mpm.seen', 0), held = Object.keys(engine.state.positions);
  if (!since || !held.length || Date.now() - since < 15000) return;
  const gap = Date.now() - since, iv = gap < 900 * 60e3 ? '1m' : gap < 900 * 3600e3 ? '1h' : '1d';
  for (const sym of held) {
    delete engine.funding[sym];   // 아래에서 직접 정산하므로 실시간 쪽에서 한 번 더 내지 않게 한다
    try {
      const rows = await getJSON(`/fapi/v1/markPriceKlines?symbol=${sym}&interval=${iv}&startTime=${since}&limit=1000`);
      if (rows.length && engine.settleGap(sym, Math.min(...rows.map(r => +r[3])), Math.max(...rows.map(r => +r[2])))) continue;
      const paid = await getJSON(`/fapi/v1/fundingRate?symbol=${sym}&startTime=${since}&limit=100`);
      for (const f of paid) engine.payFunding(sym, +f.fundingRate, +f.markPrice || engine.state.positions[sym].entry);
    } catch (e) { toast('warn', `${coin(sym)}: 닫아 둔 동안의 시세를 확인하지 못했습니다`); }
  }
  after();
}
function seen() { if (S.ready && pub.alive && !document.hidden) store.set('mpm.seen', Date.now()); }
setInterval(seen, 3000);
document.addEventListener('visibilitychange', () => {
  if (document.hidden) { seen(); S.hiddenAt = Date.now(); if (G.timer) { clearTimeout(G.timer); G.timer = 0; ghSync(); } return; }
  if (!S.ready || Date.now() - S.hiddenAt < 3000) return;
  engine.clearBooks();              // 낡은 호가로 체결하지 않는다
  pub.restart(); mkt.restart();
  loadKlines();
  catchUp();
});

/* ------------------------------------------------------------------ 시작 */
async function start() {
  $('sym').textContent = coin(S.symbol);
  requestAnimationFrame(frame);
  for (;;) {
    try { await loadSpecs(); break; } catch (e) { S.specError = true; status(); await new Promise(r => setTimeout(r, 3000)); }
  }
  S.specError = false;
  if (!engine.specs[S.symbol]) S.symbol = 'BTCUSDT';
  S.ready = true;
  status();
  setSymbol(S.symbol);
  catchUp();
  loadTickers().then(() => { if (!S.tick24 && S.tickers[S.symbol]) { S.tick24 = S.tickers[S.symbol].slice(0, 2); S.dirty.quote = true; } }).catch(() => {});
}
start();
