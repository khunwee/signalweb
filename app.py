from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict
import yfinance as yf
import pandas as pd
import threading
import time
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')

# Use eventlet for production
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Bangkok timezone (UTC+7)
BANGKOK_TZ = timezone(timedelta(hours=7))

def get_bangkok_time():
    return datetime.now(BANGKOK_TZ)

def format_bangkok_time(fmt="%H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

def format_bangkok_datetime(fmt="%Y-%m-%d %H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

# ============================================================
# CONFIGURATION
# ============================================================
class Config:
    SCAN_INTERVAL_SECONDS = 15
    MIN_CONFLUENCE_SCORE = 80
    MIN_RR_RATIO = 2.5
    MAX_DRAWDOWN_DOLLARS = 10.0
    
    SYMBOLS = {
        "GOLD": {"yf": "GC=F", "name": "Gold Futures", "emoji": "🥇", "pip": 0.1},
        "SILVER": {"yf": "SI=F", "name": "Silver Futures", "emoji": "🥈", "pip": 0.01},
        "OIL": {"yf": "CL=F", "name": "Crude Oil", "emoji": "🛢️", "pip": 0.01},
    }

# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class Signal:
    signal_id: str
    symbol: str
    direction: str
    strength: str
    entry_price: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    risk_dollars: float
    reward_tp1: float
    reward_tp2: float
    reward_tp3: float
    risk_reward: float
    confluence_score: int
    reasons: List[str]
    session: str
    timestamp: str
    timestamp_unix: float

class Store:
    def __init__(self):
        self.prices: Dict[str, Dict] = {}
        self.signals: Dict[str, Signal] = {}
        self.history: List[Signal] = []
        self.last_scan = "Never"
        self.scan_count = 0
        self.connected_clients = 0

store = Store()

# ============================================================
# SIGNAL ANALYZER
# ============================================================
class SignalAnalyzer:
    def get_price(self, symbol: str) -> Optional[Dict]:
        yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", "GC=F")
        try:
            ticker = yf.Ticker(yf_symbol)
            hist = ticker.history(period="1d", interval="1m")
            if hist.empty:
                hist = ticker.history(period="1d", interval="5m")
            if hist.empty:
                return None
            latest = hist.iloc[-1]
            prev = hist.iloc[-2] if len(hist) > 1 else latest
            change = latest["Close"] - prev["Close"]
            return {
                "symbol": symbol,
                "price": round(float(latest["Close"]), 2),
                "high": round(float(hist["High"].max()), 2),
                "low": round(float(hist["Low"].min()), 2),
                "open": round(float(hist.iloc[0]["Open"]), 2),
                "change": round(change, 2),
                "change_pct": round((change / prev["Close"]) * 100, 3),
                "time": format_bangkok_time(),
                "volume": int(hist["Volume"].sum()) if "Volume" in hist.columns else 0
            }
        except Exception as e:
            print(f"Price error {symbol}: {e}")
            return None
    
    def get_candles(self, symbol: str, interval="15m", period="5d"):
        yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", "GC=F")
        try:
            df = yf.Ticker(yf_symbol).history(period=period, interval=interval)
            if df.empty or len(df) < 50:
                return None
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            print(f"Candle error {symbol}: {e}")
            return None
    
    def calculate_indicators(self, df):
        df = df.copy()
        for p in [9, 21, 50, 200]:
            df[f"ema{p}"] = df["close"].ewm(span=p, adjust=False).mean()
        
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / (loss + 0.0001)
        df["rsi"] = 100 - (100 / (1 + rs))
        
        ema12 = df["close"].ewm(span=12, adjust=False).mean()
        ema26 = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = ema12 - ema26
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        
        high, low, close = df["high"], df["low"], df["close"].shift(1)
        tr = pd.concat([high-low, (high-close).abs(), (low-close).abs()], axis=1).max(axis=1)
        df["atr"] = tr.rolling(14).mean()
        
        plus_dm = high.diff().where(lambda x: x > 0, 0)
        minus_dm = (-low.diff()).where(lambda x: x > 0, 0)
        atr14 = tr.rolling(14).mean()
        plus_di = 100 * (plus_dm.rolling(14).mean() / (atr14 + 0.0001))
        minus_di = 100 * (minus_dm.rolling(14).mean() / (atr14 + 0.0001))
        dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 0.0001))
        df["adx"] = dx.rolling(14).mean()
        df["plus_di"] = plus_di
        df["minus_di"] = minus_di
        
        df["bb_mid"] = df["close"].rolling(20).mean()
        bb_std = df["close"].rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * bb_std
        df["bb_lower"] = df["bb_mid"] - 2 * bb_std
        
        df["swing_high"] = df["high"].rolling(10).max()
        df["swing_low"] = df["low"].rolling(10).min()
        
        return df
    
    def check_session(self):
        hour = get_bangkok_time().hour
        if 19 <= hour <= 23:
            return "LONDON-NY OVERLAP ⭐⭐", True, 15
        elif 14 <= hour <= 23:
            return "LONDON SESSION ⭐", True, 10
        elif 19 <= hour or hour <= 4:
            return "NEW YORK SESSION", True, 8
        elif 6 <= hour <= 14:
            return "ASIA SESSION", True, 5
        return "OFF-PEAK", False, 0
    
    def generate_signal(self, symbol: str) -> Optional[Signal]:
        htf = self.get_candles(symbol, "1h", "1mo")
        ltf = self.get_candles(symbol, "15m", "5d")
        
        if htf is None or ltf is None:
            return None
        
        htf = self.calculate_indicators(htf)
        ltf = self.calculate_indicators(ltf)
        
        latest = ltf.iloc[-1]
        htf_latest = htf.iloc[-1]
        
        score, reasons = 0, []
        direction = None
        
        # HTF Trend
        if htf_latest["ema9"] > htf_latest["ema21"] > htf_latest["ema50"]:
            if htf_latest["ema50"] > htf_latest.get("ema200", htf_latest["ema50"]):
                score += 30
                reasons.append("✅ HTF Strong Bullish")
            else:
                score += 20
                reasons.append("✅ HTF Bullish")
            direction = "BUY"
        elif htf_latest["ema9"] < htf_latest["ema21"] < htf_latest["ema50"]:
            if htf_latest["ema50"] < htf_latest.get("ema200", htf_latest["ema50"]):
                score += 30
                reasons.append("✅ HTF Strong Bearish")
            else:
                score += 20
                reasons.append("✅ HTF Bearish")
            direction = "SELL"
        else:
            return None
        
        # LTF Alignment
        ltf_aligned = False
        if direction == "BUY" and latest["ema9"] > latest["ema21"]:
            score += 15
            ltf_aligned = True
            reasons.append("✅ LTF Aligned")
        elif direction == "SELL" and latest["ema9"] < latest["ema21"]:
            score += 15
            ltf_aligned = True
            reasons.append("✅ LTF Aligned")
        
        if not ltf_aligned:
            return None
        
        # RSI
        rsi = latest["rsi"]
        if direction == "BUY":
            if 30 <= rsi <= 50:
                score += 15
                reasons.append(f"✅ RSI {rsi:.0f}")
            elif rsi < 30:
                score += 10
                reasons.append(f"⚠️ RSI {rsi:.0f}")
        else:
            if 50 <= rsi <= 70:
                score += 15
                reasons.append(f"✅ RSI {rsi:.0f}")
            elif rsi > 70:
                score += 10
                reasons.append(f"⚠️ RSI {rsi:.0f}")
        
        # MACD
        if direction == "BUY" and latest["macd_hist"] > 0:
            score += 10
            if latest["macd_hist"] > ltf.iloc[-2]["macd_hist"]:
                score += 5
                reasons.append("✅ MACD ↑")
            else:
                reasons.append("✅ MACD")
        elif direction == "SELL" and latest["macd_hist"] < 0:
            score += 10
            if latest["macd_hist"] < ltf.iloc[-2]["macd_hist"]:
                score += 5
                reasons.append("✅ MACD ↓")
            else:
                reasons.append("✅ MACD")
        
        # ADX
        adx = latest["adx"]
        if adx > 25:
            score += 10
            reasons.append(f"✅ ADX {adx:.0f}")
        elif adx > 20:
            score += 5
            reasons.append(f"✅ ADX {adx:.0f}")
        
        # DI
        if direction == "BUY" and latest["plus_di"] > latest["minus_di"]:
            score += 5
            reasons.append("✅ +DI")
        elif direction == "SELL" and latest["minus_di"] > latest["plus_di"]:
            score += 5
            reasons.append("✅ -DI")
        
        # Session
        session, is_good, session_score = self.check_session()
        if is_good:
            score += session_score
            reasons.append(f"✅ {session}")
        
        if score < Config.MIN_CONFLUENCE_SCORE:
            return None
        
        # Calculate levels
        entry = round(latest["close"], 2)
        atr = latest["atr"]
        
        if direction == "BUY":
            swing_sl = latest["swing_low"] - atr * 0.3
            atr_sl = entry - atr * 1.5
            sl = round(max(swing_sl, atr_sl, entry - Config.MAX_DRAWDOWN_DOLLARS), 2)
            risk = entry - sl
            tp1 = round(entry + risk * 1.5, 2)
            tp2 = round(entry + risk * 2.5, 2)
            tp3 = round(entry + risk * 4.0, 2)
        else:
            swing_sl = latest["swing_high"] + atr * 0.3
            atr_sl = entry + atr * 1.5
            sl = round(min(swing_sl, atr_sl, entry + Config.MAX_DRAWDOWN_DOLLARS), 2)
            risk = sl - entry
            tp1 = round(entry - risk * 1.5, 2)
            tp2 = round(entry - risk * 2.5, 2)
            tp3 = round(entry - risk * 4.0, 2)
        
        reward_tp1 = abs(tp1 - entry)
        reward_tp2 = abs(tp2 - entry)
        reward_tp3 = abs(tp3 - entry)
        
        rr = reward_tp2 / risk if risk > 0 else 0
        if rr < Config.MIN_RR_RATIO:
            return None
        
        if score >= 90:
            strength = "🔥 STRONG"
        elif score >= 80:
            strength = "⭐ STANDARD"
        else:
            strength = "📊 WEAK"
        
        return Signal(
            signal_id=f"{symbol}_{direction}_{get_bangkok_time().strftime('%H%M%S')}",
            symbol=symbol,
            direction=direction,
            strength=strength,
            entry_price=entry,
            stop_loss=sl,
            tp1=tp1, tp2=tp2, tp3=tp3,
            risk_dollars=round(risk, 2),
            reward_tp1=round(reward_tp1, 2),
            reward_tp2=round(reward_tp2, 2),
            reward_tp3=round(reward_tp3, 2),
            risk_reward=round(rr, 2),
            confluence_score=score,
            reasons=reasons,
            session=session,
            timestamp=format_bangkok_datetime(),
            timestamp_unix=time.time()
        )

analyzer = SignalAnalyzer()

# ============================================================
# BACKGROUND SCANNER
# ============================================================
def background_scanner():
    while True:
        try:
            store.scan_count += 1
            store.last_scan = format_bangkok_time()
            
            for symbol in Config.SYMBOLS.keys():
                price = analyzer.get_price(symbol)
                if price:
                    old_price = store.prices.get(symbol, {}).get("price", 0)
                    store.prices[symbol] = price
                    
                    socketio.emit('price_update', {
                        'symbol': symbol,
                        'data': price,
                        'flash': abs(price['price'] - old_price) > 0.01
                    })
                
                signal = analyzer.generate_signal(symbol)
                if signal:
                    old_signal = store.signals.get(symbol)
                    is_new = (not old_signal or 
                              old_signal.direction != signal.direction or
                              time.time() - old_signal.timestamp_unix > 1800)
                    
                    if is_new:
                        store.signals[symbol] = signal
                        store.history.insert(0, signal)
                        store.history = store.history[:100]
                        
                        socketio.emit('new_signal', {
                            'symbol': symbol,
                            'signal': asdict(signal),
                            'is_new': True
                        })
                        print(f"🎯 [{format_bangkok_time()}] NEW: {symbol} {signal.direction} @ ${signal.entry_price}")
            
            socketio.emit('scan_update', {
                'scan_count': store.scan_count,
                'last_scan': store.last_scan,
                'connected': store.connected_clients,
                'bangkok_time': format_bangkok_datetime()
            })
                        
        except Exception as e:
            print(f"Scanner error: {e}")
        
        time.sleep(Config.SCAN_INTERVAL_SECONDS)

# ============================================================
# WEBSOCKET EVENTS
# ============================================================
@socketio.on('connect')
def handle_connect():
    store.connected_clients += 1
    print(f"✅ [{format_bangkok_time()}] Client connected ({store.connected_clients})")
    
    emit('initial_state', {
        'prices': store.prices,
        'signals': {k: asdict(v) for k, v in store.signals.items()},
        'history': [asdict(s) for s in store.history[:20]],
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'bangkok_time': format_bangkok_datetime()
    })

@socketio.on('disconnect')
def handle_disconnect():
    store.connected_clients -= 1
    print(f"❌ [{format_bangkok_time()}] Client disconnected ({store.connected_clients})")

# ============================================================
# HTML TEMPLATE
# ============================================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 Signal Dashboard - Bangkok Time</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}.pulse{animation:pulse 1s infinite}
        @keyframes flash-green{0%{background:#10b981}100%{background:transparent}}.flash-up{animation:flash-green .5s ease-out}
        @keyframes flash-red{0%{background:#ef4444}100%{background:transparent}}.flash-down{animation:flash-red .5s ease-out}
        @keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}.slide-in{animation:slideIn .5s ease-out}
        @keyframes glow{0%,100%{box-shadow:0 0 5px #10b981}50%{box-shadow:0 0 30px #10b981}}.glow{animation:glow 1.5s infinite}
        .scrollbar-thin::-webkit-scrollbar{width:6px}.scrollbar-thin::-webkit-scrollbar-track{background:#1f2937}.scrollbar-thin::-webkit-scrollbar-thumb{background:#4b5563;border-radius:3px}
    </style>
</head>
<body class="bg-gray-900 text-white min-h-screen">
    <div class="container mx-auto px-4 py-6 max-w-7xl">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4">
            <div>
                <h1 class="text-2xl md:text-3xl font-bold">🎯 Precision Signal Dashboard</h1>
                <p class="text-gray-400 text-sm">High-accuracy signals • Bangkok Time (UTC+7)</p>
            </div>
            <div class="flex items-center gap-4 text-sm">
                <div id="bangkokClock" class="bg-gray-800 px-4 py-2 rounded-lg font-mono text-xl">🇹🇭 --:--:--</div>
                <span id="connectionStatus" class="text-red-400">● Connecting...</span>
            </div>
        </div>
        
        <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
            <div class="bg-gray-800 rounded-lg p-3 text-center"><div class="text-gray-400 text-xs">Scans</div><div id="scanCount" class="text-xl font-bold">0</div></div>
            <div class="bg-gray-800 rounded-lg p-3 text-center"><div class="text-gray-400 text-xs">Active Signals</div><div id="signalCount" class="text-xl font-bold text-green-400">0</div></div>
            <div class="bg-gray-800 rounded-lg p-3 text-center"><div class="text-gray-400 text-xs">Last Scan</div><div id="lastScan" class="text-lg font-mono">--:--:--</div></div>
            <div class="bg-gray-800 rounded-lg p-3 text-center"><div class="text-gray-400 text-xs">Session</div><div id="sessionInfo" class="text-sm font-bold text-yellow-400">-</div></div>
            <div class="bg-gray-800 rounded-lg p-3 text-center"><div class="text-gray-400 text-xs">Clients</div><div id="clientCount" class="text-xl font-bold">0</div></div>
        </div>
        
        <div id="alertArea" class="hidden mb-6">
            <div class="bg-gradient-to-r from-green-900 to-green-800 border-2 border-green-500 rounded-xl p-4 glow">
                <div class="flex items-center justify-between">
                    <div class="flex items-center gap-3">
                        <span class="text-4xl">🚨</span>
                        <div><div id="alertTitle" class="text-xl font-bold">NEW SIGNAL!</div><div id="alertText" class="text-green-300"></div></div>
                    </div>
                    <button onclick="document.getElementById('alertArea').classList.add('hidden')" class="text-gray-400 hover:text-white text-2xl">&times;</button>
                </div>
            </div>
        </div>
        
        <h2 class="text-lg font-bold mb-3 flex items-center gap-2">💰 Live Prices <span class="text-xs text-green-400 pulse">● REAL-TIME</span></h2>
        <div id="priceCards" class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6"></div>
        
        <h2 class="text-lg font-bold mb-3">🎯 Active Trading Signals</h2>
        <div id="signalCards" class="grid grid-cols-1 xl:grid-cols-2 gap-4 mb-6">
            <div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full">
                <div class="text-4xl mb-2">📡</div><div>Scanning for high-probability setups...</div>
                <div class="text-sm mt-2">Min Score: 80% | Min R:R: 2.5:1</div>
            </div>
        </div>
        
        <h2 class="text-lg font-bold mb-3">📜 Signal History</h2>
        <div class="bg-gray-800 rounded-xl overflow-hidden mb-6">
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead class="bg-gray-700">
                        <tr><th class="px-4 py-3 text-left">Time</th><th class="px-4 py-3 text-left">Symbol</th><th class="px-4 py-3 text-left">Dir</th><th class="px-4 py-3 text-left">Entry</th><th class="px-4 py-3 text-left">SL</th><th class="px-4 py-3 text-left">TP1</th><th class="px-4 py-3 text-left">TP2</th><th class="px-4 py-3 text-left">R:R</th><th class="px-4 py-3 text-left">Score</th></tr>
                    </thead>
                    <tbody id="historyBody"></tbody>
                </table>
            </div>
        </div>
        
        <h2 class="text-lg font-bold mb-3">📡 Event Log</h2>
        <div id="eventLog" class="bg-gray-800 rounded-xl p-4 h-40 overflow-y-auto font-mono text-xs scrollbar-thin"></div>
        
        <div class="mt-6 text-center text-gray-500 text-xs">
            <p>⚠️ Educational purposes only. Not financial advice.</p>
        </div>
    </div>
    
    <script>
        const socket = io();
        let prices = {}, signals = {}, history = [];
        
        function updateClock() {
            const now = new Date();
            const bkk = new Date(now.toLocaleString("en-US", {timeZone: "Asia/Bangkok"}));
            document.getElementById('bangkokClock').textContent = '🇹🇭 ' + bkk.toLocaleTimeString('en-GB');
            const hour = bkk.getHours();
            let session = hour >= 19 && hour <= 23 ? 'LONDON-NY ⭐⭐' : hour >= 14 && hour <= 23 ? 'LONDON ⭐' : hour >= 19 || hour <= 4 ? 'NEW YORK' : hour >= 6 && hour <= 14 ? 'ASIA' : 'OFF-PEAK';
            document.getElementById('sessionInfo').textContent = session;
        }
        setInterval(updateClock, 1000); updateClock();
        
        function log(msg, type='info') {
            const el = document.getElementById('eventLog');
            const bkk = new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Bangkok"}));
            const colors = {'signal': 'text-green-400', 'price': 'text-blue-400', 'error': 'text-red-400', 'info': 'text-gray-400'};
            el.innerHTML = `<div class="${colors[type]}">[${bkk.toLocaleTimeString('en-GB')}] ${msg}</div>` + el.innerHTML;
        }
        
        function renderPrices() {
            const symbols = {'GOLD': '🥇', 'SILVER': '🥈', 'OIL': '🛢️'};
            let html = '';
            for (const [sym, emoji] of Object.entries(symbols)) {
                const p = prices[sym];
                if (!p) { html += `<div class="bg-gray-800 rounded-xl p-4"><div class="text-gray-400">${emoji} ${sym} - Loading...</div></div>`; continue; }
                const color = p.change >= 0 ? 'text-green-400' : 'text-red-400';
                html += `<div id="price-${sym}" class="bg-gray-800 rounded-xl p-4 border border-gray-700">
                    <div class="flex justify-between items-center mb-2"><span class="text-lg font-bold">${emoji} ${sym}</span><span class="${color} text-sm font-mono">${p.change >= 0 ? '▲' : '▼'} ${p.change_pct.toFixed(3)}%</span></div>
                    <div class="text-3xl font-bold font-mono">$${p.price.toLocaleString()}</div>
                    <div class="flex justify-between text-xs text-gray-400 mt-2"><span>H: $${p.high}</span><span>L: $${p.low}</span></div>
                    <div class="text-xs text-gray-500 mt-1">Updated: ${p.time}</div></div>`;
            }
            document.getElementById('priceCards').innerHTML = html;
        }
        
        function renderSignals() {
            let html = '';
            const list = Object.values(signals);
            if (list.length === 0) {
                html = `<div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full"><div class="text-4xl mb-2">📡</div><div>Scanning...</div></div>`;
            } else {
                for (const s of list) {
                    const isBuy = s.direction === 'BUY';
                    const border = isBuy ? 'border-green-500' : 'border-red-500';
                    const bg = isBuy ? 'from-green-900/30' : 'from-red-900/30';
                    const dir = isBuy ? 'text-green-400' : 'text-red-400';
                    html += `<div class="bg-gradient-to-br ${bg} to-gray-800 rounded-xl p-5 border-l-4 ${border} slide-in">
                        <div class="flex justify-between items-start mb-4">
                            <div><div class="text-2xl font-bold ${dir}">${isBuy ? '🟢' : '🔴'} ${s.symbol} ${s.direction}</div><div class="text-sm text-gray-400">${s.session}</div></div>
                            <div class="text-right"><span class="bg-yellow-600/80 px-3 py-1 rounded-full text-sm">${s.strength}</span><div class="text-xs text-gray-400 mt-1">${s.timestamp}</div></div>
                        </div>
                        <div class="grid grid-cols-2 gap-3 mb-4">
                            <div class="bg-gray-900/50 rounded-lg p-3"><div class="text-xs text-gray-400">Entry</div><div class="text-xl font-bold font-mono">$${s.entry_price}</div></div>
                            <div class="bg-red-900/30 rounded-lg p-3 border border-red-900"><div class="text-xs text-red-400">Stop Loss</div><div class="text-xl font-bold font-mono text-red-400">$${s.stop_loss}</div><div class="text-xs text-gray-500">Risk: $${s.risk_dollars}</div></div>
                        </div>
                        <div class="grid grid-cols-3 gap-2 mb-4">
                            <div class="bg-green-900/30 rounded-lg p-2 border border-green-900 text-center"><div class="text-xs text-green-400">TP1</div><div class="font-bold font-mono text-green-400">$${s.tp1}</div><div class="text-xs text-gray-500">+$${s.reward_tp1}</div></div>
                            <div class="bg-green-900/40 rounded-lg p-2 border border-green-700 text-center"><div class="text-xs text-green-300">TP2</div><div class="font-bold font-mono text-green-300">$${s.tp2}</div><div class="text-xs text-gray-500">+$${s.reward_tp2}</div></div>
                            <div class="bg-green-900/50 rounded-lg p-2 border border-green-500 text-center"><div class="text-xs text-green-200">TP3</div><div class="font-bold font-mono text-green-200">$${s.tp3}</div><div class="text-xs text-gray-500">+$${s.reward_tp3}</div></div>
                        </div>
                        <div class="mb-3"><div class="flex justify-between text-xs mb-1"><span>Confluence</span><span>${s.confluence_score}% | R:R ${s.risk_reward}:1</span></div><div class="bg-gray-700 rounded-full h-2"><div class="bg-gradient-to-r from-green-500 to-emerald-400 h-2 rounded-full" style="width:${Math.min(s.confluence_score,100)}%"></div></div></div>
                        <div class="flex flex-wrap gap-1">${s.reasons.map(r => `<span class="bg-gray-700 px-2 py-1 rounded text-xs">${r}</span>`).join('')}</div>
                    </div>`;
                }
            }
            document.getElementById('signalCards').innerHTML = html;
            document.getElementById('signalCount').textContent = list.length;
        }
        
        function renderHistory() {
            let html = '';
            if (history.length === 0) { html = '<tr><td colspan="9" class="px-4 py-6 text-center text-gray-400">No signals yet...</td></tr>'; }
            else {
                for (const s of history.slice(0, 15)) {
                    const dir = s.direction === 'BUY' ? 'text-green-400 bg-green-900/30' : 'text-red-400 bg-red-900/30';
                    html += `<tr class="border-t border-gray-700 hover:bg-gray-700/30">
                        <td class="px-4 py-2 font-mono text-xs">${s.timestamp}</td><td class="px-4 py-2 font-bold">${s.symbol}</td>
                        <td class="px-4 py-2"><span class="${dir} px-2 py-1 rounded">${s.direction}</span></td>
                        <td class="px-4 py-2 font-mono">$${s.entry_price}</td><td class="px-4 py-2 font-mono text-red-400">$${s.stop_loss}</td>
                        <td class="px-4 py-2 font-mono text-green-400">$${s.tp1}</td><td class="px-4 py-2 font-mono text-green-300">$${s.tp2}</td>
                        <td class="px-4 py-2 font-bold">${s.risk_reward}:1</td><td class="px-4 py-2">${s.confluence_score}%</td></tr>`;
                }
            }
            document.getElementById('historyBody').innerHTML = html;
        }
        
        function showAlert(signal) {
            const area = document.getElementById('alertArea');
            document.getElementById('alertTitle').textContent = `NEW ${signal.direction} SIGNAL!`;
            document.getElementById('alertText').textContent = `${signal.symbol} @ $${signal.entry_price} | SL: $${signal.stop_loss} | TP1: $${signal.tp1}`;
            area.classList.remove('hidden');
            try { const ctx = new AudioContext(); const osc = ctx.createOscillator(); const gain = ctx.createGain(); osc.connect(gain); gain.connect(ctx.destination); osc.frequency.value = signal.direction === 'BUY' ? 800 : 600; gain.gain.value = 0.1; osc.start(); osc.stop(ctx.currentTime + 0.2); } catch(e) {}
            setTimeout(() => area.classList.add('hidden'), 10000);
        }
        
        socket.on('connect', () => { document.getElementById('connectionStatus').innerHTML = '<span class="text-green-400 pulse">● CONNECTED</span>'; log('Connected', 'info'); });
        socket.on('disconnect', () => { document.getElementById('connectionStatus').innerHTML = '<span class="text-red-400">● DISCONNECTED</span>'; log('Disconnected', 'error'); });
        socket.on('initial_state', (data) => { prices = data.prices || {}; signals = data.signals || {}; history = data.history || []; document.getElementById('scanCount').textContent = data.scan_count; document.getElementById('lastScan').textContent = data.last_scan; renderPrices(); renderSignals(); renderHistory(); log('Initial state received', 'info'); });
        socket.on('price_update', (data) => { const old = prices[data.symbol]?.price || 0; prices[data.symbol] = data.data; renderPrices(); if (data.flash) { const el = document.getElementById(`price-${data.symbol}`); if (el) { el.classList.add(data.data.price > old ? 'flash-up' : 'flash-down'); setTimeout(() => el.classList.remove('flash-up', 'flash-down'), 500); } } });
        socket.on('new_signal', (data) => { signals[data.symbol] = data.signal; history.unshift(data.signal); history = history.slice(0, 100); renderSignals(); renderHistory(); showAlert(data.signal); log(`🎯 NEW: ${data.symbol} ${data.signal.direction} @ $${data.signal.entry_price}`, 'signal'); });
        socket.on('scan_update', (data) => { document.getElementById('scanCount').textContent = data.scan_count; document.getElementById('lastScan').textContent = data.last_scan; document.getElementById('clientCount').textContent = data.connected; });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    return {'status': 'ok', 'time': format_bangkok_datetime()}

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("=" * 50)
    print(" 🎯 SIGNAL DASHBOARD - Bangkok Time")
    print("=" * 50)
    print(f"Time: {format_bangkok_datetime()}")
    
    scanner = threading.Thread(target=background_scanner, daemon=True)
    scanner.start()
    print("✅ Scanner started")
    
    port = int(os.environ.get('PORT', 8000))
    print(f"🌐 Running on port {port}")
    
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
