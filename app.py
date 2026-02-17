"""
⚡ UTS Pro v4.0 - Fixed Version with Better Error Handling
"""
from flask import Flask, render_template_string, request, redirect, url_for, session, jsonify, flash
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict, field
from functools import wraps
from enum import Enum
import pandas as pd
import numpy as np
import threading
import hashlib
import secrets
import time
import json
import os
import traceback

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

BANGKOK_TZ = timezone(timedelta(hours=7))

def get_bangkok_time(): return datetime.now(BANGKOK_TZ)
def format_bangkok_time(fmt="%H:%M:%S"): return get_bangkok_time().strftime(fmt)
def format_bangkok_datetime(fmt="%Y-%m-%d %H:%M:%S"): return get_bangkok_time().strftime(fmt)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
class Config:
    ATR_PERIOD = 14
    PIVOT_LOOKBACK = 10
    ZONE_LOOKBACK = 100
    SWING_STRENGTH = 5
    SIGNAL_MODE = "Combined"
    SIGNAL_SENSITIVITY = 2
    MIN_CONFLUENCE_SCORE = 3
    SR_CONFLUENCE_BOOST = 2
    AUTO_ZONE_ENABLED = True
    ZONE_THICKNESS_PIPS = 30
    MM_ENABLED = True
    MM_MULTIPLIER = 1.5
    MM_BODY_RATIO = 0.6
    REV_WICK_RATIO = 0.6
    REV_LOOKBACK = 3
    USE_TREND_FILTER = True
    EMA_PERIOD = 50
    USE_VOLUME_FILTER = False  # Disabled - volume data unreliable
    VOLUME_PERIOD = 20
    VOLUME_MULTIPLIER = 1.2
    USE_RSI_FILTER = True
    RSI_PERIOD = 14
    RSI_OB = 70
    RSI_OS = 30
    USE_ADX = True
    ADX_PERIOD = 14
    ADX_THRESHOLD = 25
    USE_ATR_STOPS = True
    ATR_STOP_MULT = 1.5
    RR_RATIO = 2.0
    MIN_RR = 1.5
    ACCOUNT_SIZE = 10000
    RISK_PERCENT = 1.0
    PIVOT_ENABLED = True
    FIB_ENABLED = True
    FIB_LOOKBACK = 100
    
    SYMBOLS = {
        "XAUUSD": {"name": "Gold", "emoji": "🥇", "yf": "GC=F"},
        "XAGUSD": {"name": "Silver", "emoji": "🥈", "yf": "SI=F"},
        "USOUSD": {"name": "Oil", "emoji": "🛢️", "yf": "CL=F"},
    }
    
    SCAN_INTERVAL = 60  # Increased to avoid rate limiting

# ═══════════════════════════════════════════════════════════════════════════════
# ENUMS & DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════
class CandleType(Enum):
    NONE = 0
    MM_BULL = 1
    MM_BEAR = 2
    REV_BULL = 3
    REV_BEAR = 4

class MarketStructure(Enum):
    RANGING = 0
    BULLISH = 1
    BEARISH = -1

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
    market_structure: str
    zone_status: str
    sr_cluster: int
    timestamp: str
    timestamp_unix: float
    candle_type: str = "NONE"
    bos_choch: str = ""
    premium_discount: str = ""
    adx_value: float = 0
    rsi_value: float = 0
    atr_value: float = 0
    position_size: float = 0

# ═══════════════════════════════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════════════════════════════
class Indicators:
    @staticmethod
    def ema(s, p): 
        return s.ewm(span=p, adjust=False).mean()
    
    @staticmethod
    def sma(s, p): 
        return s.rolling(window=p).mean()
    
    @staticmethod
    def rsi(s, p=14):
        d = s.diff()
        g = d.where(d > 0, 0).rolling(p).mean()
        l = (-d.where(d < 0, 0)).rolling(p).mean()
        return 100 - (100 / (1 + g / (l + 0.0001)))
    
    @staticmethod
    def atr(h, l, c, p=14):
        tr = pd.concat([h-l, abs(h-c.shift(1)), abs(l-c.shift(1))], axis=1).max(axis=1)
        return tr.rolling(p).mean()
    
    @staticmethod
    def adx(h, l, c, p=14):
        plus_dm = h.diff().where(lambda x: x > 0, 0)
        minus_dm = (-l.diff()).where(lambda x: x > 0, 0)
        tr = Indicators.atr(h, l, c, 1) * p
        plus_di = 100 * (plus_dm.rolling(p).mean() / (tr + 0.0001))
        minus_di = 100 * (minus_dm.rolling(p).mean() / (tr + 0.0001))
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 0.0001)
        return plus_di, minus_di, dx.rolling(p).mean()

# ═══════════════════════════════════════════════════════════════════════════════
# DATA FETCHER - IMPROVED WITH BETTER ERROR HANDLING
# ═══════════════════════════════════════════════════════════════════════════════
class DataFetcher:
    _cache = {}
    _cache_time = {}
    CACHE_DURATION = 30  # Cache for 30 seconds
    
    @staticmethod
    def get_current_price(symbol: str) -> Optional[Dict]:
        """Fetch current price with caching and better error handling"""
        cache_key = f"price_{symbol}"
        now = time.time()
        
        # Check cache
        if cache_key in DataFetcher._cache:
            if now - DataFetcher._cache_time.get(cache_key, 0) < DataFetcher.CACHE_DURATION:
                cached = DataFetcher._cache[cache_key].copy()
                cached['time'] = format_bangkok_time()
                cached['cached'] = True
                return cached
        
        try:
            import yfinance as yf
            yf_sym = Config.SYMBOLS.get(symbol, {}).get('yf', 'GC=F')
            
            print(f"[{format_bangkok_time()}] Fetching price for {symbol} ({yf_sym})...")
            
            ticker = yf.Ticker(yf_sym)
            
            # Try 1-minute data first
            hist = ticker.history(period="1d", interval="1m")
            
            # Fallback to 5-minute if 1-minute is empty
            if hist.empty:
                print(f"[{format_bangkok_time()}] 1m data empty, trying 5m...")
                hist = ticker.history(period="2d", interval="5m")
            
            # Fallback to 15-minute
            if hist.empty:
                print(f"[{format_bangkok_time()}] 5m data empty, trying 15m...")
                hist = ticker.history(period="5d", interval="15m")
            
            # Final fallback to daily
            if hist.empty:
                print(f"[{format_bangkok_time()}] 15m data empty, trying daily...")
                hist = ticker.history(period="1mo", interval="1d")
            
            if hist.empty:
                print(f"[{format_bangkok_time()}] ❌ No data for {symbol}")
                return None
            
            print(f"[{format_bangkok_time()}] ✅ Got {len(hist)} bars for {symbol}")
            
            latest = hist.iloc[-1]
            prev = hist.iloc[-2] if len(hist) > 1 else latest
            chg = float(latest["Close"]) - float(prev["Close"])
            
            result = {
                "symbol": symbol,
                "price": round(float(latest["Close"]), 2),
                "high": round(float(hist["High"].max()), 2),
                "low": round(float(hist["Low"].min()), 2),
                "change": round(chg, 2),
                "change_pct": round((chg / float(prev["Close"])) * 100, 3) if prev["Close"] != 0 else 0,
                "time": format_bangkok_time(),
                "bars": len(hist),
                "cached": False
            }
            
            # Update cache
            DataFetcher._cache[cache_key] = result
            DataFetcher._cache_time[cache_key] = now
            
            return result
            
        except Exception as e:
            print(f"[{format_bangkok_time()}] ❌ Price error {symbol}: {e}")
            traceback.print_exc()
            return None
    
    @staticmethod
    def fetch_candles(symbol: str, interval: str = "15m", period: str = "5d") -> Optional[pd.DataFrame]:
        """Fetch candle data with caching"""
        cache_key = f"candles_{symbol}_{interval}_{period}"
        now = time.time()
        
        # Check cache (longer duration for candles)
        if cache_key in DataFetcher._cache:
            if now - DataFetcher._cache_time.get(cache_key, 0) < 60:  # 60 second cache
                return DataFetcher._cache[cache_key].copy()
        
        try:
            import yfinance as yf
            yf_sym = Config.SYMBOLS.get(symbol, {}).get('yf', 'GC=F')
            
            print(f"[{format_bangkok_time()}] Fetching candles for {symbol} ({interval}, {period})...")
            
            df = yf.Ticker(yf_sym).history(period=period, interval=interval)
            
            if df.empty:
                print(f"[{format_bangkok_time()}] ❌ Empty candles for {symbol}")
                return None
            
            df.columns = [c.lower() for c in df.columns]
            
            print(f"[{format_bangkok_time()}] ✅ Got {len(df)} candles for {symbol}")
            
            # Update cache
            DataFetcher._cache[cache_key] = df.copy()
            DataFetcher._cache_time[cache_key] = now
            
            return df
            
        except Exception as e:
            print(f"[{format_bangkok_time()}] ❌ Candle error {symbol}: {e}")
            traceback.print_exc()
            return None

fetcher = DataFetcher()

# ═══════════════════════════════════════════════════════════════════════════════
# SESSION DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════
class SessionDetector:
    @staticmethod
    def get_current_session():
        h = datetime.utcnow().hour
        in_kill = (8 <= h < 10) or (14 <= h < 16)
        
        if 8 <= h < 10:
            name = "LONDON-KZ ⭐⭐"
        elif 14 <= h < 16:
            name = "NY-KZ ⭐⭐"
        elif 8 <= h < 16 and 13 <= h:
            name = "LONDON-NY ⭐⭐"
        elif 8 <= h < 16:
            name = "LONDON ⭐"
        elif 13 <= h < 22:
            name = "NEW YORK"
        elif h < 9:
            name = "ASIAN"
        else:
            name = "OFF-PEAK"
            
        return {'name': name, 'in_kill_zone': in_kill, 'score': 15 if in_kill else 10 if "LONDON" in name else 5}

# ═══════════════════════════════════════════════════════════════════════════════
# SIMPLIFIED SIGNAL GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════
class SignalGenerator:
    def generate_signal(self, symbol: str) -> Optional[Signal]:
        try:
            # Fetch data
            df = fetcher.fetch_candles(symbol, "15m", "5d")
            
            if df is None or len(df) < 50:
                print(f"[{format_bangkok_time()}] Not enough data for {symbol}: {len(df) if df is not None else 0} bars")
                return None
            
            # Calculate indicators
            df['ema9'] = Indicators.ema(df['close'], 9)
            df['ema21'] = Indicators.ema(df['close'], 21)
            df['ema50'] = Indicators.ema(df['close'], 50)
            df['ema200'] = Indicators.ema(df['close'], 200)
            df['rsi'] = Indicators.rsi(df['close'], Config.RSI_PERIOD)
            df['atr'] = Indicators.atr(df['high'], df['low'], df['close'], Config.ATR_PERIOD)
            df['di_plus'], df['di_minus'], df['adx'] = Indicators.adx(df['high'], df['low'], df['close'], Config.ADX_PERIOD)
            
            lat = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Get session
            session = SessionDetector.get_current_session()
            
            # Determine trend direction
            price = lat['close']
            direction = None
            score = 0
            reasons = []
            
            # EMA Trend Check
            if lat['ema9'] > lat['ema21'] > lat['ema50']:
                direction = "BUY"
                score += 3
                reasons.append("✅ EMA Bullish")
            elif lat['ema9'] < lat['ema21'] < lat['ema50']:
                direction = "SELL"
                score += 3
                reasons.append("✅ EMA Bearish")
            else:
                # No clear trend
                return None
            
            # RSI Check
            rsi = lat['rsi']
            if not pd.isna(rsi):
                if direction == "BUY" and 30 <= rsi <= 60:
                    score += 2
                    reasons.append(f"✅ RSI {rsi:.0f}")
                elif direction == "SELL" and 40 <= rsi <= 70:
                    score += 2
                    reasons.append(f"✅ RSI {rsi:.0f}")
            
            # ADX Check
            adx = lat['adx']
            if not pd.isna(adx) and adx > Config.ADX_THRESHOLD:
                score += 2
                reasons.append(f"✅ ADX {adx:.0f}")
            
            # Session bonus
            if session['in_kill_zone']:
                score += 1
                reasons.append(f"✅ {session['name']}")
            
            # Price above/below EMA200
            if direction == "BUY" and price > lat['ema200']:
                score += 1
                reasons.append("✅ Above EMA200")
            elif direction == "SELL" and price < lat['ema200']:
                score += 1
                reasons.append("✅ Below EMA200")
            
            # Minimum score check
            if score < Config.MIN_CONFLUENCE_SCORE:
                print(f"[{format_bangkok_time()}] {symbol} score too low: {score}")
                return None
            
            # Calculate entry levels
            entry = round(price, 2)
            atr_val = lat['atr'] if not pd.isna(lat['atr']) else 1.0
            
            if direction == "BUY":
                sl = round(entry - atr_val * Config.ATR_STOP_MULT, 2)
                risk = entry - sl
                tp1 = round(entry + risk * 1.5, 2)
                tp2 = round(entry + risk * 2.5, 2)
                tp3 = round(entry + risk * 4.0, 2)
            else:
                sl = round(entry + atr_val * Config.ATR_STOP_MULT, 2)
                risk = sl - entry
                tp1 = round(entry - risk * 1.5, 2)
                tp2 = round(entry - risk * 2.5, 2)
                tp3 = round(entry - risk * 4.0, 2)
            
            # R:R Check
            rr = abs(tp2 - entry) / risk if risk > 0 else 0
            if rr < Config.MIN_RR:
                return None
            
            # Create signal
            strength = "🔥 STRONG" if score >= 8 else "⭐ GOOD" if score >= 5 else "📊 MODERATE"
            
            # Market structure
            if lat['ema50'] > lat['ema200']:
                market_struct = "BULLISH"
            elif lat['ema50'] < lat['ema200']:
                market_struct = "BEARISH"
            else:
                market_struct = "RANGING"
            
            return Signal(
                signal_id=f"{symbol}_{direction}_{get_bangkok_time().strftime('%H%M%S')}",
                symbol=symbol,
                direction=direction,
                strength=strength,
                entry_price=entry,
                stop_loss=sl,
                tp1=tp1, tp2=tp2, tp3=tp3,
                risk_dollars=round(risk, 2),
                reward_tp1=round(abs(tp1-entry), 2),
                reward_tp2=round(abs(tp2-entry), 2),
                reward_tp3=round(abs(tp3-entry), 2),
                risk_reward=round(rr, 2),
                confluence_score=score,
                reasons=reasons,
                session=session['name'],
                market_structure=market_struct,
                zone_status="NEUTRAL",
                sr_cluster=0,
                timestamp=format_bangkok_datetime(),
                timestamp_unix=time.time(),
                adx_value=round(adx, 1) if not pd.isna(adx) else 0,
                rsi_value=round(rsi, 1) if not pd.isna(rsi) else 50,
                atr_value=round(atr_val, 2),
                position_size=round((Config.ACCOUNT_SIZE * Config.RISK_PERCENT / 100) / risk, 2) if risk > 0 else 0
            )
            
        except Exception as e:
            print(f"[{format_bangkok_time()}] ❌ Signal generation error for {symbol}: {e}")
            traceback.print_exc()
            return None

generator = SignalGenerator()

# ═══════════════════════════════════════════════════════════════════════════════
# STORE & AUTH
# ═══════════════════════════════════════════════════════════════════════════════
class Store:
    def __init__(self):
        self.prices = {}
        self.signals = {}
        self.history = []
        self.last_scan = "Never"
        self.scan_count = 0
        self.connected_clients = 0
        self.online_users = {}
        self.total_buy_signals = 0
        self.total_sell_signals = 0
        self.errors = []

store = Store()

class UserManager:
    def __init__(self):
        self.users_file = 'users.json'
        self.users = self.load_users()
    
    def load_users(self):
        try:
            if os.path.exists(self.users_file):
                with open(self.users_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        default = {
            'admin': {
                'password': self.hash_password('admin123'),
                'role': 'admin',
                'name': 'Administrator',
                'created': format_bangkok_datetime(),
                'active': True,
                'last_login': None
            }
        }
        self.save_users(default)
        return default
    
    def save_users(self, users=None):
        try:
            with open(self.users_file, 'w') as f:
                json.dump(users or self.users, f, indent=2)
        except:
            pass
    
    def hash_password(self, p):
        return hashlib.sha256(f"{p}uts_pro_2024".encode()).hexdigest()
    
    def verify_password(self, u, p):
        return u in self.users and self.users[u].get('active', True) and self.users[u]['password'] == self.hash_password(p)
    
    def create_user(self, u, p, n, r='user'):
        if u in self.users:
            return False, "Username exists"
        self.users[u] = {
            'password': self.hash_password(p),
            'role': r,
            'name': n,
            'created': format_bangkok_datetime(),
            'active': True,
            'last_login': None
        }
        self.save_users()
        return True, "Created"
    
    def update_user(self, u, d):
        if u not in self.users:
            return False, "Not found"
        if 'password' in d and d['password']:
            self.users[u]['password'] = self.hash_password(d['password'])
        for k in ['name', 'role', 'active']:
            if k in d:
                self.users[u][k] = d[k]
        self.save_users()
        return True, "Updated"
    
    def delete_user(self, u):
        if u == 'admin':
            return False, "Cannot delete admin"
        if u in self.users:
            del self.users[u]
            self.save_users()
            return True, "Deleted"
        return False, "Not found"
    
    def get_user(self, u):
        return self.users.get(u)
    
    def get_all_users(self):
        return {u: {k: v for k, v in d.items() if k != 'password'} for u, d in self.users.items()}
    
    def update_last_login(self, u):
        if u in self.users:
            self.users[u]['last_login'] = format_bangkok_datetime()
            self.save_users()

user_manager = UserManager()

def login_required(f):
    @wraps(f)
    def decorated(*a, **kw):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*a, **kw)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*a, **kw):
        if 'user' not in session:
            return redirect(url_for('login'))
        u = user_manager.get_user(session['user'])
        if not u or u.get('role') != 'admin':
            flash('Admin required', 'error')
            return redirect(url_for('dashboard'))
        return f(*a, **kw)
    return decorated

# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SCANNER - IMPROVED
# ═══════════════════════════════════════════════════════════════════════════════
scanner_running = False

def background_scanner():
    global scanner_running
    scanner_running = True
    
    print(f"[{format_bangkok_time()}] 🚀 Scanner thread started!")
    
    # Initial delay to let server start
    time.sleep(3)
    
    while True:
        try:
            store.scan_count += 1
            store.last_scan = format_bangkok_time()
            
            print(f"\n[{format_bangkok_time()}] ═══════════════════════════════════")
            print(f"[{format_bangkok_time()}] 📡 Scan #{store.scan_count} starting...")
            
            for symbol in Config.SYMBOLS.keys():
                print(f"[{format_bangkok_time()}] Processing {symbol}...")
                
                # Fetch price
                price = fetcher.get_current_price(symbol)
                if price:
                    store.prices[symbol] = price
                    socketio.emit('price_update', {'symbol': symbol, 'data': price})
                    print(f"[{format_bangkok_time()}] ✅ {symbol} price: ${price['price']}")
                else:
                    print(f"[{format_bangkok_time()}] ⚠️ No price for {symbol}")
                
                # Generate signal
                sig = generator.generate_signal(symbol)
                if sig:
                    old = store.signals.get(symbol)
                    if not old or old.direction != sig.direction or time.time() - old.timestamp_unix > 1800:
                        store.signals[symbol] = sig
                        store.history.insert(0, sig)
                        store.history = store.history[:100]
                        
                        if sig.direction == "BUY":
                            store.total_buy_signals += 1
                        else:
                            store.total_sell_signals += 1
                        
                        socketio.emit('new_signal', {'symbol': symbol, 'signal': asdict(sig)})
                        print(f"[{format_bangkok_time()}] 🎯 NEW SIGNAL: {symbol} {sig.direction} @ ${sig.entry_price}")
                
                # Small delay between symbols to avoid rate limiting
                time.sleep(5)
            
            # Emit scan update
            socketio.emit('scan_update', {
                'scan_count': store.scan_count,
                'last_scan': store.last_scan,
                'connected': store.connected_clients,
                'stats': {
                    'total_buy': store.total_buy_signals,
                    'total_sell': store.total_sell_signals
                }
            })
            
            print(f"[{format_bangkok_time()}] ✅ Scan #{store.scan_count} complete")
            print(f"[{format_bangkok_time()}] ═══════════════════════════════════\n")
            
        except Exception as e:
            error_msg = f"Scanner error: {e}"
            print(f"[{format_bangkok_time()}] ❌ {error_msg}")
            traceback.print_exc()
            store.errors.append({'time': format_bangkok_datetime(), 'error': str(e)})
            store.errors = store.errors[-10:]  # Keep last 10 errors
        
        # Wait before next scan
        time.sleep(Config.SCAN_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return redirect(url_for('dashboard') if 'user' in session else url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        u = request.form.get('username', '').strip().lower()
        p = request.form.get('password', '')
        if user_manager.verify_password(u, p):
            session.permanent = True
            session['user'] = u
            user_manager.update_last_login(u)
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        flash('Invalid credentials', 'error')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    if 'user' in session:
        for sid, u in list(store.online_users.items()):
            if u == session['user']:
                del store.online_users[sid]
    session.clear()
    flash('Logged out', 'success')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    u = user_manager.get_user(session['user'])
    return render_template_string(DASHBOARD_TEMPLATE, 
        user_name=u.get('name', session['user']), 
        is_admin=u.get('role') == 'admin')

@app.route('/admin')
@admin_required
def admin():
    return render_template_string(ADMIN_TEMPLATE, 
        users=user_manager.get_all_users(), 
        online_users=store.online_users)

@app.route('/admin/create-user', methods=['POST'])
@admin_required
def admin_create_user():
    ok, msg = user_manager.create_user(
        request.form.get('username', '').strip().lower(),
        request.form.get('password', ''),
        request.form.get('name', '').strip(),
        request.form.get('role', 'user')
    )
    flash(msg, 'success' if ok else 'error')
    return redirect(url_for('admin'))

@app.route('/admin/toggle-user/<username>', methods=['POST'])
@admin_required
def admin_toggle_user(username):
    u = user_manager.get_user(username)
    if u:
        user_manager.update_user(username, {'active': not u.get('active', True)})
    return redirect(url_for('admin'))

@app.route('/admin/delete-user/<username>', methods=['POST'])
@admin_required
def admin_delete_user(username):
    user_manager.delete_user(username)
    return redirect(url_for('admin'))

@app.route('/admin/change-password', methods=['POST'])
@admin_required
def admin_change_password():
    p = request.form.get('new_password', '')
    if len(p) >= 6:
        user_manager.update_user('admin', {'password': p})
        flash('Changed', 'success')
    else:
        flash('Min 6 chars', 'error')
    return redirect(url_for('admin'))

@app.route('/health')
def health():
    return {
        'status': 'ok',
        'time': format_bangkok_datetime(),
        'version': 'UTS Pro 4.0',
        'scanner_running': scanner_running,
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'prices_count': len(store.prices),
        'signals_count': len(store.signals),
        'errors': store.errors[-3:]
    }

@app.route('/debug')
def debug():
    """Debug endpoint to check system status"""
    return jsonify({
        'time': format_bangkok_datetime(),
        'scanner_running': scanner_running,
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'prices': store.prices,
        'signals_count': len(store.signals),
        'history_count': len(store.history),
        'connected_clients': store.connected_clients,
        'errors': store.errors
    })

@socketio.on('connect')
def handle_connect():
    store.connected_clients += 1
    if 'user' in session:
        store.online_users[request.sid] = session['user']
    
    print(f"[{format_bangkok_time()}] ✅ Client connected ({store.connected_clients} total)")
    
    emit('initial_state', {
        'prices': store.prices,
        'signals': {k: asdict(v) for k, v in store.signals.items()},
        'history': [asdict(s) for s in store.history[:20]],
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'stats': {
            'total_buy': store.total_buy_signals,
            'total_sell': store.total_sell_signals
        }
    })

@socketio.on('disconnect')
def handle_disconnect():
    store.connected_clients = max(0, store.connected_clients - 1)
    if request.sid in store.online_users:
        del store.online_users[request.sid]
    print(f"[{format_bangkok_time()}] ❌ Client disconnected ({store.connected_clients} remaining)")

# ═══════════════════════════════════════════════════════════════════════════════
# TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════
LOGIN_TEMPLATE = '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>🔐 UTS Pro v4</title><script src="https://cdn.tailwindcss.com"></script></head><body class="bg-gray-900 min-h-screen flex items-center justify-center"><div class="bg-gray-800 p-8 rounded-2xl shadow-2xl w-full max-w-md border border-yellow-500/30"><div class="text-center mb-8"><div class="text-5xl mb-4">⚡</div><h1 class="text-2xl font-bold text-yellow-400">UTS Pro v4.0</h1><p class="text-gray-400">Ultimate Trading System</p></div>{% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for category, message in messages %}<div class="mb-4 p-3 rounded-lg {% if category == 'error' %}bg-red-900 text-red-200{% else %}bg-green-900 text-green-200{% endif %}">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}<form method="POST" class="space-y-6"><div><label class="block text-gray-300 text-sm font-medium mb-2">Username</label><input type="text" name="username" required class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-yellow-500" placeholder="Enter username"></div><div><label class="block text-gray-300 text-sm font-medium mb-2">Password</label><input type="password" name="password" required class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-yellow-500" placeholder="Enter password"></div><button type="submit" class="w-full py-3 bg-yellow-600 hover:bg-yellow-700 text-black font-bold rounded-lg transition">🔓 Login</button></form><div class="mt-6 text-center text-gray-500 text-sm"><p>Default: admin / admin123</p></div></div></body></html>'''

ADMIN_TEMPLATE = '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>👑 Admin - UTS Pro</title><script src="https://cdn.tailwindcss.com"></script></head><body class="bg-gray-900 min-h-screen text-white"><div class="container mx-auto px-4 py-8 max-w-6xl"><div class="flex justify-between items-center mb-8"><div><h1 class="text-3xl font-bold text-yellow-400">👑 Admin Panel</h1><p class="text-gray-400">UTS Pro v4.0</p></div><div class="flex gap-4"><a href="{{ url_for('dashboard') }}" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg">📊 Dashboard</a><a href="{{ url_for('logout') }}" class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg">🚪 Logout</a></div></div>{% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for category, message in messages %}<div class="mb-4 p-4 rounded-lg {% if category == 'error' %}bg-red-900{% else %}bg-green-900{% endif %}">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}<div class="bg-gray-800 rounded-xl p-6 mb-8 border border-gray-700"><h2 class="text-xl font-bold mb-4 text-yellow-400">➕ Create User</h2><form method="POST" action="{{ url_for('admin_create_user') }}" class="grid grid-cols-1 md:grid-cols-5 gap-4"><input type="text" name="username" placeholder="Username" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg"><input type="password" name="password" placeholder="Password" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg"><input type="text" name="name" placeholder="Name" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg"><select name="role" class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg"><option value="user">User</option><option value="admin">Admin</option></select><button type="submit" class="px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg font-bold">✅ Create</button></form></div><div class="bg-gray-800 rounded-xl overflow-hidden border border-gray-700 mb-8"><table class="w-full"><thead class="bg-gray-700"><tr><th class="px-6 py-3 text-left">User</th><th class="px-6 py-3 text-left">Name</th><th class="px-6 py-3 text-left">Role</th><th class="px-6 py-3 text-left">Status</th><th class="px-6 py-3 text-left">Actions</th></tr></thead><tbody>{% for username, user in users.items() %}<tr class="border-t border-gray-700"><td class="px-6 py-4 font-bold">{{ username }}</td><td class="px-6 py-4">{{ user.name }}</td><td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.role == 'admin' %}bg-yellow-600{% else %}bg-blue-600{% endif %}">{{ user.role }}</span></td><td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.active %}bg-green-600{% else %}bg-red-600{% endif %}">{{ 'Active' if user.active else 'Inactive' }}</span></td><td class="px-6 py-4">{% if username != 'admin' %}<form method="POST" action="{{ url_for('admin_toggle_user', username=username) }}" class="inline"><button type="submit" class="px-3 py-1 {% if user.active %}bg-orange-600{% else %}bg-green-600{% endif %} rounded text-sm">Toggle</button></form><form method="POST" action="{{ url_for('admin_delete_user', username=username) }}" class="inline ml-2"><button type="submit" class="px-3 py-1 bg-red-600 rounded text-sm">🗑️</button></form>{% endif %}</td></tr>{% endfor %}</tbody></table></div><div class="bg-gray-800 rounded-xl p-6 border border-gray-700"><h2 class="text-xl font-bold mb-4 text-yellow-400">🔑 Change Password</h2><form method="POST" action="{{ url_for('admin_change_password') }}" class="flex gap-4"><input type="password" name="new_password" placeholder="New Password" required minlength="6" class="flex-1 px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg"><button type="submit" class="px-6 py-2 bg-yellow-600 hover:bg-yellow-700 rounded-lg font-bold text-black">🔄 Change</button></form></div></div></body></html>'''

DASHBOARD_TEMPLATE = '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>⚡ UTS Pro v4.0</title><script src="https://cdn.tailwindcss.com"></script><script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script><style>@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}.pulse{animation:pulse 1s infinite}@keyframes glow{0%,100%{box-shadow:0 0 5px #ffd700}50%{box-shadow:0 0 20px #ffd700}}.glow{animation:glow 1.5s infinite}</style></head><body class="bg-gray-900 text-white min-h-screen"><div class="container mx-auto px-4 py-6 max-w-7xl"><div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4"><div><h1 class="text-2xl md:text-3xl font-bold"><span class="text-yellow-400">⚡ UTS Pro</span> <span class="text-gray-400">v4.0</span></h1><p class="text-gray-400 text-sm">Welcome, <span class="text-yellow-400 font-bold">{{ user_name }}</span></p></div><div class="flex items-center gap-4 flex-wrap"><div id="clock" class="bg-gray-800 px-4 py-2 rounded-lg font-mono text-lg border border-yellow-500/30">--:--:--</div><span id="status" class="text-red-400">● Connecting...</span>{% if is_admin %}<a href="{{ url_for('admin') }}" class="px-3 py-2 bg-yellow-600 hover:bg-yellow-700 rounded-lg text-sm text-black font-bold">👑 Admin</a>{% endif %}<a href="{{ url_for('logout') }}" class="px-3 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm">🚪</a></div></div><div class="grid grid-cols-2 md:grid-cols-6 gap-3 mb-6"><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Scans</div><div id="scanCount" class="text-xl font-bold text-yellow-400">0</div></div><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Signals</div><div id="signalCount" class="text-xl font-bold text-green-400">0</div></div><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Last Scan</div><div id="lastScan" class="text-lg font-mono">--:--</div></div><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Session</div><div id="session" class="text-sm font-bold text-yellow-400">-</div></div><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Buy Signals</div><div id="buyCount" class="text-xl font-bold text-green-400">0</div></div><div class="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"><div class="text-gray-400 text-xs">Sell Signals</div><div id="sellCount" class="text-xl font-bold text-red-400">0</div></div></div><div id="alertArea" class="hidden mb-6"><div class="bg-gradient-to-r from-yellow-900/50 to-yellow-800/50 border-2 border-yellow-500 rounded-xl p-4 glow"><div class="flex items-center gap-3"><span class="text-4xl">🚨</span><div><div id="alertTitle" class="text-xl font-bold text-yellow-400">NEW SIGNAL!</div><div id="alertText" class="text-yellow-200"></div></div></div></div></div><h2 class="text-lg font-bold mb-3 text-yellow-400">💰 Live Prices <span class="text-xs text-green-400 pulse">● LIVE</span></h2><div id="prices" class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6"></div><h2 class="text-lg font-bold mb-3 text-yellow-400">🎯 Active Signals</h2><div id="signals" class="grid grid-cols-1 xl:grid-cols-2 gap-4 mb-6"><div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full border border-gray-700"><div class="text-4xl mb-2">📡</div><div>Scanning for signals...</div></div></div><h2 class="text-lg font-bold mb-3 text-yellow-400">📜 History</h2><div class="bg-gray-800 rounded-xl overflow-hidden mb-6 border border-gray-700"><div class="overflow-x-auto"><table class="w-full text-sm"><thead class="bg-gray-700"><tr><th class="px-3 py-2 text-left">Time</th><th class="px-3 py-2 text-left">Symbol</th><th class="px-3 py-2 text-left">Dir</th><th class="px-3 py-2 text-left">Entry</th><th class="px-3 py-2 text-left">SL</th><th class="px-3 py-2 text-left">TP1</th><th class="px-3 py-2 text-left">R:R</th><th class="px-3 py-2 text-left">Score</th></tr></thead><tbody id="history"></tbody></table></div></div><div id="log" class="bg-gray-800 rounded-xl p-4 h-32 overflow-y-auto font-mono text-xs border border-gray-700"></div></div><script>const socket=io();let prices={},signals={},history=[],stats={};function updateClock(){const d=new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Bangkok"}));document.getElementById('clock').textContent='TH '+d.toLocaleTimeString('en-GB');const h=d.getUTCHours();let s='OFF-PEAK';if(h>=8&&h<10)s='LONDON-KZ ⭐⭐';else if(h>=14&&h<16)s='NY-KZ ⭐⭐';else if(h>=8&&h<16)s='LONDON ⭐';else if(h>=13&&h<22)s='NEW YORK';else if(h<9)s='ASIAN';document.getElementById('session').textContent=s}setInterval(updateClock,1000);updateClock();function log(m,t='info'){const el=document.getElementById('log');const tm=new Date().toLocaleTimeString('en-GB',{timeZone:'Asia/Bangkok'});const c={signal:'text-green-400',error:'text-red-400'}[t]||'text-gray-400';el.innerHTML=`<div class="${c}">[${tm}] ${m}</div>`+el.innerHTML}function renderPrices(){const syms={XAUUSD:'🥇 GOLD',XAGUSD:'🥈 SILVER',USOUSD:'🛢️ OIL'};let h='';for(const[s,n]of Object.entries(syms)){const p=prices[s];if(!p){h+=`<div class="bg-gray-800 rounded-xl p-4 text-gray-400 border border-gray-700">${n} - Loading...</div>`;continue}const c=p.change>=0?'text-green-400':'text-red-400';h+=`<div class="bg-gray-800 rounded-xl p-4 border border-gray-700"><div class="flex justify-between mb-2"><span class="font-bold">${n}</span><span class="${c}">${p.change>=0?'▲':'▼'} ${p.change_pct.toFixed(3)}%</span></div><div class="text-3xl font-bold">$${p.price.toLocaleString()}</div><div class="flex justify-between text-xs text-gray-400 mt-2"><span>H: $${p.high}</span><span>L: $${p.low}</span></div><div class="text-xs text-gray-500 mt-1">${p.time} ${p.cached?'(cached)':''}</div></div>`}document.getElementById('prices').innerHTML=h}function renderSignals(){const list=Object.values(signals);if(!list.length){document.getElementById('signals').innerHTML=`<div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full border border-gray-700"><div class="text-4xl mb-2">📡</div><div>Scanning for signals...</div></div>`;document.getElementById('signalCount').textContent='0';return}let h='';for(const s of list){const buy=s.direction==='BUY';const gc=buy?'from-green-900/30':'from-red-900/30';const bc=buy?'border-green-500':'border-red-500';const dc=buy?'text-green-400':'text-red-400';h+=`<div class="bg-gradient-to-br ${gc} to-gray-800 rounded-xl p-5 border-l-4 ${bc}"><div class="flex justify-between mb-4"><div><div class="text-2xl font-bold ${dc}">${buy?'🟢':'🔴'} ${s.symbol} ${s.direction}</div><div class="text-sm text-gray-400">${s.session} • ${s.market_structure}</div></div><div class="text-right"><span class="bg-yellow-600/80 px-3 py-1 rounded-full text-sm text-black font-bold">${s.strength}</span><div class="text-xs text-gray-400 mt-1">${s.timestamp}</div></div></div><div class="grid grid-cols-2 gap-3 mb-4"><div class="bg-gray-900/50 rounded-lg p-3"><div class="text-xs text-gray-400">Entry</div><div class="text-xl font-bold text-yellow-400">$${s.entry_price}</div></div><div class="bg-red-900/30 rounded-lg p-3 border border-red-900"><div class="text-xs text-red-400">Stop Loss</div><div class="text-xl font-bold text-red-400">$${s.stop_loss}</div><div class="text-xs text-gray-500">Risk: $${s.risk_dollars}</div></div></div><div class="grid grid-cols-3 gap-2 mb-4"><div class="bg-green-900/30 rounded-lg p-2 border border-green-900 text-center"><div class="text-xs text-green-400">TP1</div><div class="font-bold text-green-400">$${s.tp1}</div></div><div class="bg-green-900/40 rounded-lg p-2 border border-green-700 text-center"><div class="text-xs text-green-300">TP2</div><div class="font-bold text-green-300">$${s.tp2}</div></div><div class="bg-green-900/50 rounded-lg p-2 border border-green-500 text-center"><div class="text-xs text-green-200">TP3</div><div class="font-bold text-green-200">$${s.tp3}</div></div></div><div class="mb-3"><div class="flex justify-between text-xs mb-1"><span>Confluence Score</span><span>${s.confluence_score}/10 | R:R ${s.risk_reward}:1</span></div><div class="bg-gray-700 rounded-full h-2"><div class="bg-yellow-500 h-2 rounded-full" style="width:${Math.min(s.confluence_score*10,100)}%"></div></div></div><div class="flex flex-wrap gap-1">${s.reasons.map(r=>`<span class="bg-gray-700 px-2 py-1 rounded text-xs">${r}</span>`).join('')}</div></div>`}document.getElementById('signals').innerHTML=h;document.getElementById('signalCount').textContent=list.length}function renderHistory(){if(!history.length){document.getElementById('history').innerHTML='<tr><td colspan="8" class="px-3 py-4 text-center text-gray-400">No signals yet...</td></tr>';return}document.getElementById('history').innerHTML=history.slice(0,10).map(s=>`<tr class="border-t border-gray-700"><td class="px-3 py-2 text-xs">${s.timestamp}</td><td class="px-3 py-2 font-bold">${s.symbol}</td><td class="px-3 py-2"><span class="${s.direction==='BUY'?'text-green-400 bg-green-900/30':'text-red-400 bg-red-900/30'} px-2 py-1 rounded">${s.direction}</span></td><td class="px-3 py-2 text-yellow-400">$${s.entry_price}</td><td class="px-3 py-2 text-red-400">$${s.stop_loss}</td><td class="px-3 py-2 text-green-400">$${s.tp1}</td><td class="px-3 py-2 font-bold">${s.risk_reward}:1</td><td class="px-3 py-2">${s.confluence_score}</td></tr>`).join('')}function updateStats(d){if(!d)return;document.getElementById('buyCount').textContent=d.total_buy||0;document.getElementById('sellCount').textContent=d.total_sell||0}function showAlert(s){document.getElementById('alertTitle').textContent=`NEW ${s.direction} SIGNAL!`;document.getElementById('alertText').textContent=`${s.symbol} @ $${s.entry_price} | Score: ${s.confluence_score}`;document.getElementById('alertArea').classList.remove('hidden');try{const ctx=new AudioContext();const o=ctx.createOscillator();const g=ctx.createGain();o.connect(g);g.connect(ctx.destination);o.frequency.value=s.direction==='BUY'?800:600;g.gain.value=0.1;o.start();o.stop(ctx.currentTime+0.2)}catch(e){}setTimeout(()=>document.getElementById('alertArea').classList.add('hidden'),15000)}socket.on('connect',()=>{document.getElementById('status').innerHTML='<span class="text-green-400 pulse">● CONNECTED</span>';log('Connected to UTS Pro server','signal')});socket.on('disconnect',()=>{document.getElementById('status').innerHTML='<span class="text-red-400">● DISCONNECTED</span>';log('Disconnected','error')});socket.on('initial_state',(d)=>{prices=d.prices||{};signals=d.signals||{};history=d.history||[];stats=d.stats||{};document.getElementById('scanCount').textContent=d.scan_count;document.getElementById('lastScan').textContent=d.last_scan;renderPrices();renderSignals();renderHistory();updateStats(stats);log('Initial state received','signal')});socket.on('price_update',(d)=>{prices[d.symbol]=d.data;renderPrices();log(`Price update: ${d.symbol} $${d.data.price}`)});socket.on('new_signal',(d)=>{signals[d.symbol]=d.signal;history.unshift(d.signal);renderSignals();renderHistory();showAlert(d.signal);log(`🎯 NEW SIGNAL: ${d.symbol} ${d.signal.direction} @ $${d.signal.entry_price}`,'signal')});socket.on('scan_update',(d)=>{document.getElementById('scanCount').textContent=d.scan_count;document.getElementById('lastScan').textContent=d.last_scan;if(d.stats)updateStats(d.stats);log(`Scan #${d.scan_count} complete`)})</script></body></html>'''

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("="*60)
    print(" ⚡ ULTIMATE TRADING SYSTEM PRO v4.0 - FIXED")
    print("="*60)
    print(f"Time: {format_bangkok_datetime()}")
    print("Default: admin / admin123")
    print("="*60)
    
    # Start scanner thread
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    print("✅ Scanner thread started")
    
    port = int(os.environ.get('PORT', 8000))
    print(f"\n🌐 Running on port {port}")
    print(f"📊 Debug endpoint: /debug")
    print(f"❤️ Health endpoint: /health")
    
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
