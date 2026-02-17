"""
Ultimate Trading System Pro v4 - Web Dashboard (FIXED v2)
Fixes: yfinance ticker errors, direct Yahoo Finance API fallback,
       show_errors parameter removed, proper JSON parsing
"""

# CRITICAL: Must monkey-patch BEFORE all other imports
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template_string, request, redirect, url_for, session, flash
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict, field
from functools import wraps
import pandas as pd
import numpy as np
import requests
import hashlib
import secrets
import time
import json
import os
import traceback
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('UTS')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet',
                    ping_timeout=60, ping_interval=25)

# Bangkok timezone (UTC+7)
BANGKOK_TZ = timezone(timedelta(hours=7))

def get_bangkok_time():
    return datetime.now(BANGKOK_TZ)

def format_time(fmt="%H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

def format_datetime(fmt="%Y-%m-%d %H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
class Config:
    ATR_PERIOD = 14
    PIVOT_LOOKBACK = 10
    ZONE_LOOKBACK = 100
    SWING_STRENGTH = 5

    MIN_CONFLUENCE_SCORE = 5
    MIN_RR_RATIO = 1.5
    SIGNAL_EXPIRY_BARS = 30
    SR_CONFLUENCE_BOOST = 2

    ATR_STOP_MULT = 1.5
    RR_RATIO = 2.0
    MAX_RISK_DOLLARS = 15.0

    MM_CANDLE_SIZE_ATR = 1.5
    MM_BODY_RATIO = 0.6
    REV_WICK_RATIO = 0.6

    FVG_MIN_SIZE_ATR = 0.5
    MAX_FVG = 10

    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    ADX_PERIOD = 14
    ADX_THRESHOLD = 25
    EMA_PERIOD = 50

    SCAN_INTERVAL = 30

    SYMBOLS = {
        "XAUUSD": {"yf": "GC=F", "name": "Gold", "emoji": "🥇", "decimals": 2},
        "XAGUSD": {"yf": "SI=F", "name": "Silver", "emoji": "🥈", "decimals": 3},
        "USOUSD": {"yf": "CL=F", "name": "Oil", "emoji": "🛢️", "decimals": 2},
    }

# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class SRLevel:
    price: float
    name: str
    level_type: str
    strength: int

@dataclass
class Zone:
    top: float
    bottom: float
    zone_type: str
    strength: int
    fresh: bool

@dataclass
class FVG:
    top: float
    bottom: float
    bullish: bool
    mitigated: bool

@dataclass
class OrderBlock:
    top: float
    bottom: float
    bullish: bool
    mitigated: bool

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
    risk_reward: float
    confluence_score: int
    confluence_details: Dict
    reasons: List[str]
    session_name: str
    market_structure: str
    premium_discount: str
    sr_levels_near: int
    timestamp: str
    timestamp_unix: float

@dataclass
class MarketAnalysis:
    symbol: str
    price: float
    prev_close: float
    change: float
    change_pct: float
    atr: float
    rsi: float
    adx: float
    ema50: float
    ema200: float
    market_structure: str
    trend: str
    premium_discount: str
    in_kill_zone: bool
    session_name: str
    buy_confluence: int
    sell_confluence: int
    sr_levels: List[SRLevel]
    supply_zones: List[Zone]
    demand_zones: List[Zone]
    fvgs: List[FVG]
    order_blocks: List[OrderBlock]
    swing_high: float
    swing_low: float
    daily_high: float
    daily_low: float
    weekly_high: float
    weekly_low: float

class Store:
    def __init__(self):
        self.prices: Dict[str, Dict] = {}
        self.signals: Dict[str, Signal] = {}
        self.analysis: Dict[str, MarketAnalysis] = {}
        self.history: List[Signal] = []
        self.last_scan = "Never"
        self.scan_count = 0
        self.connected_clients = 0
        self.errors: List[str] = []
        self.scanner_status = "STARTING"
        self.stats = {
            'total_buy': 0, 'total_sell': 0,
            'buy_wins': 0, 'buy_losses': 0,
            'sell_wins': 0, 'sell_losses': 0
        }

    def add_error(self, msg):
        self.errors.insert(0, f"[{format_time()}] {msg}")
        self.errors = self.errors[:50]

store = Store()

# ═══════════════════════════════════════════════════════════════════════════════
# USER AUTHENTICATION
# ═══════════════════════════════════════════════════════════════════════════════
class UserManager:
    def __init__(self):
        self.users_file = 'users.json'
        self.users = self.load_users()

    def load_users(self):
        try:
            if os.path.exists(self.users_file):
                with open(self.users_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load users: {e}")
        default = {
            'admin': {
                'password': self.hash_password('admin123'),
                'role': 'admin', 'name': 'Administrator',
                'created': format_datetime(), 'active': True, 'last_login': None
            }
        }
        self.save_users(default)
        return default

    def save_users(self, users=None):
        try:
            with open(self.users_file, 'w') as f:
                json.dump(users or self.users, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save users: {e}")

    def hash_password(self, password):
        return hashlib.sha256(f"{password}uts_pro_2024".encode()).hexdigest()

    def verify(self, username, password):
        if username not in self.users:
            return False
        user = self.users[username]
        return user.get('active', True) and user['password'] == self.hash_password(password)

    def create_user(self, username, password, name, role='user'):
        if username in self.users:
            return False, "User exists"
        if len(password) < 6:
            return False, "Password too short (min 6)"
        self.users[username] = {
            'password': self.hash_password(password), 'role': role, 'name': name,
            'created': format_datetime(), 'active': True, 'last_login': None
        }
        self.save_users()
        return True, "Created"

    def get_all_users(self):
        return {u: {k: v for k, v in d.items() if k != 'password'} for u, d in self.users.items()}

user_manager = UserManager()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        user = user_manager.users.get(session['user'], {})
        if user.get('role') != 'admin':
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════════════════════
# DATA FETCHER - Direct Yahoo Finance API (no yfinance dependency issues)
# ═══════════════════════════════════════════════════════════════════════════════
class DataFetcher:
    """Fetch OHLCV data using direct Yahoo Finance API with yfinance fallback"""

    # Yahoo Finance v8 API intervals and period mapping
    INTERVAL_MAP = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "1d": "1d", "1wk": "1wk", "1mo": "1mo"
    }
    PERIOD_MAP = {
        "1d": "1d", "5d": "5d", "1mo": "1mo", "3mo": "3mo",
        "6mo": "6mo", "1y": "1y", "2y": "2y"
    }

    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 20  # seconds
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self._crumb = None
        self._cookies = None

    def _cache_key(self, symbol, interval, period):
        return f"{symbol}_{interval}_{period}"

    def _is_cached(self, key):
        if key in self._cache and key in self._cache_time:
            if time.time() - self._cache_time[key] < self._cache_ttl:
                return True
        return False

    def _get_yahoo_crumb(self):
        """Get Yahoo Finance crumb for API auth"""
        try:
            resp = self._session.get('https://fc.yahoo.com', timeout=10, allow_redirects=True)
            self._cookies = resp.cookies

            resp2 = self._session.get(
                'https://query2.finance.yahoo.com/v1/test/getcrumb',
                cookies=self._cookies, timeout=10
            )
            if resp2.status_code == 200 and resp2.text:
                self._crumb = resp2.text.strip()
                logger.info(f"✅ Yahoo crumb obtained")
                return True
        except Exception as e:
            logger.warning(f"Crumb fetch failed: {e}")
        return False

    def _fetch_yahoo_direct(self, symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
        """Fetch data directly from Yahoo Finance v8 API"""
        try:
            yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", symbol)

            # Try without crumb first (often works)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}"
            params = {
                'interval': self.INTERVAL_MAP.get(interval, interval),
                'range': self.PERIOD_MAP.get(period, period),
                'includePrePost': 'true',
                'events': 'div,split',
            }

            if self._crumb:
                params['crumb'] = self._crumb

            resp = self._session.get(url, params=params, cookies=self._cookies, timeout=15)

            if resp.status_code == 401 or resp.status_code == 403:
                # Need fresh crumb
                if self._get_yahoo_crumb():
                    params['crumb'] = self._crumb
                    resp = self._session.get(url, params=params, cookies=self._cookies, timeout=15)

            if resp.status_code != 200:
                logger.warning(f"Yahoo API {resp.status_code} for {yf_symbol}")
                return None

            data = resp.json()
            chart = data.get('chart', {}).get('result', [])
            if not chart:
                logger.warning(f"No chart data for {yf_symbol}")
                return None

            result = chart[0]
            timestamps = result.get('timestamp', [])
            quote = result.get('indicators', {}).get('quote', [{}])[0]

            if not timestamps or not quote:
                logger.warning(f"Empty quote data for {yf_symbol}")
                return None

            df = pd.DataFrame({
                'open': quote.get('open', []),
                'high': quote.get('high', []),
                'low': quote.get('low', []),
                'close': quote.get('close', []),
                'volume': quote.get('volume', []),
            }, index=pd.to_datetime(timestamps, unit='s', utc=True))

            df = df.dropna(subset=['open', 'high', 'low', 'close'])

            if len(df) > 0:
                logger.info(f"✅ Yahoo Direct OK: {yf_symbol} {interval} -> {len(df)} bars")
                return df

        except Exception as e:
            logger.warning(f"Yahoo Direct error {symbol} {interval}: {e}")
        return None

    def _fetch_yfinance(self, symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
        """Fallback: yfinance library"""
        try:
            import yfinance as yf
            yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", symbol)

            # Set headers to avoid blocks
            yf_ticker = yf.Ticker(yf_symbol)
            yf_ticker.session = self._session

            df = yf_ticker.history(period=period, interval=interval, prepost=True)

            if df is not None and not df.empty:
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                required = ['open', 'high', 'low', 'close']
                if not all(col in df.columns for col in required):
                    return None
                df = df.dropna(subset=required)
                if len(df) > 0:
                    logger.info(f"✅ yfinance OK: {yf_symbol} {interval} -> {len(df)} bars")
                    return df
        except Exception as e:
            logger.warning(f"yfinance error {symbol} {interval}: {e}")
        return None

    def _fetch_yfinance_download(self, symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
        """Fallback 2: yfinance.download()"""
        try:
            import yfinance as yf
            yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", symbol)

            alt_periods = {"5d": "1mo", "1mo": "3mo", "3mo": "6mo", "6mo": "1y"}
            alt_period = alt_periods.get(period, period)

            df = yf.download(yf_symbol, period=alt_period, interval=interval, progress=False)

            if df is not None and not df.empty:
                # Handle both single and multi-level columns
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0].lower() for c in df.columns]
                else:
                    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

                df = df.dropna(subset=['open', 'high', 'low', 'close'])
                if len(df) > 0:
                    logger.info(f"✅ yf.download OK: {yf_symbol} {interval} -> {len(df)} bars")
                    return df
        except Exception as e:
            logger.warning(f"yf.download error {symbol}: {e}")
        return None

    def fetch(self, symbol: str, interval: str = "15m", period: str = "5d") -> Optional[pd.DataFrame]:
        """Fetch data with caching and 3-level fallback"""
        cache_key = self._cache_key(symbol, interval, period)

        if self._is_cached(cache_key):
            return self._cache[cache_key]

        df = None

        # Method 1: Direct Yahoo Finance API (most reliable on servers)
        df = self._fetch_yahoo_direct(symbol, interval, period)

        # Method 2: yfinance Ticker.history()
        if df is None or df.empty:
            df = self._fetch_yfinance(symbol, interval, period)

        # Method 3: yfinance.download()
        if df is None or df.empty:
            df = self._fetch_yfinance_download(symbol, interval, period)

        if df is not None and not df.empty:
            self._cache[cache_key] = df
            self._cache_time[cache_key] = time.time()
            return df

        # Return stale cache if all methods fail
        if cache_key in self._cache:
            logger.warning(f"Using stale cache for {symbol} {interval}")
            return self._cache[cache_key]

        logger.error(f"ALL fetch methods failed for {symbol} {interval} {period}")
        return None

data_fetcher = DataFetcher()

# ═══════════════════════════════════════════════════════════════════════════════
# TECHNICAL ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
class UTSProAnalyzer:

    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        high, low, close = df['high'], df['low'], df['close'].shift(1)
        tr = pd.concat([high - low, (high - close).abs(), (low - close).abs()], axis=1).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()

    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0.0).rolling(period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(period, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
        high, low = df['high'], df['low']
        close_prev = df['close'].shift(1)
        tr = pd.concat([high - low, (high - close_prev).abs(), (low - close_prev).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period, min_periods=1).mean()

        plus_dm = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)

        plus_di = 100 * (plus_dm.rolling(period, min_periods=1).mean() / (atr + 1e-10))
        minus_di = 100 * (minus_dm.rolling(period, min_periods=1).mean() / (atr + 1e-10))

        dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10))
        adx = dx.rolling(period, min_periods=1).mean()
        return adx, plus_di, minus_di

    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        return df['close'].ewm(span=period, adjust=False, min_periods=1).mean()

    def find_swing_points(self, df: pd.DataFrame, strength: int = 5) -> Tuple[List[float], List[float]]:
        swing_highs, swing_lows = [], []
        n = len(df)

        if n < strength * 2 + 1:
            if n > 2:
                swing_highs = [df['high'].max()]
                swing_lows = [df['low'].min()]
            return swing_highs, swing_lows

        for i in range(strength, n - strength):
            is_high = all(df['high'].iloc[i] > df['high'].iloc[i - j] and
                         df['high'].iloc[i] > df['high'].iloc[i + j]
                         for j in range(1, strength + 1))
            if is_high:
                swing_highs.append(df['high'].iloc[i])

            is_low = all(df['low'].iloc[i] < df['low'].iloc[i - j] and
                        df['low'].iloc[i] < df['low'].iloc[i + j]
                        for j in range(1, strength + 1))
            if is_low:
                swing_lows.append(df['low'].iloc[i])

        if not swing_highs:
            swing_highs = [df['high'].iloc[-20:].max() if n >= 20 else df['high'].max()]
        if not swing_lows:
            swing_lows = [df['low'].iloc[-20:].min() if n >= 20 else df['low'].min()]

        return swing_highs, swing_lows

    def detect_market_structure(self, swing_highs: List[float], swing_lows: List[float]) -> str:
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return "RANGING"
        hh = swing_highs[-1] > swing_highs[-2]
        hl = swing_lows[-1] > swing_lows[-2]
        lh = swing_highs[-1] < swing_highs[-2]
        ll = swing_lows[-1] < swing_lows[-2]
        if hh and hl: return "BULLISH"
        elif lh and ll: return "BEARISH"
        return "RANGING"

    def detect_bos_choch(self, df, swing_highs, swing_lows, market_structure):
        if len(df) < 2 or not swing_highs or not swing_lows:
            return False, False, False, False
        close = df['close'].iloc[-1]
        close_prev = df['close'].iloc[-2]
        last_high = swing_highs[-1]
        last_low = swing_lows[-1]

        bullish_bos = market_structure == "BULLISH" and close > last_high and close_prev <= last_high
        bearish_bos = market_structure == "BEARISH" and close < last_low and close_prev >= last_low
        bullish_choch = market_structure == "BEARISH" and close > last_high and close_prev <= last_high
        bearish_choch = market_structure == "BULLISH" and close < last_low and close_prev >= last_low
        return bullish_bos, bearish_bos, bullish_choch, bearish_choch

    def detect_fvg(self, df: pd.DataFrame, atr: float) -> List[FVG]:
        fvgs = []
        min_size = atr * Config.FVG_MIN_SIZE_ATR
        n = len(df)
        for i in range(2, min(n, 50)):
            idx = n - i
            if idx - 2 < 0: break
            if df['low'].iloc[idx] > df['high'].iloc[idx - 2]:
                gap = df['low'].iloc[idx] - df['high'].iloc[idx - 2]
                if gap >= min_size:
                    fvg = FVG(top=df['low'].iloc[idx], bottom=df['high'].iloc[idx - 2],
                             bullish=True, mitigated=df['low'].iloc[-1] <= df['low'].iloc[idx])
                    if not fvg.mitigated: fvgs.append(fvg)
            if df['high'].iloc[idx] < df['low'].iloc[idx - 2]:
                gap = df['low'].iloc[idx - 2] - df['high'].iloc[idx]
                if gap >= min_size:
                    fvg = FVG(top=df['low'].iloc[idx - 2], bottom=df['high'].iloc[idx],
                             bullish=False, mitigated=df['high'].iloc[-1] >= df['high'].iloc[idx])
                    if not fvg.mitigated: fvgs.append(fvg)
            if len(fvgs) >= Config.MAX_FVG: break
        return fvgs

    def detect_order_blocks(self, df: pd.DataFrame, atr: float) -> List[OrderBlock]:
        obs = []
        n = len(df)
        for i in range(3, min(n, 30)):
            idx = n - i
            if idx - 1 < 0: break
            if (df['close'].iloc[idx - 1] < df['open'].iloc[idx - 1] and
                df['close'].iloc[idx] > df['open'].iloc[idx] and
                df['close'].iloc[idx] > df['high'].iloc[idx - 1] and
                abs(df['close'].iloc[idx] - df['open'].iloc[idx]) > atr * 0.5):
                ob = OrderBlock(top=df['high'].iloc[idx - 1], bottom=df['low'].iloc[idx - 1],
                               bullish=True, mitigated=df['low'].iloc[-1] <= df['low'].iloc[idx - 1])
                if not ob.mitigated: obs.append(ob)
            if (df['close'].iloc[idx - 1] > df['open'].iloc[idx - 1] and
                df['close'].iloc[idx] < df['open'].iloc[idx] and
                df['close'].iloc[idx] < df['low'].iloc[idx - 1] and
                abs(df['close'].iloc[idx] - df['open'].iloc[idx]) > atr * 0.5):
                ob = OrderBlock(top=df['high'].iloc[idx - 1], bottom=df['low'].iloc[idx - 1],
                               bullish=False, mitigated=df['high'].iloc[-1] >= df['high'].iloc[idx - 1])
                if not ob.mitigated: obs.append(ob)
            if len(obs) >= 10: break
        return obs

    def detect_supply_demand_zones(self, swing_highs, swing_lows, atr, current_price):
        supply_zones, demand_zones = [], []
        zone_thickness = atr * 1.5
        for sh in sorted(swing_highs, reverse=True)[:5]:
            if sh > current_price:
                supply_zones.append(Zone(top=sh + zone_thickness * 0.3, bottom=sh - zone_thickness * 0.7,
                    zone_type="supply", strength=2 if sh == max(swing_highs) else 1, fresh=True))
        for sl in sorted(swing_lows)[:5]:
            if sl < current_price:
                demand_zones.append(Zone(top=sl + zone_thickness * 0.7, bottom=sl - zone_thickness * 0.3,
                    zone_type="demand", strength=2 if sl == min(swing_lows) else 1, fresh=True))
        return supply_zones, demand_zones

    def calculate_sr_levels(self, df_daily, df_weekly, df_h4, current_price):
        levels = []
        if df_daily is not None and len(df_daily) >= 3:
            for i in range(1, min(4, len(df_daily))):
                levels.append(SRLevel(df_daily['high'].iloc[-i], f"D H[{i}]", "daily", 3))
                levels.append(SRLevel(df_daily['low'].iloc[-i], f"D L[{i}]", "daily", 3))
        if df_weekly is not None and len(df_weekly) >= 2:
            for i in range(1, min(3, len(df_weekly))):
                levels.append(SRLevel(df_weekly['high'].iloc[-i], f"W H[{i}]", "weekly", 4))
                levels.append(SRLevel(df_weekly['low'].iloc[-i], f"W L[{i}]", "weekly", 4))
        if df_h4 is not None and len(df_h4) >= 4:
            for i in range(1, min(5, len(df_h4))):
                levels.append(SRLevel(df_h4['high'].iloc[-i], f"H4 H[{i}]", "h4", 2))
                levels.append(SRLevel(df_h4['low'].iloc[-i], f"H4 L[{i}]", "h4", 2))
        if df_daily is not None and len(df_daily) >= 2:
            h, l, c = df_daily['high'].iloc[-2], df_daily['low'].iloc[-2], df_daily['close'].iloc[-2]
            pivot = (h + l + c) / 3
            r1, s1 = 2 * pivot - l, 2 * pivot - h
            r2, s2 = pivot + (h - l), pivot - (h - l)
            levels.extend([SRLevel(pivot, "Pivot", "pivot", 3), SRLevel(r1, "R1", "pivot", 2),
                          SRLevel(s1, "S1", "pivot", 2), SRLevel(r2, "R2", "pivot", 2), SRLevel(s2, "S2", "pivot", 2)])
        if df_daily is not None and len(df_daily) >= 20:
            fib_high = df_daily['high'].tail(20).max()
            fib_low = df_daily['low'].tail(20).min()
            fib_range = fib_high - fib_low
            if fib_range > 0:
                if current_price > (fib_high + fib_low) / 2:
                    for fib, name in [(0.236, "23.6%"), (0.382, "38.2%"), (0.5, "50%"), (0.618, "61.8%")]:
                        levels.append(SRLevel(fib_high - fib_range * fib, f"Fib {name}", "fib", 2 if fib < 0.5 else 3))
                else:
                    for fib, name in [(0.236, "23.6%"), (0.382, "38.2%"), (0.5, "50%"), (0.618, "61.8%")]:
                        levels.append(SRLevel(fib_low + fib_range * fib, f"Fib {name}", "fib", 2 if fib < 0.5 else 3))
        return levels

    def detect_market_maker_candle(self, df, atr):
        if len(df) < 2: return "NONE", False
        latest = df.iloc[-1]
        candle_range = latest['high'] - latest['low']
        body = abs(latest['close'] - latest['open'])
        if candle_range <= 0: return "NONE", False
        body_ratio = body / candle_range
        upper_wick = latest['high'] - max(latest['open'], latest['close'])
        lower_wick = min(latest['open'], latest['close']) - latest['low']
        upper_ratio = upper_wick / candle_range
        lower_ratio = lower_wick / candle_range
        is_bullish = latest['close'] > latest['open']
        if body_ratio >= Config.MM_BODY_RATIO and candle_range >= atr * Config.MM_CANDLE_SIZE_ATR:
            return ("MM_BULL" if is_bullish else "MM_BEAR"), True
        if body_ratio <= 0.4:
            if lower_ratio >= Config.REV_WICK_RATIO: return "REV_BULL", True
            if upper_ratio >= Config.REV_WICK_RATIO: return "REV_BEAR", True
        return "NONE", False

    def detect_candlestick_patterns(self, df):
        patterns = {'bullish_engulfing': False, 'bearish_engulfing': False,
                   'hammer': False, 'shooting_star': False, 'strong_bullish': False, 'strong_bearish': False}
        if len(df) < 2: return patterns
        curr, prev = df.iloc[-1], df.iloc[-2]
        body = abs(curr['close'] - curr['open'])
        total_range = curr['high'] - curr['low']
        if total_range <= 0: return patterns
        body_pct = body / total_range
        lower_wick = min(curr['close'], curr['open']) - curr['low']
        upper_wick = curr['high'] - max(curr['close'], curr['open'])
        if (curr['close'] > curr['open'] and prev['close'] < prev['open'] and
            curr['open'] <= prev['close'] and curr['close'] >= prev['open']):
            patterns['bullish_engulfing'] = True
        if (curr['close'] < curr['open'] and prev['close'] > prev['open'] and
            curr['open'] >= prev['close'] and curr['close'] <= prev['open']):
            patterns['bearish_engulfing'] = True
        if curr['close'] > curr['open'] and body > 0 and lower_wick > body * 1.5:
            patterns['hammer'] = True
        if curr['close'] < curr['open'] and body > 0 and upper_wick > body * 1.5:
            patterns['shooting_star'] = True
        if body_pct > 0.6:
            if curr['close'] > curr['open']: patterns['strong_bullish'] = True
            else: patterns['strong_bearish'] = True
        return patterns

    def check_session(self) -> Tuple[str, bool]:
        hour = get_bangkok_time().hour
        if 19 <= hour < 22: return "LONDON-NY ⭐⭐", True
        elif 14 <= hour < 19: return "LONDON ⭐", True
        elif 22 <= hour or hour < 4: return "NEW YORK", True
        elif 7 <= hour < 14: return "ASIA", True
        else: return "OFF-PEAK", False

    def calculate_confluence(self, df, direction, analysis, patterns, mm_type, bos_choch):
        score = 0; details = {}; reasons = []
        close = df['close'].iloc[-1]
        bullish_bos, bearish_bos, bullish_choch, bearish_choch = bos_choch

        if direction == "BUY":
            if close > analysis.ema50: score += 1; details['ema50'] = True; reasons.append("✅ Above EMA50")
            if close > analysis.ema200: score += 1; details['ema200'] = True; reasons.append("✅ Above EMA200")
            if analysis.premium_discount == "DISCOUNT": score += 1; details['discount'] = True; reasons.append("✅ Discount Zone")
            if analysis.rsi < 50: score += 1; details['rsi'] = True; reasons.append(f"✅ RSI {analysis.rsi:.0f}")
            if analysis.rsi < Config.RSI_OVERSOLD: score += 1; reasons.append("✅ RSI Oversold")
            if analysis.adx > Config.ADX_THRESHOLD: score += 1; details['adx'] = True; reasons.append(f"✅ ADX {analysis.adx:.0f}")
            if analysis.market_structure == "BULLISH": score += 1; details['structure'] = True; reasons.append("✅ Bullish Structure")
            if analysis.in_kill_zone: score += 1; details['killzone'] = True; reasons.append(f"✅ {analysis.session_name}")
            for zone in analysis.demand_zones:
                if zone.bottom <= close <= zone.top: score += 2; details['demand_zone'] = True; reasons.append("✅ In Demand Zone"); break
            sr_near = sum(1 for sr in analysis.sr_levels if sr.price < close and close - sr.price < analysis.atr * 0.5)
            if sr_near > 0: score += min(sr_near, Config.SR_CONFLUENCE_BOOST); details['sr_support'] = True; reasons.append(f"✅ Near {sr_near} SR Support")
            if bullish_bos: score += 1; reasons.append("✅ Bullish BOS")
            if bullish_choch: score += 1; reasons.append("🔄 Bullish CHOCH")
            if patterns.get('bullish_engulfing') or patterns.get('hammer'):
                score += 1; details['pattern'] = True
                if patterns.get('bullish_engulfing'): reasons.append("✅ Bullish Engulfing")
                if patterns.get('hammer'): reasons.append("✅ Hammer")
            if mm_type in ["MM_BULL", "REV_BULL"]: score += 1; details['mm'] = True; reasons.append("✅ MM Bull Signal")
        else:
            if close < analysis.ema50: score += 1; details['ema50'] = True; reasons.append("✅ Below EMA50")
            if close < analysis.ema200: score += 1; details['ema200'] = True; reasons.append("✅ Below EMA200")
            if analysis.premium_discount == "PREMIUM": score += 1; details['premium'] = True; reasons.append("✅ Premium Zone")
            if analysis.rsi > 50: score += 1; details['rsi'] = True; reasons.append(f"✅ RSI {analysis.rsi:.0f}")
            if analysis.rsi > Config.RSI_OVERBOUGHT: score += 1; reasons.append("✅ RSI Overbought")
            if analysis.adx > Config.ADX_THRESHOLD: score += 1; details['adx'] = True; reasons.append(f"✅ ADX {analysis.adx:.0f}")
            if analysis.market_structure == "BEARISH": score += 1; details['structure'] = True; reasons.append("✅ Bearish Structure")
            if analysis.in_kill_zone: score += 1; details['killzone'] = True; reasons.append(f"✅ {analysis.session_name}")
            for zone in analysis.supply_zones:
                if zone.bottom <= close <= zone.top: score += 2; details['supply_zone'] = True; reasons.append("✅ In Supply Zone"); break
            sr_near = sum(1 for sr in analysis.sr_levels if sr.price > close and sr.price - close < analysis.atr * 0.5)
            if sr_near > 0: score += min(sr_near, Config.SR_CONFLUENCE_BOOST); details['sr_resistance'] = True; reasons.append(f"✅ Near {sr_near} SR Resistance")
            if bearish_bos: score += 1; reasons.append("✅ Bearish BOS")
            if bearish_choch: score += 1; reasons.append("🔄 Bearish CHOCH")
            if patterns.get('bearish_engulfing') or patterns.get('shooting_star'):
                score += 1; details['pattern'] = True
                if patterns.get('bearish_engulfing'): reasons.append("✅ Bearish Engulfing")
                if patterns.get('shooting_star'): reasons.append("✅ Shooting Star")
            if mm_type in ["MM_BEAR", "REV_BEAR"]: score += 1; details['mm'] = True; reasons.append("✅ MM Bear Signal")
        return score, details, reasons

    def calculate_entry_levels(self, direction, entry, atr, analysis):
        dec = Config.SYMBOLS.get('XAUUSD', {}).get('decimals', 2)
        if direction == "BUY":
            zone_sl = analysis.demand_zones[0].bottom if analysis.demand_zones else entry - atr * 2
            atr_sl = entry - atr * Config.ATR_STOP_MULT
            sl = max(zone_sl, atr_sl, entry - Config.MAX_RISK_DOLLARS)
            risk = max(entry - sl, 0.01)
            tp1, tp2, tp3 = entry + risk * 1.5, entry + risk * Config.RR_RATIO, entry + risk * 3.0
        else:
            zone_sl = analysis.supply_zones[0].top if analysis.supply_zones else entry + atr * 2
            atr_sl = entry + atr * Config.ATR_STOP_MULT
            sl = min(zone_sl, atr_sl, entry + Config.MAX_RISK_DOLLARS)
            risk = max(sl - entry, 0.01)
            tp1, tp2, tp3 = entry - risk * 1.5, entry - risk * Config.RR_RATIO, entry - risk * 3.0
        return round(sl, dec), round(tp1, dec), round(tp2, dec), round(tp3, dec)

    def analyze(self, symbol: str) -> Optional[Tuple[MarketAnalysis, Optional[Signal]]]:
        try:
            df_15m = data_fetcher.fetch(symbol, "15m", "5d")
            df_daily = data_fetcher.fetch(symbol, "1d", "3mo")
            df_weekly = data_fetcher.fetch(symbol, "1wk", "6mo")
            df_h4 = data_fetcher.fetch(symbol, "1h", "1mo")

            if df_15m is None or len(df_15m) < 20:
                logger.warning(f"Insufficient data for {symbol}: {len(df_15m) if df_15m is not None else 0} bars")
                return None

            dec = Config.SYMBOLS.get(symbol, {}).get('decimals', 2)

            atr_series = self.calculate_atr(df_15m, Config.ATR_PERIOD)
            atr = atr_series.iloc[-1] if not atr_series.empty else 1.0
            if pd.isna(atr) or atr <= 0: atr = (df_15m['high'] - df_15m['low']).mean()

            rsi_series = self.calculate_rsi(df_15m, Config.RSI_PERIOD)
            rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50.0
            if pd.isna(rsi): rsi = 50.0

            adx, plus_di, minus_di = self.calculate_adx(df_15m, Config.ADX_PERIOD)
            adx_val = adx.iloc[-1] if not adx.empty else 20.0
            if pd.isna(adx_val): adx_val = 20.0

            ema50 = self.calculate_ema(df_15m, 50).iloc[-1]
            ema200 = self.calculate_ema(df_15m, 200).iloc[-1] if len(df_15m) >= 200 else ema50

            close = df_15m['close'].iloc[-1]
            prev_close = df_15m['close'].iloc[-2] if len(df_15m) >= 2 else close
            change = close - prev_close
            change_pct = (change / prev_close * 100) if prev_close != 0 else 0

            swing_highs, swing_lows = self.find_swing_points(df_15m, Config.SWING_STRENGTH)
            market_structure = self.detect_market_structure(swing_highs, swing_lows)
            bos_choch = self.detect_bos_choch(df_15m, swing_highs, swing_lows, market_structure)
            supply_zones, demand_zones = self.detect_supply_demand_zones(swing_highs, swing_lows, atr, close)
            fvgs = self.detect_fvg(df_15m, atr)
            obs = self.detect_order_blocks(df_15m, atr)
            sr_levels = self.calculate_sr_levels(df_daily, df_weekly, df_h4, close)

            if df_daily is not None and len(df_daily) >= 2:
                daily_high, daily_low = df_daily['high'].iloc[-1], df_daily['low'].iloc[-1]
                equilibrium = (daily_high + daily_low) / 2
                premium_discount = "PREMIUM" if close > equilibrium else "DISCOUNT" if close < equilibrium else "EQUILIBRIUM"
            else:
                daily_high, daily_low = df_15m['high'].max(), df_15m['low'].min()
                equilibrium = (daily_high + daily_low) / 2
                premium_discount = "PREMIUM" if close > equilibrium else "DISCOUNT"

            session_name, in_kill_zone = self.check_session()
            weekly_high = df_weekly['high'].iloc[-1] if df_weekly is not None and len(df_weekly) >= 1 else daily_high
            weekly_low = df_weekly['low'].iloc[-1] if df_weekly is not None and len(df_weekly) >= 1 else daily_low
            mm_type, has_mm = self.detect_market_maker_candle(df_15m, atr)
            patterns = self.detect_candlestick_patterns(df_15m)

            analysis = MarketAnalysis(
                symbol=symbol, price=round(close, dec), prev_close=round(prev_close, dec),
                change=round(change, dec), change_pct=round(change_pct, 2),
                atr=round(atr, dec + 2), rsi=round(rsi, 1), adx=round(adx_val, 1),
                ema50=round(ema50, dec), ema200=round(ema200, dec),
                market_structure=market_structure, trend="UP" if close > ema50 else "DOWN",
                premium_discount=premium_discount, in_kill_zone=in_kill_zone, session_name=session_name,
                buy_confluence=0, sell_confluence=0, sr_levels=sr_levels,
                supply_zones=supply_zones, demand_zones=demand_zones, fvgs=fvgs, order_blocks=obs,
                swing_high=swing_highs[-1] if swing_highs else close,
                swing_low=swing_lows[-1] if swing_lows else close,
                daily_high=round(daily_high, dec), daily_low=round(daily_low, dec),
                weekly_high=round(weekly_high, dec), weekly_low=round(weekly_low, dec)
            )

            buy_score, buy_details, buy_reasons = self.calculate_confluence(df_15m, "BUY", analysis, patterns, mm_type, bos_choch)
            sell_score, sell_details, sell_reasons = self.calculate_confluence(df_15m, "SELL", analysis, patterns, mm_type, bos_choch)
            analysis.buy_confluence = buy_score
            analysis.sell_confluence = sell_score

            signal = None

            if buy_score >= Config.MIN_CONFLUENCE_SCORE and buy_score > sell_score:
                has_trigger = (patterns.get('bullish_engulfing') or patterns.get('hammer') or
                              patterns.get('strong_bullish') or mm_type in ["MM_BULL", "REV_BULL"] or
                              bos_choch[0] or bos_choch[2])
                if has_trigger:
                    sl, tp1, tp2, tp3 = self.calculate_entry_levels("BUY", close, atr, analysis)
                    risk = max(close - sl, 0.01)
                    rr = abs(tp2 - close) / risk if risk > 0 else 0
                    if rr >= Config.MIN_RR_RATIO:
                        strength = "🔥 STRONG" if buy_score >= 10 else "⭐ GOOD" if buy_score >= 7 else "📊 MODERATE"
                        signal = Signal(
                            signal_id=f"{symbol}_BUY_{get_bangkok_time().strftime('%H%M%S')}",
                            symbol=symbol, direction="BUY", strength=strength,
                            entry_price=round(close, dec), stop_loss=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                            risk_dollars=round(risk, dec), risk_reward=round(rr, 2),
                            confluence_score=buy_score, confluence_details=buy_details,
                            reasons=buy_reasons, session_name=session_name,
                            market_structure=market_structure, premium_discount=premium_discount,
                            sr_levels_near=len([sr for sr in sr_levels if abs(sr.price - close) < atr]),
                            timestamp=format_datetime(), timestamp_unix=time.time())

            elif sell_score >= Config.MIN_CONFLUENCE_SCORE and sell_score > buy_score:
                has_trigger = (patterns.get('bearish_engulfing') or patterns.get('shooting_star') or
                              patterns.get('strong_bearish') or mm_type in ["MM_BEAR", "REV_BEAR"] or
                              bos_choch[1] or bos_choch[3])
                if has_trigger:
                    sl, tp1, tp2, tp3 = self.calculate_entry_levels("SELL", close, atr, analysis)
                    risk = max(sl - close, 0.01)
                    rr = abs(close - tp2) / risk if risk > 0 else 0
                    if rr >= Config.MIN_RR_RATIO:
                        strength = "🔥 STRONG" if sell_score >= 10 else "⭐ GOOD" if sell_score >= 7 else "📊 MODERATE"
                        signal = Signal(
                            signal_id=f"{symbol}_SELL_{get_bangkok_time().strftime('%H%M%S')}",
                            symbol=symbol, direction="SELL", strength=strength,
                            entry_price=round(close, dec), stop_loss=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                            risk_dollars=round(risk, dec), risk_reward=round(rr, 2),
                            confluence_score=sell_score, confluence_details=sell_details,
                            reasons=sell_reasons, session_name=session_name,
                            market_structure=market_structure, premium_discount=premium_discount,
                            sr_levels_near=len([sr for sr in sr_levels if abs(sr.price - close) < atr]),
                            timestamp=format_datetime(), timestamp_unix=time.time())

            return analysis, signal

        except Exception as e:
            logger.error(f"Analysis error {symbol}: {e}\n{traceback.format_exc()}")
            store.add_error(f"{symbol}: {str(e)}")
            return None

analyzer = UTSProAnalyzer()

# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SCANNER
# ═══════════════════════════════════════════════════════════════════════════════
def background_scanner():
    eventlet.sleep(3)
    logger.info(f"🚀 UTS Pro Scanner started at {format_time()}")
    store.scanner_status = "RUNNING"
    consecutive_failures = 0

    while True:
        try:
            store.scan_count += 1
            store.last_scan = format_time()
            scan_start = time.time()
            logger.info(f"\n{'='*50}")
            logger.info(f"📡 Scan #{store.scan_count} at {store.last_scan}")

            symbols_ok = 0
            symbols_fail = 0

            for symbol, sym_config in Config.SYMBOLS.items():
                try:
                    result = analyzer.analyze(symbol)
                    if result:
                        analysis, signal = result
                        symbols_ok += 1

                        store.prices[symbol] = {
                            'symbol': symbol, 'name': sym_config['name'], 'emoji': sym_config['emoji'],
                            'price': analysis.price, 'prev_close': analysis.prev_close,
                            'change': analysis.change, 'change_pct': analysis.change_pct,
                            'high': analysis.daily_high, 'low': analysis.daily_low, 'time': format_time()
                        }
                        store.analysis[symbol] = analysis

                        analysis_summary = {
                            'rsi': analysis.rsi, 'adx': analysis.adx, 'atr': analysis.atr,
                            'ema50': analysis.ema50, 'ema200': analysis.ema200,
                            'structure': analysis.market_structure, 'trend': analysis.trend,
                            'zone': analysis.premium_discount,
                            'buy_score': analysis.buy_confluence, 'sell_score': analysis.sell_confluence,
                            'session': analysis.session_name, 'in_kill_zone': analysis.in_kill_zone,
                            'sr_count': len(analysis.sr_levels),
                            'supply_zones': len(analysis.supply_zones), 'demand_zones': len(analysis.demand_zones),
                            'fvg_count': len(analysis.fvgs), 'ob_count': len(analysis.order_blocks),
                            'swing_high': analysis.swing_high, 'swing_low': analysis.swing_low
                        }

                        socketio.emit('price_update', {'symbol': symbol, 'data': store.prices[symbol], 'analysis': analysis_summary})

                        logger.info(f"  💰 {symbol}: ${analysis.price} | Chg: {analysis.change_pct:+.2f}%")
                        logger.info(f"     RSI:{analysis.rsi:.0f} ADX:{analysis.adx:.0f} | {analysis.market_structure} | {analysis.premium_discount}")
                        logger.info(f"     Buy:{analysis.buy_confluence}/15 Sell:{analysis.sell_confluence}/15 | SR:{len(analysis.sr_levels)}")

                        if signal:
                            old_signal = store.signals.get(symbol)
                            is_new = (not old_signal or old_signal.direction != signal.direction or
                                     time.time() - old_signal.timestamp_unix > 1800)
                            if is_new:
                                store.signals[symbol] = signal
                                store.history.insert(0, signal)
                                store.history = store.history[:100]
                                if signal.direction == "BUY": store.stats['total_buy'] += 1
                                else: store.stats['total_sell'] += 1
                                socketio.emit('new_signal', {'symbol': symbol, 'signal': asdict(signal)})
                                logger.info(f"  🎯 NEW SIGNAL: {symbol} {signal.direction} @ ${signal.entry_price} | Score: {signal.confluence_score}/15")
                    else:
                        symbols_fail += 1
                        logger.warning(f"  ⚠️ {symbol}: No data returned")
                except Exception as e:
                    symbols_fail += 1
                    logger.error(f"  ❌ {symbol} error: {e}")
                    store.add_error(f"{symbol}: {str(e)}")

                eventlet.sleep(2)

            scan_duration = time.time() - scan_start
            socketio.emit('scan_update', {
                'scan_count': store.scan_count, 'last_scan': store.last_scan,
                'connected': store.connected_clients, 'stats': store.stats,
                'scanner_status': store.scanner_status,
                'symbols_ok': symbols_ok, 'symbols_fail': symbols_fail,
                'scan_duration': round(scan_duration, 1)
            })

            logger.info(f"  ✅ Scan complete: {symbols_ok} OK, {symbols_fail} failed ({scan_duration:.1f}s)")

            if symbols_ok > 0:
                consecutive_failures = 0
                store.scanner_status = "RUNNING"
            else:
                consecutive_failures += 1
                if consecutive_failures > 5:
                    logger.warning("⚠️ Multiple scan failures - market may be closed")
                    store.scanner_status = "MARKET_CLOSED"
                    socketio.emit('scanner_status', {'status': 'MARKET_CLOSED', 'message': 'Market may be closed or data unavailable'})

        except Exception as e:
            logger.error(f"Scanner loop error: {e}\n{traceback.format_exc()}")
            store.add_error(f"Scanner: {str(e)}")

        eventlet.sleep(Config.SCAN_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
# HTML TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html><head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 UTS Pro v4 - Login</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 min-h-screen flex items-center justify-center">
    <div class="bg-gray-800 p-8 rounded-2xl shadow-2xl w-full max-w-md">
        <div class="text-center mb-8">
            <div class="text-5xl mb-4">⚡</div>
            <h1 class="text-2xl font-bold text-white">UTS Pro v4</h1>
            <p class="text-gray-400">Ultimate Trading System</p>
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}{% for cat, msg in messages %}
        <div class="mb-4 p-3 rounded-lg {% if cat == 'error' %}bg-red-900 text-red-200{% else %}bg-green-900 text-green-200{% endif %}">{{ msg }}</div>
        {% endfor %}{% endif %}{% endwith %}
        <form method="POST" class="space-y-6">
            <div><label class="block text-gray-300 text-sm mb-2">Username</label>
            <input type="text" name="username" required class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:border-yellow-500 focus:outline-none"></div>
            <div><label class="block text-gray-300 text-sm mb-2">Password</label>
            <input type="password" name="password" required class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:border-yellow-500 focus:outline-none"></div>
            <button type="submit" class="w-full py-3 bg-yellow-600 hover:bg-yellow-700 text-white font-bold rounded-lg transition">🔓 Login</button>
        </form>
        <p class="mt-4 text-center text-gray-500 text-sm">Default: admin / admin123</p>
    </div>
</body></html>
'''

DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html><head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ UTS Pro v4 Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
    <style>
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}.pulse{animation:pulse 1.5s infinite}
        @keyframes glow{0%,100%{box-shadow:0 0 5px #ffd700}50%{box-shadow:0 0 25px #ffd700}}.glow{animation:glow 1.5s infinite}
        @keyframes slideIn{from{transform:translateY(-20px);opacity:0}to{transform:translateY(0);opacity:1}}.slide-in{animation:slideIn .3s}
        .scrollbar::-webkit-scrollbar{width:6px}.scrollbar::-webkit-scrollbar-track{background:#1f2937}.scrollbar::-webkit-scrollbar-thumb{background:#4b5563;border-radius:3px}
    </style>
</head>
<body class="bg-gray-900 text-white min-h-screen">
    <div class="container mx-auto px-4 py-4 max-w-7xl">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-3">
            <div>
                <h1 class="text-xl md:text-2xl font-bold flex items-center gap-2">⚡ UTS Pro v4 <span class="text-sm text-yellow-400">Ultimate Trading System</span></h1>
                <p class="text-gray-400 text-sm">Welcome, <span class="text-yellow-400">{{ username }}</span> • SMC • Multi-TF S/R • Supply/Demand • FVG</p>
            </div>
            <div class="flex items-center gap-3">
                <div id="clock" class="bg-gray-800 px-3 py-2 rounded-lg font-mono text-sm">TH --:--:--</div>
                <span id="status" class="text-red-400 text-sm">● Connecting...</span>
                <span id="scannerBadge" class="hidden bg-blue-600 px-2 py-1 rounded text-xs">SCANNING</span>
                {% if is_admin %}<a href="/admin" class="px-3 py-2 bg-yellow-600 hover:bg-yellow-700 rounded-lg text-sm">👑 Admin</a>{% endif %}
                <a href="/logout" class="px-3 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm">🚪</a>
            </div>
        </div>

        <div class="grid grid-cols-3 md:grid-cols-7 gap-2 mb-4">
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Scans</div><div id="scanCount" class="font-bold text-lg">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Active Signals</div><div id="signalCount" class="font-bold text-lg text-green-400">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Last Scan</div><div id="lastScan" class="font-mono text-sm mt-1">--:--</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Session</div><div id="session" class="text-yellow-400 text-sm mt-1">-</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Buy Signals</div><div id="buyCount" class="text-green-400 font-bold text-lg">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Sell Signals</div><div id="sellCount" class="text-red-400 font-bold text-lg">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Scanner</div><div id="scannerStatus" class="text-blue-400 text-sm mt-1">STARTING</div></div>
        </div>

        <div id="alertArea" class="hidden mb-4">
            <div class="bg-gradient-to-r from-yellow-900 to-yellow-800 border-2 border-yellow-500 rounded-xl p-4 glow">
                <div class="flex items-center gap-3">
                    <span class="text-3xl">🎯</span>
                    <div><div id="alertTitle" class="text-xl font-bold">NEW SIGNAL!</div><div id="alertText" class="text-yellow-200"></div></div>
                    <button onclick="document.getElementById('alertArea').classList.add('hidden')" class="ml-auto text-yellow-400 hover:text-white text-xl">✕</button>
                </div>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
            <div class="lg:col-span-1 space-y-4">
                <h2 class="font-bold flex items-center gap-2">💰 Live Prices <span id="priceStatus" class="text-xs text-gray-500">waiting...</span></h2>
                <div id="prices" class="space-y-3">
                    <div class="bg-gray-800 rounded-lg p-4 text-center text-gray-500">
                        <div class="text-2xl mb-2">📡</div><div>Connecting to market data...</div>
                        <div class="text-xs mt-1">First scan may take 15-30 seconds</div>
                    </div>
                </div>
                <div class="bg-gray-800 rounded-xl p-4">
                    <h3 class="font-bold mb-3 text-yellow-400">🎯 Confluence Scores</h3>
                    <div id="confluence" class="space-y-2 text-sm"><div class="text-gray-500">Waiting for analysis...</div></div>
                </div>
                <div class="bg-gray-800 rounded-xl p-4">
                    <h3 class="font-bold mb-3 text-blue-400">📊 Technical Summary</h3>
                    <div id="techSummary" class="space-y-2 text-sm"><div class="text-gray-500">Waiting for data...</div></div>
                </div>
            </div>
            <div class="lg:col-span-2">
                <h2 class="font-bold mb-3">🎯 Active Trading Signals</h2>
                <div id="signals" class="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <div class="bg-gray-800 rounded-xl p-6 text-center text-gray-400 col-span-full">
                        <div class="text-4xl mb-2">📡</div><div>Scanning for high-probability setups...</div>
                        <div class="text-sm mt-2">Min Confluence: 5/15 | Min R:R: 1.5:1</div>
                    </div>
                </div>
            </div>
        </div>

        <div class="bg-gray-800 rounded-xl overflow-hidden mb-4">
            <h2 class="font-bold p-4 border-b border-gray-700">📜 Signal History</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead class="bg-gray-700"><tr>
                        <th class="px-3 py-2 text-left">Time</th><th class="px-3 py-2 text-left">Symbol</th>
                        <th class="px-3 py-2 text-left">Dir</th><th class="px-3 py-2 text-left">Entry</th>
                        <th class="px-3 py-2 text-left">SL</th><th class="px-3 py-2 text-left">TP1</th>
                        <th class="px-3 py-2 text-left">TP2</th><th class="px-3 py-2 text-left">TP3</th>
                        <th class="px-3 py-2 text-left">R:R</th><th class="px-3 py-2 text-left">Score</th>
                        <th class="px-3 py-2 text-left">Structure</th>
                    </tr></thead>
                    <tbody id="history"><tr><td colspan="11" class="px-3 py-4 text-center text-gray-500">No signals yet...</td></tr></tbody>
                </table>
            </div>
        </div>

        <div class="bg-gray-800 rounded-xl p-4">
            <div class="flex justify-between items-center mb-2">
                <h2 class="font-bold">📡 Event Log</h2>
                <button onclick="document.getElementById('log').innerHTML=''" class="text-xs text-gray-500 hover:text-white">Clear</button>
            </div>
            <div id="log" class="h-40 overflow-y-auto font-mono text-xs scrollbar space-y-1"></div>
        </div>
        <div class="mt-4 text-center text-gray-600 text-xs">⚠️ Educational purposes only. Not financial advice.</div>
    </div>

    <script>
        let prices={},signals={},history=[],analysisData={},connected=false,lastPriceUpdate={};

        const socket = io({transports:['websocket','polling'],reconnection:true,reconnectionDelay:1000,reconnectionAttempts:Infinity,timeout:20000});

        function updateClock(){
            const bkk=new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Bangkok"}));
            document.getElementById('clock').textContent='TH '+bkk.toLocaleTimeString('en-GB',{hour12:false});
            const h=bkk.getHours();
            let s='OFF-PEAK';
            if(h>=19&&h<22)s='LONDON-NY ⭐⭐';else if(h>=14&&h<19)s='LONDON ⭐';else if(h>=22||h<4)s='NEW YORK';else if(h>=7&&h<14)s='ASIA';
            document.getElementById('session').textContent=s;
        }
        setInterval(updateClock,1000);updateClock();

        function log(msg,type='info'){
            const el=document.getElementById('log');
            const t=new Date().toLocaleTimeString('en-GB',{timeZone:'Asia/Bangkok',hour12:false});
            const colors={signal:'text-green-400',price:'text-blue-300',error:'text-red-400',info:'text-gray-400',warn:'text-yellow-400',system:'text-purple-400'};
            const div=document.createElement('div');div.className=colors[type]||'text-gray-400';
            div.textContent=`[${t}] ${msg}`;el.insertBefore(div,el.firstChild);
            while(el.children.length>200)el.removeChild(el.lastChild);
        }

        function renderPrices(){
            const syms={XAUUSD:['🥇','GOLD'],XAGUSD:['🥈','SILVER'],USOUSD:['🛢️','OIL']};
            let html='',hasData=false;
            for(const[sym,[emoji,name]]of Object.entries(syms)){
                const p=prices[sym],a=analysisData[sym]||{};
                if(!p||!p.price){html+=`<div class="bg-gray-700/50 rounded-lg p-3 border border-gray-700"><div class="flex justify-between items-center"><span class="font-bold text-gray-400">${emoji} ${name}</span><span class="text-gray-500 pulse">Loading...</span></div></div>`;continue;}
                hasData=true;
                const chgColor=p.change>=0?'text-green-400':'text-red-400';
                const chgIcon=p.change>=0?'▲':'▼';
                const structColor=a.structure==='BULLISH'?'text-green-400':a.structure==='BEARISH'?'text-red-400':'text-yellow-400';
                const zoneColor=a.zone==='PREMIUM'?'text-red-400':a.zone==='DISCOUNT'?'text-green-400':'text-gray-400';
                const buyBarW=((a.buy_score||0)/15*100),sellBarW=((a.sell_score||0)/15*100);
                html+=`<div class="bg-gray-700 rounded-lg p-3 border border-gray-600 hover:border-gray-500 transition">
                    <div class="flex justify-between items-center mb-2"><div><span class="font-bold text-lg">${emoji} ${name}</span><span class="text-xs text-gray-400 ml-1">${sym}</span></div>
                    <div class="text-right"><div class="text-2xl font-bold">$${Number(p.price).toLocaleString(undefined,{minimumFractionDigits:2})}</div>
                    <div class="${chgColor} text-xs">${chgIcon} ${p.change>=0?'+':''}${p.change} (${p.change_pct>=0?'+':''}${p.change_pct}%)</div></div></div>
                    <div class="grid grid-cols-2 gap-x-4 gap-y-1 text-xs border-t border-gray-600 pt-2">
                        <div>RSI: <span class="${(a.rsi||50)<30?'text-green-400':(a.rsi||50)>70?'text-red-400':'text-white'} font-bold">${a.rsi||'-'}</span></div>
                        <div>ADX: <span class="${(a.adx||0)>25?'text-green-400':'text-gray-400'} font-bold">${a.adx||'-'}</span></div>
                        <div>Structure: <span class="${structColor} font-bold">${a.structure||'-'}</span></div>
                        <div>Zone: <span class="${zoneColor} font-bold">${a.zone||'-'}</span></div>
                        <div>H: <span class="text-green-400">$${p.high||'-'}</span></div>
                        <div>L: <span class="text-red-400">$${p.low||'-'}</span></div>
                    </div>
                    <div class="mt-2 grid grid-cols-2 gap-2">
                        <div><div class="flex justify-between text-xs mb-1"><span class="text-green-400">BUY</span><span class="text-green-400 font-bold">${a.buy_score||0}/15</span></div>
                        <div class="bg-gray-600 rounded-full h-1.5"><div class="bg-green-500 h-1.5 rounded-full transition-all" style="width:${buyBarW}%"></div></div></div>
                        <div><div class="flex justify-between text-xs mb-1"><span class="text-red-400">SELL</span><span class="text-red-400 font-bold">${a.sell_score||0}/15</span></div>
                        <div class="bg-gray-600 rounded-full h-1.5"><div class="bg-red-500 h-1.5 rounded-full transition-all" style="width:${sellBarW}%"></div></div></div>
                    </div>
                    <div class="mt-1 text-xs text-gray-500 flex justify-between"><span>SR:${a.sr_count||0} FVG:${a.fvg_count||0} OB:${a.ob_count||0}</span><span>${p.time||''}</span></div>
                </div>`;
            }
            document.getElementById('prices').innerHTML=html;
            document.getElementById('priceStatus').innerHTML=hasData?'<span class="text-green-400 pulse">● LIVE</span>':'<span class="text-gray-500 pulse">● waiting...</span>';
        }

        function renderConfluence(){
            let html='';
            for(const[sym,a]of Object.entries(analysisData)){
                const buyOk=(a.buy_score||0)>=5,sellOk=(a.sell_score||0)>=5;
                html+=`<div class="flex justify-between items-center py-2 border-b border-gray-700 last:border-0"><span class="font-bold">${sym}</span><div class="flex gap-2">
                    <span class="px-2 py-1 rounded text-xs font-bold ${buyOk?'bg-green-900 text-green-400 border border-green-700':'bg-gray-700 text-gray-500'}">BUY ${a.buy_score||0}/15 ${buyOk?'✓':''}</span>
                    <span class="px-2 py-1 rounded text-xs font-bold ${sellOk?'bg-red-900 text-red-400 border border-red-700':'bg-gray-700 text-gray-500'}">SELL ${a.sell_score||0}/15 ${sellOk?'✓':''}</span>
                </div></div>`;
            }
            document.getElementById('confluence').innerHTML=html||'<div class="text-gray-500">Waiting for analysis...</div>';
        }

        function renderTechSummary(){
            let html='';
            for(const[sym,a]of Object.entries(analysisData)){
                html+=`<div class="py-2 border-b border-gray-700 last:border-0"><div class="font-bold text-sm mb-1">${sym} ${a.trend==='UP'?'📈':'📉'}</div>
                <div class="grid grid-cols-2 gap-1 text-xs text-gray-400">
                    <div>EMA50: $${a.ema50||'-'}</div><div>EMA200: $${a.ema200||'-'}</div>
                    <div>ATR: ${a.atr||'-'}</div><div>Session: ${a.session||'-'}</div>
                    <div>Supply: ${a.supply_zones||0}</div><div>Demand: ${a.demand_zones||0}</div>
                </div></div>`;
            }
            document.getElementById('techSummary').innerHTML=html||'<div class="text-gray-500">Waiting for data...</div>';
        }

        function renderSignals(){
            const list=Object.values(signals);
            if(!list.length){document.getElementById('signals').innerHTML=`<div class="bg-gray-800 rounded-xl p-6 text-center text-gray-400 col-span-full"><div class="text-4xl mb-2">📡</div><div>Scanning for signals...</div><div class="text-sm mt-2">Min Confluence: 5/15 | Min R:R: 1.5:1</div></div>`;document.getElementById('signalCount').textContent='0';return;}
            let html='';
            for(const s of list){
                const isBuy=s.direction==='BUY',border=isBuy?'border-green-500':'border-red-500',bg=isBuy?'from-green-900/30':'from-red-900/30',dir=isBuy?'text-green-400':'text-red-400';
                const scoreW=((s.confluence_score||0)/15*100);
                html+=`<div class="bg-gradient-to-br ${bg} to-gray-800 rounded-xl p-4 border-l-4 ${border} slide-in">
                    <div class="flex justify-between items-start mb-3"><div><div class="text-xl font-bold ${dir}">${isBuy?'🟢':'🔴'} ${s.symbol} ${s.direction}</div>
                    <div class="text-xs text-gray-400">${s.session_name||''} | ${s.market_structure} | ${s.premium_discount}</div></div>
                    <div class="text-right"><span class="bg-yellow-600/80 px-2 py-1 rounded text-xs font-bold">${s.strength}</span><div class="text-xs text-gray-400 mt-1">${s.timestamp}</div></div></div>
                    <div class="grid grid-cols-2 gap-2 mb-3 text-sm">
                        <div class="bg-gray-900/50 rounded p-2"><div class="text-xs text-gray-400">Entry</div><div class="font-bold text-lg">$${s.entry_price}</div></div>
                        <div class="bg-red-900/30 rounded p-2 border border-red-900"><div class="text-xs text-red-400">Stop Loss</div><div class="font-bold text-red-400">$${s.stop_loss}</div><div class="text-xs text-gray-500">Risk: $${s.risk_dollars}</div></div>
                    </div>
                    <div class="grid grid-cols-3 gap-1 mb-3 text-xs">
                        <div class="bg-green-900/30 rounded p-2 text-center border border-green-900"><div class="text-green-400">TP1</div><div class="font-bold text-green-400">$${s.tp1}</div></div>
                        <div class="bg-green-900/40 rounded p-2 text-center border border-green-700"><div class="text-green-300">TP2</div><div class="font-bold text-green-300">$${s.tp2}</div></div>
                        <div class="bg-green-900/50 rounded p-2 text-center border border-green-500"><div class="text-green-200">TP3</div><div class="font-bold text-green-200">$${s.tp3}</div></div>
                    </div>
                    <div class="mb-2"><div class="flex justify-between text-xs mb-1"><span>Confluence</span><span class="font-bold">${s.confluence_score}/15 | R:R ${s.risk_reward}:1</span></div>
                    <div class="bg-gray-700 rounded-full h-2"><div class="h-2 rounded-full transition-all ${s.confluence_score>=10?'bg-green-500':s.confluence_score>=7?'bg-yellow-500':'bg-orange-500'}" style="width:${scoreW}%"></div></div></div>
                    <div class="flex flex-wrap gap-1">${(s.reasons||[]).slice(0,8).map(r=>`<span class="bg-gray-700/80 px-2 py-0.5 rounded text-xs">${r}</span>`).join('')}</div>
                </div>`;
            }
            document.getElementById('signals').innerHTML=html;document.getElementById('signalCount').textContent=list.length;
        }

        function renderHistory(){
            if(!history.length){document.getElementById('history').innerHTML='<tr><td colspan="11" class="px-3 py-4 text-center text-gray-500">No signals yet...</td></tr>';return;}
            document.getElementById('history').innerHTML=history.slice(0,30).map(s=>`
                <tr class="border-t border-gray-700 hover:bg-gray-700/30"><td class="px-3 py-2 text-xs text-gray-400">${s.timestamp}</td><td class="px-3 py-2 font-bold">${s.symbol}</td>
                <td class="px-3 py-2"><span class="${s.direction==='BUY'?'text-green-400 bg-green-900/30':'text-red-400 bg-red-900/30'} px-2 py-1 rounded text-xs font-bold">${s.direction}</span></td>
                <td class="px-3 py-2 font-mono">$${s.entry_price}</td><td class="px-3 py-2 text-red-400 font-mono">$${s.stop_loss}</td>
                <td class="px-3 py-2 text-green-400 font-mono">$${s.tp1}</td><td class="px-3 py-2 text-green-300 font-mono">$${s.tp2}</td><td class="px-3 py-2 text-green-200 font-mono">$${s.tp3}</td>
                <td class="px-3 py-2 font-bold">${s.risk_reward}:1</td>
                <td class="px-3 py-2"><span class="px-2 py-0.5 rounded text-xs ${s.confluence_score>=10?'bg-green-900 text-green-400':s.confluence_score>=7?'bg-yellow-900 text-yellow-400':'bg-gray-700 text-gray-300'}">${s.confluence_score}/15</span></td>
                <td class="px-3 py-2 text-xs">${s.market_structure}</td></tr>`).join('');
        }

        function showAlert(signal){
            document.getElementById('alertTitle').textContent=`🎯 NEW ${signal.direction} SIGNAL!`;
            document.getElementById('alertText').textContent=`${signal.symbol} @ $${signal.entry_price} | SL: $${signal.stop_loss} | TP2: $${signal.tp2} | Score: ${signal.confluence_score}/15`;
            document.getElementById('alertArea').classList.remove('hidden');
            try{const ctx=new(window.AudioContext||window.webkitAudioContext)();const o=ctx.createOscillator();const g=ctx.createGain();o.connect(g);g.connect(ctx.destination);o.frequency.value=signal.direction==='BUY'?880:660;g.gain.value=0.15;o.start();o.stop(ctx.currentTime+0.3);}catch(e){}
            setTimeout(()=>document.getElementById('alertArea').classList.add('hidden'),30000);
        }

        socket.on('connect',()=>{connected=true;document.getElementById('status').innerHTML='<span class="text-green-400 pulse">● CONNECTED</span>';log('Connected to UTS Pro server','system');});
        socket.on('disconnect',(r)=>{connected=false;document.getElementById('status').innerHTML='<span class="text-red-400">● DISCONNECTED</span>';log(`Disconnected: ${r}`,'error');});
        socket.on('reconnect_attempt',(n)=>{document.getElementById('status').innerHTML=`<span class="text-yellow-400 pulse">● Reconnecting (${n})...</span>`;});

        socket.on('initial_state',(d)=>{
            log('Received initial state','system');
            prices=d.prices||{};signals={};
            if(d.signals)for(const[k,v]of Object.entries(d.signals))signals[k]=v;
            history=d.history||[];
            document.getElementById('scanCount').textContent=d.scan_count||0;
            document.getElementById('lastScan').textContent=d.last_scan||'--:--';
            document.getElementById('scannerStatus').textContent=d.scanner_status||'RUNNING';
            if(d.stats){document.getElementById('buyCount').textContent=d.stats.total_buy||0;document.getElementById('sellCount').textContent=d.stats.total_sell||0;}
            renderPrices();renderSignals();renderHistory();
            log(`State: ${Object.keys(prices).length} prices, ${Object.keys(signals).length} signals, ${history.length} history`,'info');
        });

        socket.on('price_update',(d)=>{
            if(!d||!d.symbol)return;prices[d.symbol]=d.data;if(d.analysis)analysisData[d.symbol]=d.analysis;
            renderPrices();renderConfluence();renderTechSummary();
            const now=Date.now();if(!lastPriceUpdate[d.symbol]||now-lastPriceUpdate[d.symbol]>30000){log(`${d.symbol}: $${d.data.price} | Buy:${d.analysis?.buy_score||0}/15 Sell:${d.analysis?.sell_score||0}/15`,'price');lastPriceUpdate[d.symbol]=now;}
        });

        socket.on('new_signal',(d)=>{
            if(!d||!d.signal)return;signals[d.symbol]=d.signal;history.unshift(d.signal);history=history.slice(0,100);
            renderSignals();renderHistory();showAlert(d.signal);
            log(`🎯 NEW: ${d.symbol} ${d.signal.direction} @ $${d.signal.entry_price} | Score: ${d.signal.confluence_score}/15`,'signal');
        });

        socket.on('scan_update',(d)=>{
            if(!d)return;document.getElementById('scanCount').textContent=d.scan_count||0;document.getElementById('lastScan').textContent=d.last_scan||'--:--';
            document.getElementById('scannerStatus').textContent=d.scanner_status||'RUNNING';
            if(d.stats){document.getElementById('buyCount').textContent=d.stats.total_buy||0;document.getElementById('sellCount').textContent=d.stats.total_sell||0;}
            const badge=document.getElementById('scannerBadge');badge.classList.remove('hidden');badge.textContent=`Scan #${d.scan_count} (${d.symbols_ok||0}/${(d.symbols_ok||0)+(d.symbols_fail||0)})`;setTimeout(()=>badge.classList.add('hidden'),5000);
        });

        socket.on('scanner_status',(d)=>{if(d&&d.status){document.getElementById('scannerStatus').textContent=d.status;if(d.message)log(d.message,'warn');}});
        setInterval(()=>{if(!connected)log('Connection lost - reconnecting...','warn');},30000);
    </script>
</body></html>
'''

ADMIN_TEMPLATE = '''
<!DOCTYPE html>
<html><head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>👑 UTS Pro Admin</title><script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 min-h-screen text-white">
    <div class="container mx-auto px-4 py-8 max-w-4xl">
        <div class="flex justify-between items-center mb-8">
            <h1 class="text-2xl font-bold">👑 Admin Panel</h1>
            <div class="flex gap-4"><a href="/dashboard" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg">📊 Dashboard</a><a href="/logout" class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg">🚪 Logout</a></div>
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for cat, msg in messages %}
        <div class="mb-4 p-4 rounded-lg {% if cat == 'error' %}bg-red-900 text-red-200{% else %}bg-green-900 text-green-200{% endif %}">{{ msg }}</div>
        {% endfor %}{% endif %}{% endwith %}
        <div class="bg-gray-800 rounded-xl p-6 mb-6">
            <h2 class="text-xl font-bold mb-4">📊 System Status</h2>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div><span class="text-gray-400">Scans:</span> <span class="font-bold">{{ scan_count }}</span></div>
                <div><span class="text-gray-400">Last Scan:</span> <span class="font-mono">{{ last_scan }}</span></div>
                <div><span class="text-gray-400">Active Signals:</span> <span class="font-bold text-green-400">{{ active_signals }}</span></div>
                <div><span class="text-gray-400">Scanner:</span> <span class="text-blue-400">{{ scanner_status }}</span></div>
            </div>
            {% if errors %}<div class="mt-4"><h3 class="text-sm font-bold text-red-400 mb-2">Recent Errors:</h3>
            <div class="bg-gray-900 rounded p-3 max-h-40 overflow-y-auto text-xs font-mono">{% for err in errors[:10] %}<div class="text-red-300">{{ err }}</div>{% endfor %}</div></div>{% endif %}
        </div>
        <div class="bg-gray-800 rounded-xl p-6 mb-6">
            <h2 class="text-xl font-bold mb-4">➕ Create User</h2>
            <form method="POST" action="/admin/create" class="grid grid-cols-1 md:grid-cols-5 gap-4">
                <input type="text" name="username" placeholder="Username" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white">
                <input type="password" name="password" placeholder="Password (min 6)" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white">
                <input type="text" name="name" placeholder="Display Name" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white">
                <select name="role" class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white"><option value="user">User</option><option value="admin">Admin</option></select>
                <button type="submit" class="px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg font-bold">✅ Create</button>
            </form>
        </div>
        <div class="bg-gray-800 rounded-xl overflow-hidden">
            <h2 class="text-xl font-bold p-6 border-b border-gray-700">👥 Users</h2>
            <table class="w-full"><thead class="bg-gray-700"><tr>
                <th class="px-6 py-3 text-left">Username</th><th class="px-6 py-3 text-left">Name</th>
                <th class="px-6 py-3 text-left">Role</th><th class="px-6 py-3 text-left">Status</th><th class="px-6 py-3 text-left">Actions</th>
            </tr></thead><tbody>
            {% for username, user in users.items() %}<tr class="border-t border-gray-700">
                <td class="px-6 py-4 font-bold">{{ username }}</td><td class="px-6 py-4">{{ user.name }}</td>
                <td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.role == 'admin' %}bg-yellow-600{% else %}bg-blue-600{% endif %}">{{ user.role }}</span></td>
                <td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.active %}bg-green-600{% else %}bg-red-600{% endif %}">{{ 'Active' if user.active else 'Disabled' }}</span></td>
                <td class="px-6 py-4">{% if username != 'admin' %}
                    <form method="POST" action="/admin/toggle/{{ username }}" class="inline"><button class="px-3 py-1 {% if user.active %}bg-orange-600{% else %}bg-green-600{% endif %} rounded text-sm">{{ '🔒' if user.active else '🔓' }}</button></form>
                    <form method="POST" action="/admin/delete/{{ username }}" class="inline ml-2" onsubmit="return confirm('Delete?')"><button class="px-3 py-1 bg-red-600 rounded text-sm">🗑️</button></form>
                {% else %}<span class="text-gray-500 text-sm">Protected</span>{% endif %}</td>
            </tr>{% endfor %}</tbody></table>
        </div>
    </div>
</body></html>
'''

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return redirect(url_for('login') if 'user' not in session else url_for('dashboard'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user' in session: return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')
        if user_manager.verify(username, password):
            session.permanent = True; session['user'] = username
            user_manager.users[username]['last_login'] = format_datetime()
            user_manager.save_users()
            return redirect(url_for('dashboard'))
        flash('Invalid credentials', 'error')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    user = user_manager.users.get(session['user'], {})
    return render_template_string(DASHBOARD_TEMPLATE, is_admin=user.get('role')=='admin', username=user.get('name', session['user']))

@app.route('/admin')
@admin_required
def admin():
    return render_template_string(ADMIN_TEMPLATE, users=user_manager.get_all_users(),
        scan_count=store.scan_count, last_scan=store.last_scan,
        active_signals=len(store.signals), scanner_status=store.scanner_status, errors=store.errors)

@app.route('/admin/create', methods=['POST'])
@admin_required
def admin_create_user():
    success, msg = user_manager.create_user(request.form.get('username','').strip().lower(),
        request.form.get('password',''), request.form.get('name','').strip(), request.form.get('role','user'))
    flash(msg, 'success' if success else 'error'); return redirect(url_for('admin'))

@app.route('/admin/toggle/<username>', methods=['POST'])
@admin_required
def admin_toggle(username):
    if username in user_manager.users and username != 'admin':
        user_manager.users[username]['active'] = not user_manager.users[username].get('active', True)
        user_manager.save_users()
    return redirect(url_for('admin'))

@app.route('/admin/delete/<username>', methods=['POST'])
@admin_required
def admin_delete(username):
    if username in user_manager.users and username != 'admin':
        del user_manager.users[username]; user_manager.save_users()
    return redirect(url_for('admin'))

@app.route('/health')
def health():
    return {'status': 'ok', 'time': format_datetime(), 'scans': store.scan_count,
            'last_scan': store.last_scan, 'scanner': store.scanner_status,
            'prices': {s: p.get('price', 0) for s, p in store.prices.items()},
            'signals': len(store.signals), 'connected': store.connected_clients, 'errors': store.errors[:5]}

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET EVENTS
# ═══════════════════════════════════════════════════════════════════════════════
@socketio.on('connect')
def handle_connect():
    store.connected_clients += 1
    logger.info(f"Client connected (total: {store.connected_clients})")
    emit('initial_state', {
        'prices': store.prices,
        'signals': {k: asdict(v) for k, v in store.signals.items()},
        'history': [asdict(s) for s in store.history[:30]],
        'scan_count': store.scan_count, 'last_scan': store.last_scan,
        'scanner_status': store.scanner_status, 'stats': store.stats
    })

@socketio.on('disconnect')
def handle_disconnect():
    store.connected_clients = max(0, store.connected_clients - 1)
    logger.info(f"Client disconnected (total: {store.connected_clients})")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("=" * 60)
    print(" ⚡ UTS PRO v4 - Ultimate Trading System (FIXED v2)")
    print(" Direct Yahoo Finance API + yfinance fallback")
    print("=" * 60)
    print(f" Time: {format_datetime()}")
    print(f" Min Confluence: {Config.MIN_CONFLUENCE_SCORE}/15 | Min R:R: {Config.MIN_RR_RATIO}:1")
    print(f" Symbols: {', '.join(Config.SYMBOLS.keys())}")
    print(f" Default login: admin / admin123")
    print("=" * 60)

    eventlet.spawn(background_scanner)
    port = int(os.environ.get('PORT', 8000))
    print(f"\n🌐 Starting on port {port}...")
    socketio.run(app, host='0.0.0.0', port=port, debug=False, use_reloader=False)
