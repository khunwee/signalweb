"""
Ultimate Trading System Pro v4 - Web Dashboard
Based on Pine Script UTS Pro v4.0 Indicator
Features: SMC, Multi-TF S/R, Supply/Demand, Market Maker, FVG, Confluence Scoring
"""

from flask import Flask, render_template_string, request, redirect, url_for, session, flash
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict, field
from functools import wraps
import pandas as pd
import numpy as np
import threading
import hashlib
import secrets
import time
import json
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Bangkok timezone (UTC+7)
BANGKOK_TZ = timezone(timedelta(hours=7))

def get_bangkok_time():
    return datetime.now(BANGKOK_TZ)

def format_time(fmt="%H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

def format_datetime(fmt="%Y-%m-%d %H:%M:%S"):
    return get_bangkok_time().strftime(fmt)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION - Matching Pine Script Settings
# ═══════════════════════════════════════════════════════════════════════════════
class Config:
    # Main Settings
    ATR_PERIOD = 14
    PIVOT_LOOKBACK = 10
    ZONE_LOOKBACK = 100
    SWING_STRENGTH = 5
    
    # Signal Settings
    MIN_CONFLUENCE_SCORE = 5  # Out of 15
    MIN_RR_RATIO = 1.5
    SIGNAL_EXPIRY_BARS = 30
    SR_CONFLUENCE_BOOST = 2
    
    # Risk Management
    ATR_STOP_MULT = 1.5
    RR_RATIO = 2.0
    MAX_RISK_DOLLARS = 15.0
    
    # Market Maker Settings
    MM_CANDLE_SIZE_ATR = 1.5
    MM_BODY_RATIO = 0.6
    REV_WICK_RATIO = 0.6
    
    # FVG Settings
    FVG_MIN_SIZE_ATR = 0.5
    MAX_FVG = 10
    
    # RSI/ADX Filters
    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    ADX_PERIOD = 14
    ADX_THRESHOLD = 25
    EMA_PERIOD = 50
    
    # Scan Settings
    SCAN_INTERVAL = 30
    
    SYMBOLS = {
        "XAUUSD": {"yf": "GC=F", "name": "Gold", "emoji": "🥇"},
        "XAGUSD": {"yf": "SI=F", "name": "Silver", "emoji": "🥈"},
        "USOUSD": {"yf": "CL=F", "name": "Oil", "emoji": "🛢️"},
    }

# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class SRLevel:
    price: float
    name: str
    level_type: str  # daily, weekly, monthly, h4, pivot, fib
    strength: int

@dataclass
class Zone:
    top: float
    bottom: float
    zone_type: str  # supply, demand
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
    session: str
    market_structure: str
    premium_discount: str
    sr_levels_near: int
    timestamp: str
    timestamp_unix: float

@dataclass
class MarketAnalysis:
    symbol: str
    price: float
    atr: float
    rsi: float
    adx: float
    ema50: float
    ema200: float
    market_structure: str  # BULLISH, BEARISH, RANGING
    trend: str  # UP, DOWN
    premium_discount: str  # PREMIUM, DISCOUNT, EQUILIBRIUM
    in_kill_zone: bool
    session: str
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
        self.stats = {
            'total_buy': 0, 'total_sell': 0,
            'buy_wins': 0, 'buy_losses': 0,
            'sell_wins': 0, 'sell_losses': 0
        }

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
        except: pass
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
        except: pass
    
    def hash_password(self, password):
        return hashlib.sha256(f"{password}uts_pro_2024".encode()).hexdigest()
    
    def verify(self, username, password):
        if username not in self.users: return False
        user = self.users[username]
        return user.get('active', True) and user['password'] == self.hash_password(password)
    
    def create_user(self, username, password, name, role='user'):
        if username in self.users: return False, "User exists"
        if len(password) < 6: return False, "Password too short"
        self.users[username] = {
            'password': self.hash_password(password), 'role': role, 'name': name,
            'created': format_datetime(), 'active': True, 'last_login': None
        }
        self.save_users()
        return True, "Created"
    
    def get_all_users(self):
        return {u: {k:v for k,v in d.items() if k != 'password'} for u, d in self.users.items()}

user_manager = UserManager()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        user = user_manager.users.get(session['user'], {})
        if user.get('role') != 'admin': return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════════════════════
# TECHNICAL ANALYSIS ENGINE - Matching Pine Script Logic
# ═══════════════════════════════════════════════════════════════════════════════
class UTSProAnalyzer:
    """Ultimate Trading System Pro Analyzer - Python Implementation"""
    
    def fetch_data(self, symbol: str, interval: str = "15m", period: str = "5d") -> Optional[pd.DataFrame]:
        """Fetch OHLCV data"""
        try:
            import yfinance as yf
            yf_symbol = Config.SYMBOLS.get(symbol, {}).get("yf", "GC=F")
            df = yf.Ticker(yf_symbol).history(period=period, interval=interval)
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception as e:
            print(f"Fetch error {symbol}: {e}")
        return None
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate ATR"""
        high, low, close = df['high'], df['low'], df['close'].shift(1)
        tr = pd.concat([high-low, (high-close).abs(), (low-close).abs()], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate RSI"""
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
        rs = gain / (loss + 0.0001)
        return 100 - (100 / (1 + rs))
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate ADX, +DI, -DI"""
        high, low = df['high'], df['low']
        close_prev = df['close'].shift(1)
        
        tr = pd.concat([high-low, (high-close_prev).abs(), (low-close_prev).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        plus_dm = high.diff().where(lambda x: x > 0, 0)
        minus_dm = (-low.diff()).where(lambda x: x > 0, 0)
        
        plus_di = 100 * (plus_dm.rolling(period).mean() / (atr + 0.0001))
        minus_di = 100 * (minus_dm.rolling(period).mean() / (atr + 0.0001))
        
        dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 0.0001))
        adx = dx.rolling(period).mean()
        
        return adx, plus_di, minus_di
    
    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        """Calculate EMA"""
        return df['close'].ewm(span=period, adjust=False).mean()
    
    def find_swing_points(self, df: pd.DataFrame, strength: int = 5) -> Tuple[List[float], List[float]]:
        """Find swing highs and lows"""
        swing_highs, swing_lows = [], []
        
        if len(df) < strength * 2 + 1:
            return swing_highs, swing_lows
        
        for i in range(strength, len(df) - strength):
            # Swing High
            is_swing_high = True
            for j in range(1, strength + 1):
                if df['high'].iloc[i] <= df['high'].iloc[i-j] or df['high'].iloc[i] <= df['high'].iloc[i+j]:
                    is_swing_high = False
                    break
            if is_swing_high:
                swing_highs.append(df['high'].iloc[i])
            
            # Swing Low
            is_swing_low = True
            for j in range(1, strength + 1):
                if df['low'].iloc[i] >= df['low'].iloc[i-j] or df['low'].iloc[i] >= df['low'].iloc[i+j]:
                    is_swing_low = False
                    break
            if is_swing_low:
                swing_lows.append(df['low'].iloc[i])
        
        return swing_highs, swing_lows
    
    def detect_market_structure(self, swing_highs: List[float], swing_lows: List[float]) -> str:
        """Detect market structure: BULLISH, BEARISH, RANGING"""
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return "RANGING"
        
        # Check last two swings
        hh = swing_highs[-1] > swing_highs[-2] if len(swing_highs) >= 2 else False
        hl = swing_lows[-1] > swing_lows[-2] if len(swing_lows) >= 2 else False
        lh = swing_highs[-1] < swing_highs[-2] if len(swing_highs) >= 2 else False
        ll = swing_lows[-1] < swing_lows[-2] if len(swing_lows) >= 2 else False
        
        if hh and hl:
            return "BULLISH"
        elif lh and ll:
            return "BEARISH"
        return "RANGING"
    
    def detect_bos_choch(self, df: pd.DataFrame, swing_highs: List[float], swing_lows: List[float], 
                         market_structure: str) -> Tuple[bool, bool, bool, bool]:
        """Detect Break of Structure and Change of Character"""
        if len(df) < 2:
            return False, False, False, False
        
        close = df['close'].iloc[-1]
        close_prev = df['close'].iloc[-2]
        
        bullish_bos = False
        bearish_bos = False
        bullish_choch = False
        bearish_choch = False
        
        if swing_highs and swing_lows:
            last_high = swing_highs[-1] if swing_highs else 0
            last_low = swing_lows[-1] if swing_lows else 0
            
            # BOS - Break in direction of trend
            if market_structure == "BULLISH" and close > last_high and close_prev <= last_high:
                bullish_bos = True
            if market_structure == "BEARISH" and close < last_low and close_prev >= last_low:
                bearish_bos = True
            
            # CHOCH - Break against trend (reversal)
            if market_structure == "BEARISH" and close > last_high and close_prev <= last_high:
                bullish_choch = True
            if market_structure == "BULLISH" and close < last_low and close_prev >= last_low:
                bearish_choch = True
        
        return bullish_bos, bearish_bos, bullish_choch, bearish_choch
    
    def detect_fvg(self, df: pd.DataFrame, atr: float) -> List[FVG]:
        """Detect Fair Value Gaps"""
        fvgs = []
        min_size = atr * Config.FVG_MIN_SIZE_ATR
        
        for i in range(2, min(len(df), 50)):
            # Bullish FVG: gap between current low and 2-bars-ago high
            if df['low'].iloc[-i] > df['high'].iloc[-i-2]:
                gap_size = df['low'].iloc[-i] - df['high'].iloc[-i-2]
                if gap_size >= min_size:
                    fvg = FVG(
                        top=df['low'].iloc[-i],
                        bottom=df['high'].iloc[-i-2],
                        bullish=True,
                        mitigated=df['low'].iloc[-1] <= df['low'].iloc[-i]
                    )
                    if not fvg.mitigated:
                        fvgs.append(fvg)
            
            # Bearish FVG
            if df['high'].iloc[-i] < df['low'].iloc[-i-2]:
                gap_size = df['low'].iloc[-i-2] - df['high'].iloc[-i]
                if gap_size >= min_size:
                    fvg = FVG(
                        top=df['low'].iloc[-i-2],
                        bottom=df['high'].iloc[-i],
                        bullish=False,
                        mitigated=df['high'].iloc[-1] >= df['high'].iloc[-i]
                    )
                    if not fvg.mitigated:
                        fvgs.append(fvg)
            
            if len(fvgs) >= Config.MAX_FVG:
                break
        
        return fvgs
    
    def detect_order_blocks(self, df: pd.DataFrame, atr: float) -> List[OrderBlock]:
        """Detect Order Blocks"""
        obs = []
        
        for i in range(3, min(len(df), 30)):
            # Bullish OB: bearish candle followed by bullish breakout
            if (df['close'].iloc[-i-1] < df['open'].iloc[-i-1] and  # Previous bearish
                df['close'].iloc[-i] > df['open'].iloc[-i] and      # Current bullish
                df['close'].iloc[-i] > df['high'].iloc[-i-1] and    # Breaks above
                abs(df['close'].iloc[-i] - df['open'].iloc[-i]) > atr * 0.5):
                
                ob = OrderBlock(
                    top=df['high'].iloc[-i-1],
                    bottom=df['low'].iloc[-i-1],
                    bullish=True,
                    mitigated=df['low'].iloc[-1] <= df['low'].iloc[-i-1]
                )
                if not ob.mitigated:
                    obs.append(ob)
            
            # Bearish OB
            if (df['close'].iloc[-i-1] > df['open'].iloc[-i-1] and  # Previous bullish
                df['close'].iloc[-i] < df['open'].iloc[-i] and      # Current bearish
                df['close'].iloc[-i] < df['low'].iloc[-i-1] and     # Breaks below
                abs(df['close'].iloc[-i] - df['open'].iloc[-i]) > atr * 0.5):
                
                ob = OrderBlock(
                    top=df['high'].iloc[-i-1],
                    bottom=df['low'].iloc[-i-1],
                    bullish=False,
                    mitigated=df['high'].iloc[-1] >= df['high'].iloc[-i-1]
                )
                if not ob.mitigated:
                    obs.append(ob)
            
            if len(obs) >= 10:
                break
        
        return obs
    
    def detect_supply_demand_zones(self, swing_highs: List[float], swing_lows: List[float], 
                                    atr: float, current_price: float) -> Tuple[List[Zone], List[Zone]]:
        """Detect Supply and Demand Zones"""
        supply_zones = []
        demand_zones = []
        zone_thickness = atr * 1.5
        
        # Supply zones from swing highs
        for sh in sorted(swing_highs, reverse=True)[:5]:
            if sh > current_price:
                supply_zones.append(Zone(
                    top=sh + zone_thickness * 0.3,
                    bottom=sh - zone_thickness * 0.7,
                    zone_type="supply",
                    strength=2 if sh == max(swing_highs) else 1,
                    fresh=True
                ))
        
        # Demand zones from swing lows
        for sl in sorted(swing_lows)[:5]:
            if sl < current_price:
                demand_zones.append(Zone(
                    top=sl + zone_thickness * 0.7,
                    bottom=sl - zone_thickness * 0.3,
                    zone_type="demand",
                    strength=2 if sl == min(swing_lows) else 1,
                    fresh=True
                ))
        
        return supply_zones, demand_zones
    
    def calculate_sr_levels(self, df_daily: pd.DataFrame, df_weekly: pd.DataFrame, 
                           df_h4: pd.DataFrame, current_price: float) -> List[SRLevel]:
        """Calculate Multi-TF Support/Resistance Levels"""
        levels = []
        
        # Daily levels
        if df_daily is not None and len(df_daily) >= 3:
            for i in range(1, min(4, len(df_daily))):
                levels.append(SRLevel(df_daily['high'].iloc[-i], f"D H[{i}]", "daily", 3))
                levels.append(SRLevel(df_daily['low'].iloc[-i], f"D L[{i}]", "daily", 3))
        
        # Weekly levels
        if df_weekly is not None and len(df_weekly) >= 2:
            for i in range(1, min(3, len(df_weekly))):
                levels.append(SRLevel(df_weekly['high'].iloc[-i], f"W H[{i}]", "weekly", 4))
                levels.append(SRLevel(df_weekly['low'].iloc[-i], f"W L[{i}]", "weekly", 4))
        
        # H4 levels
        if df_h4 is not None and len(df_h4) >= 4:
            for i in range(1, min(5, len(df_h4))):
                levels.append(SRLevel(df_h4['high'].iloc[-i], f"H4 H[{i}]", "h4", 2))
                levels.append(SRLevel(df_h4['low'].iloc[-i], f"H4 L[{i}]", "h4", 2))
        
        # Pivot Points (from daily)
        if df_daily is not None and len(df_daily) >= 2:
            h, l, c = df_daily['high'].iloc[-2], df_daily['low'].iloc[-2], df_daily['close'].iloc[-2]
            pivot = (h + l + c) / 3
            r1, s1 = 2 * pivot - l, 2 * pivot - h
            r2, s2 = pivot + (h - l), pivot - (h - l)
            
            levels.append(SRLevel(pivot, "Pivot", "pivot", 3))
            levels.append(SRLevel(r1, "R1", "pivot", 2))
            levels.append(SRLevel(s1, "S1", "pivot", 2))
            levels.append(SRLevel(r2, "R2", "pivot", 2))
            levels.append(SRLevel(s2, "S2", "pivot", 2))
        
        # Fibonacci levels
        if df_daily is not None and len(df_daily) >= 20:
            fib_high = df_daily['high'].tail(20).max()
            fib_low = df_daily['low'].tail(20).min()
            fib_range = fib_high - fib_low
            
            if current_price > (fib_high + fib_low) / 2:  # Uptrend
                levels.append(SRLevel(fib_high - fib_range * 0.236, "Fib 23.6%", "fib", 2))
                levels.append(SRLevel(fib_high - fib_range * 0.382, "Fib 38.2%", "fib", 2))
                levels.append(SRLevel(fib_high - fib_range * 0.5, "Fib 50%", "fib", 3))
                levels.append(SRLevel(fib_high - fib_range * 0.618, "Fib 61.8%", "fib", 3))
            else:  # Downtrend
                levels.append(SRLevel(fib_low + fib_range * 0.236, "Fib 23.6%", "fib", 2))
                levels.append(SRLevel(fib_low + fib_range * 0.382, "Fib 38.2%", "fib", 2))
                levels.append(SRLevel(fib_low + fib_range * 0.5, "Fib 50%", "fib", 3))
                levels.append(SRLevel(fib_low + fib_range * 0.618, "Fib 61.8%", "fib", 3))
        
        return levels
    
    def detect_market_maker_candle(self, df: pd.DataFrame, atr: float) -> Tuple[str, bool]:
        """Detect Market Maker and Reversal candles"""
        if len(df) < 2:
            return "NONE", False
        
        latest = df.iloc[-1]
        candle_range = latest['high'] - latest['low']
        body = abs(latest['close'] - latest['open'])
        
        if candle_range == 0:
            return "NONE", False
        
        body_ratio = body / candle_range
        upper_wick = latest['high'] - max(latest['open'], latest['close'])
        lower_wick = min(latest['open'], latest['close']) - latest['low']
        upper_ratio = upper_wick / candle_range
        lower_ratio = lower_wick / candle_range
        is_bullish = latest['close'] > latest['open']
        
        # Market Maker candle (large body, strong momentum)
        if body_ratio >= Config.MM_BODY_RATIO and candle_range >= atr * Config.MM_CANDLE_SIZE_ATR:
            return "MM_BULL" if is_bullish else "MM_BEAR", True
        
        # Reversal candle (long wick)
        if body_ratio <= 0.4:
            if lower_ratio >= Config.REV_WICK_RATIO:
                return "REV_BULL", True
            if upper_ratio >= Config.REV_WICK_RATIO:
                return "REV_BEAR", True
        
        return "NONE", False
    
    def detect_candlestick_patterns(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Detect candlestick patterns"""
        patterns = {
            'bullish_engulfing': False, 'bearish_engulfing': False,
            'hammer': False, 'shooting_star': False,
            'strong_bullish': False, 'strong_bearish': False
        }
        
        if len(df) < 2:
            return patterns
        
        curr, prev = df.iloc[-1], df.iloc[-2]
        body = abs(curr['close'] - curr['open'])
        total_range = curr['high'] - curr['low']
        
        if total_range == 0:
            return patterns
        
        body_pct = body / total_range
        lower_wick = min(curr['close'], curr['open']) - curr['low']
        upper_wick = curr['high'] - max(curr['close'], curr['open'])
        
        # Bullish Engulfing
        if (curr['close'] > curr['open'] and prev['close'] < prev['open'] and
            curr['open'] <= prev['close'] and curr['close'] >= prev['open']):
            patterns['bullish_engulfing'] = True
        
        # Bearish Engulfing
        if (curr['close'] < curr['open'] and prev['close'] > prev['open'] and
            curr['open'] >= prev['close'] and curr['close'] <= prev['open']):
            patterns['bearish_engulfing'] = True
        
        # Hammer
        if curr['close'] > curr['open'] and lower_wick > body * 1.5:
            patterns['hammer'] = True
        
        # Shooting Star
        if curr['close'] < curr['open'] and upper_wick > body * 1.5:
            patterns['shooting_star'] = True
        
        # Strong candles
        if body_pct > 0.6:
            if curr['close'] > curr['open']:
                patterns['strong_bullish'] = True
            else:
                patterns['strong_bearish'] = True
        
        return patterns
    
    def check_session(self) -> Tuple[str, bool]:
        """Check trading session and kill zones (Bangkok time)"""
        hour = get_bangkok_time().hour
        
        if 19 <= hour <= 23:  # London-NY overlap
            return "LONDON-NY ⭐⭐", True
        elif 14 <= hour < 19:  # London
            return "LONDON ⭐", True
        elif 20 <= hour or hour <= 4:  # NY
            return "NEW YORK", True
        elif 8 <= hour <= 14:  # Asia
            return "ASIA", True
        return "OFF-PEAK", False
    
    def calculate_confluence(self, df: pd.DataFrame, direction: str, analysis: MarketAnalysis, 
                            patterns: Dict, mm_type: str, bos_choch: Tuple) -> Tuple[int, Dict, List[str]]:
        """Calculate confluence score (0-15) matching Pine Script logic"""
        score = 0
        details = {}
        reasons = []
        
        latest = df.iloc[-1]
        close = latest['close']
        
        bullish_bos, bearish_bos, bullish_choch, bearish_choch = bos_choch
        
        if direction == "BUY":
            # Trend alignment (+1)
            if close > analysis.ema50:
                score += 1
                details['ema50'] = True
                reasons.append("✅ Above EMA50")
            
            # EMA200 alignment (+1)
            if close > analysis.ema200:
                score += 1
                details['ema200'] = True
                reasons.append("✅ Above EMA200")
            
            # Premium/Discount (+1)
            if analysis.premium_discount == "DISCOUNT":
                score += 1
                details['discount'] = True
                reasons.append("✅ In Discount Zone")
            
            # RSI (+1-2)
            if analysis.rsi < 50:
                score += 1
                details['rsi'] = True
                reasons.append(f"✅ RSI {analysis.rsi:.0f}")
            if analysis.rsi < Config.RSI_OVERSOLD:
                score += 1
                reasons.append("✅ RSI Oversold")
            
            # Volume/ADX (+1)
            if analysis.adx > Config.ADX_THRESHOLD:
                score += 1
                details['adx'] = True
                reasons.append(f"✅ Strong Trend ADX {analysis.adx:.0f}")
            
            # Market Structure (+1)
            if analysis.market_structure == "BULLISH":
                score += 1
                details['structure'] = True
                reasons.append("✅ Bullish Structure")
            
            # Kill Zone (+1)
            if analysis.in_kill_zone:
                score += 1
                details['killzone'] = True
                reasons.append(f"✅ {analysis.session}")
            
            # Demand Zone (+2)
            for zone in analysis.demand_zones:
                if zone.bottom <= close <= zone.top:
                    score += 2
                    details['demand_zone'] = True
                    reasons.append("✅ In Demand Zone")
                    break
            
            # Near SR Support (+2)
            sr_near = sum(1 for sr in analysis.sr_levels if sr.price < close and close - sr.price < analysis.atr * 0.5)
            if sr_near > 0:
                score += min(sr_near, Config.SR_CONFLUENCE_BOOST)
                details['sr_support'] = True
                reasons.append(f"✅ Near {sr_near} SR Support")
            
            # BOS/CHOCH (+1)
            if bullish_bos:
                score += 1
                reasons.append("✅ Bullish BOS")
            if bullish_choch:
                score += 1
                reasons.append("🔄 Bullish CHOCH")
            
            # Candlestick patterns (+1)
            if patterns.get('bullish_engulfing') or patterns.get('hammer'):
                score += 1
                details['pattern'] = True
                if patterns.get('bullish_engulfing'):
                    reasons.append("✅ Bullish Engulfing")
                if patterns.get('hammer'):
                    reasons.append("✅ Hammer")
            
            # Market Maker (+1)
            if mm_type in ["MM_BULL", "REV_BULL"]:
                score += 1
                details['mm'] = True
                reasons.append("✅ MM Bull Signal")
        
        else:  # SELL
            if close < analysis.ema50:
                score += 1
                details['ema50'] = True
                reasons.append("✅ Below EMA50")
            
            if close < analysis.ema200:
                score += 1
                details['ema200'] = True
                reasons.append("✅ Below EMA200")
            
            if analysis.premium_discount == "PREMIUM":
                score += 1
                details['premium'] = True
                reasons.append("✅ In Premium Zone")
            
            if analysis.rsi > 50:
                score += 1
                details['rsi'] = True
                reasons.append(f"✅ RSI {analysis.rsi:.0f}")
            if analysis.rsi > Config.RSI_OVERBOUGHT:
                score += 1
                reasons.append("✅ RSI Overbought")
            
            if analysis.adx > Config.ADX_THRESHOLD:
                score += 1
                details['adx'] = True
                reasons.append(f"✅ Strong Trend ADX {analysis.adx:.0f}")
            
            if analysis.market_structure == "BEARISH":
                score += 1
                details['structure'] = True
                reasons.append("✅ Bearish Structure")
            
            if analysis.in_kill_zone:
                score += 1
                details['killzone'] = True
                reasons.append(f"✅ {analysis.session}")
            
            for zone in analysis.supply_zones:
                if zone.bottom <= close <= zone.top:
                    score += 2
                    details['supply_zone'] = True
                    reasons.append("✅ In Supply Zone")
                    break
            
            sr_near = sum(1 for sr in analysis.sr_levels if sr.price > close and sr.price - close < analysis.atr * 0.5)
            if sr_near > 0:
                score += min(sr_near, Config.SR_CONFLUENCE_BOOST)
                details['sr_resistance'] = True
                reasons.append(f"✅ Near {sr_near} SR Resistance")
            
            if bearish_bos:
                score += 1
                reasons.append("✅ Bearish BOS")
            if bearish_choch:
                score += 1
                reasons.append("🔄 Bearish CHOCH")
            
            if patterns.get('bearish_engulfing') or patterns.get('shooting_star'):
                score += 1
                details['pattern'] = True
                if patterns.get('bearish_engulfing'):
                    reasons.append("✅ Bearish Engulfing")
                if patterns.get('shooting_star'):
                    reasons.append("✅ Shooting Star")
            
            if mm_type in ["MM_BEAR", "REV_BEAR"]:
                score += 1
                details['mm'] = True
                reasons.append("✅ MM Bear Signal")
        
        return score, details, reasons
    
    def calculate_entry_levels(self, direction: str, entry: float, atr: float, 
                              analysis: MarketAnalysis) -> Tuple[float, float, float, float]:
        """Calculate SL and TP levels"""
        if direction == "BUY":
            # SL below demand zone or ATR-based
            if analysis.demand_zones:
                zone_sl = analysis.demand_zones[0].bottom
            else:
                zone_sl = entry - atr * 2
            
            atr_sl = entry - atr * Config.ATR_STOP_MULT
            sl = max(zone_sl, atr_sl, entry - Config.MAX_RISK_DOLLARS)
            
            risk = entry - sl
            tp1 = entry + risk * 1.5
            tp2 = entry + risk * Config.RR_RATIO
            tp3 = entry + risk * 3.0
        else:
            if analysis.supply_zones:
                zone_sl = analysis.supply_zones[0].top
            else:
                zone_sl = entry + atr * 2
            
            atr_sl = entry + atr * Config.ATR_STOP_MULT
            sl = min(zone_sl, atr_sl, entry + Config.MAX_RISK_DOLLARS)
            
            risk = sl - entry
            tp1 = entry - risk * 1.5
            tp2 = entry - risk * Config.RR_RATIO
            tp3 = entry - risk * 3.0
        
        return round(sl, 2), round(tp1, 2), round(tp2, 2), round(tp3, 2)
    
    def analyze(self, symbol: str) -> Optional[Tuple[MarketAnalysis, Optional[Signal]]]:
        """Complete analysis for a symbol"""
        # Fetch multi-timeframe data
        df_15m = self.fetch_data(symbol, "15m", "5d")
        df_1h = self.fetch_data(symbol, "1h", "1mo")
        df_daily = self.fetch_data(symbol, "1d", "3mo")
        df_weekly = self.fetch_data(symbol, "1wk", "6mo")
        df_h4 = self.fetch_data(symbol, "1h", "1mo")  # Approximate H4
        
        if df_15m is None or len(df_15m) < 50:
            return None
        
        # Calculate indicators
        atr = self.calculate_atr(df_15m).iloc[-1]
        rsi = self.calculate_rsi(df_15m).iloc[-1]
        adx, plus_di, minus_di = self.calculate_adx(df_15m)
        adx_val = adx.iloc[-1]
        ema50 = self.calculate_ema(df_15m, 50).iloc[-1]
        ema200 = self.calculate_ema(df_15m, 200).iloc[-1] if len(df_15m) >= 200 else ema50
        
        close = df_15m['close'].iloc[-1]
        
        # Find swing points
        swing_highs, swing_lows = self.find_swing_points(df_15m, Config.SWING_STRENGTH)
        
        # Market structure
        market_structure = self.detect_market_structure(swing_highs, swing_lows)
        
        # BOS/CHOCH
        bos_choch = self.detect_bos_choch(df_15m, swing_highs, swing_lows, market_structure)
        
        # Supply/Demand zones
        supply_zones, demand_zones = self.detect_supply_demand_zones(swing_highs, swing_lows, atr, close)
        
        # FVG and Order Blocks
        fvgs = self.detect_fvg(df_15m, atr)
        obs = self.detect_order_blocks(df_15m, atr)
        
        # SR Levels
        sr_levels = self.calculate_sr_levels(df_daily, df_weekly, df_h4, close)
        
        # Premium/Discount
        if df_daily is not None and len(df_daily) >= 2:
            daily_high = df_daily['high'].iloc[-2]
            daily_low = df_daily['low'].iloc[-2]
            equilibrium = (daily_high + daily_low) / 2
            premium_discount = "PREMIUM" if close > equilibrium else "DISCOUNT" if close < equilibrium else "EQUILIBRIUM"
        else:
            daily_high, daily_low = close, close
            premium_discount = "EQUILIBRIUM"
        
        # Session
        session, in_kill_zone = self.check_session()
        
        # Weekly H/L
        weekly_high = df_weekly['high'].iloc[-2] if df_weekly is not None and len(df_weekly) >= 2 else close
        weekly_low = df_weekly['low'].iloc[-2] if df_weekly is not None and len(df_weekly) >= 2 else close
        
        # Market Maker detection
        mm_type, has_mm = self.detect_market_maker_candle(df_15m, atr)
        
        # Candlestick patterns
        patterns = self.detect_candlestick_patterns(df_15m)
        
        # Create analysis object
        analysis = MarketAnalysis(
            symbol=symbol,
            price=round(close, 2),
            atr=round(atr, 4),
            rsi=round(rsi, 1),
            adx=round(adx_val, 1),
            ema50=round(ema50, 2),
            ema200=round(ema200, 2),
            market_structure=market_structure,
            trend="UP" if close > ema50 else "DOWN",
            premium_discount=premium_discount,
            in_kill_zone=in_kill_zone,
            session=session,
            buy_confluence=0,
            sell_confluence=0,
            sr_levels=sr_levels,
            supply_zones=supply_zones,
            demand_zones=demand_zones,
            fvgs=fvgs,
            order_blocks=obs,
            swing_high=swing_highs[-1] if swing_highs else close,
            swing_low=swing_lows[-1] if swing_lows else close,
            daily_high=daily_high,
            daily_low=daily_low,
            weekly_high=weekly_high,
            weekly_low=weekly_low
        )
        
        # Calculate confluence for both directions
        buy_score, buy_details, buy_reasons = self.calculate_confluence(
            df_15m, "BUY", analysis, patterns, mm_type, bos_choch)
        sell_score, sell_details, sell_reasons = self.calculate_confluence(
            df_15m, "SELL", analysis, patterns, mm_type, bos_choch)
        
        analysis.buy_confluence = buy_score
        analysis.sell_confluence = sell_score
        
        # Determine signal
        signal = None
        
        # Check for BUY signal
        if buy_score >= Config.MIN_CONFLUENCE_SCORE and buy_score > sell_score:
            # Need pattern or MM confirmation
            has_entry_trigger = (patterns.get('bullish_engulfing') or patterns.get('hammer') or 
                               patterns.get('strong_bullish') or mm_type in ["MM_BULL", "REV_BULL"] or
                               bos_choch[0] or bos_choch[2])  # bullish_bos or bullish_choch
            
            if has_entry_trigger:
                sl, tp1, tp2, tp3 = self.calculate_entry_levels("BUY", close, atr, analysis)
                risk = close - sl
                rr = abs(tp2 - close) / risk if risk > 0 else 0
                
                if rr >= Config.MIN_RR_RATIO:
                    strength = "🔥 STRONG" if buy_score >= 10 else "⭐ GOOD" if buy_score >= 7 else "📊 MODERATE"
                    signal = Signal(
                        signal_id=f"{symbol}_BUY_{get_bangkok_time().strftime('%H%M%S')}",
                        symbol=symbol,
                        direction="BUY",
                        strength=strength,
                        entry_price=round(close, 2),
                        stop_loss=sl,
                        tp1=tp1, tp2=tp2, tp3=tp3,
                        risk_dollars=round(risk, 2),
                        risk_reward=round(rr, 2),
                        confluence_score=buy_score,
                        confluence_details=buy_details,
                        reasons=buy_reasons,
                        session=session,
                        market_structure=market_structure,
                        premium_discount=premium_discount,
                        sr_levels_near=len([sr for sr in sr_levels if abs(sr.price - close) < atr]),
                        timestamp=format_datetime(),
                        timestamp_unix=time.time()
                    )
        
        # Check for SELL signal
        elif sell_score >= Config.MIN_CONFLUENCE_SCORE and sell_score > buy_score:
            has_entry_trigger = (patterns.get('bearish_engulfing') or patterns.get('shooting_star') or
                               patterns.get('strong_bearish') or mm_type in ["MM_BEAR", "REV_BEAR"] or
                               bos_choch[1] or bos_choch[3])  # bearish_bos or bearish_choch
            
            if has_entry_trigger:
                sl, tp1, tp2, tp3 = self.calculate_entry_levels("SELL", close, atr, analysis)
                risk = sl - close
                rr = abs(close - tp2) / risk if risk > 0 else 0
                
                if rr >= Config.MIN_RR_RATIO:
                    strength = "🔥 STRONG" if sell_score >= 10 else "⭐ GOOD" if sell_score >= 7 else "📊 MODERATE"
                    signal = Signal(
                        signal_id=f"{symbol}_SELL_{get_bangkok_time().strftime('%H%M%S')}",
                        symbol=symbol,
                        direction="SELL",
                        strength=strength,
                        entry_price=round(close, 2),
                        stop_loss=sl,
                        tp1=tp1, tp2=tp2, tp3=tp3,
                        risk_dollars=round(risk, 2),
                        risk_reward=round(rr, 2),
                        confluence_score=sell_score,
                        confluence_details=sell_details,
                        reasons=sell_reasons,
                        session=session,
                        market_structure=market_structure,
                        premium_discount=premium_discount,
                        sr_levels_near=len([sr for sr in sr_levels if abs(sr.price - close) < atr]),
                        timestamp=format_datetime(),
                        timestamp_unix=time.time()
                    )
        
        return analysis, signal

analyzer = UTSProAnalyzer()

# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SCANNER
# ═══════════════════════════════════════════════════════════════════════════════
def background_scanner():
    time.sleep(5)
    print(f"🚀 UTS Pro Scanner started at {format_time()}")
    
    while True:
        try:
            store.scan_count += 1
            store.last_scan = format_time()
            print(f"\n📡 Scan #{store.scan_count} at {store.last_scan}")
            
            for symbol in Config.SYMBOLS.keys():
                try:
                    result = analyzer.analyze(symbol)
                    
                    if result:
                        analysis, signal = result
                        
                        # Update price
                        store.prices[symbol] = {
                            'symbol': symbol,
                            'price': analysis.price,
                            'high': analysis.daily_high,
                            'low': analysis.daily_low,
                            'change': 0,
                            'change_pct': 0,
                            'time': format_time()
                        }
                        
                        # Update analysis
                        store.analysis[symbol] = analysis
                        
                        # Emit price update
                        socketio.emit('price_update', {
                            'symbol': symbol,
                            'data': store.prices[symbol],
                            'analysis': {
                                'rsi': analysis.rsi,
                                'adx': analysis.adx,
                                'structure': analysis.market_structure,
                                'trend': analysis.trend,
                                'zone': analysis.premium_discount,
                                'buy_score': analysis.buy_confluence,
                                'sell_score': analysis.sell_confluence,
                                'session': analysis.session,
                                'sr_count': len(analysis.sr_levels)
                            }
                        })
                        
                        print(f"  💰 {symbol}: ${analysis.price} | RSI:{analysis.rsi:.0f} | ADX:{analysis.adx:.0f} | {analysis.market_structure}")
                        print(f"     Buy:{analysis.buy_confluence}/15 | Sell:{analysis.sell_confluence}/15 | SR:{len(analysis.sr_levels)}")
                        
                        # Handle signal
                        if signal:
                            old_signal = store.signals.get(symbol)
                            is_new = (not old_signal or 
                                     old_signal.direction != signal.direction or
                                     time.time() - old_signal.timestamp_unix > 1800)
                            
                            if is_new:
                                store.signals[symbol] = signal
                                store.history.insert(0, signal)
                                store.history = store.history[:100]
                                
                                if signal.direction == "BUY":
                                    store.stats['total_buy'] += 1
                                else:
                                    store.stats['total_sell'] += 1
                                
                                socketio.emit('new_signal', {
                                    'symbol': symbol,
                                    'signal': asdict(signal)
                                })
                                
                                print(f"  🎯 NEW SIGNAL: {symbol} {signal.direction} @ ${signal.entry_price}")
                                print(f"     SL: ${signal.stop_loss} | TP1: ${signal.tp1} | TP2: ${signal.tp2}")
                                print(f"     Score: {signal.confluence_score}/15 | R:R: {signal.risk_reward}:1")
                
                except Exception as e:
                    print(f"  ❌ Error {symbol}: {e}")
                
                time.sleep(2)
            
            socketio.emit('scan_update', {
                'scan_count': store.scan_count,
                'last_scan': store.last_scan,
                'connected': store.connected_clients,
                'stats': store.stats
            })
            
        except Exception as e:
            print(f"Scanner error: {e}")
        
        time.sleep(Config.SCAN_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
# HTML TEMPLATES - Add this to the end of app.py
# ═══════════════════════════════════════════════════════════════════════════════

LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html><head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 UTS Pro - Login</title>
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
            <button type="submit" class="w-full py-3 bg-yellow-600 hover:bg-yellow-700 text-white font-bold rounded-lg">🔓 Login</button>
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
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}.pulse{animation:pulse 1s infinite}
        @keyframes glow{0%,100%{box-shadow:0 0 5px #ffd700}50%{box-shadow:0 0 25px #ffd700}}.glow{animation:glow 1.5s infinite}
        @keyframes slideIn{from{transform:translateY(-20px);opacity:0}to{transform:translateY(0);opacity:1}}.slide-in{animation:slideIn .3s}
        .scrollbar::-webkit-scrollbar{width:6px}.scrollbar::-webkit-scrollbar-track{background:#1f2937}.scrollbar::-webkit-scrollbar-thumb{background:#4b5563;border-radius:3px}
    </style>
</head>
<body class="bg-gray-900 text-white min-h-screen">
    <div class="container mx-auto px-4 py-4 max-w-7xl">
        <!-- Header -->
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-3">
            <div>
                <h1 class="text-xl md:text-2xl font-bold flex items-center gap-2">⚡ UTS Pro v4 <span class="text-sm text-yellow-400">Ultimate Trading System</span></h1>
                <p class="text-gray-400 text-sm">SMC • Multi-TF S/R • Supply/Demand • Market Maker • FVG</p>
            </div>
            <div class="flex items-center gap-3">
                <div id="clock" class="bg-gray-800 px-3 py-2 rounded-lg font-mono">TH --:--:--</div>
                <span id="status" class="text-red-400 text-sm">● Connecting...</span>
                {% if is_admin %}<a href="{{ url_for('admin') }}" class="px-3 py-2 bg-yellow-600 hover:bg-yellow-700 rounded-lg text-sm">👑</a>{% endif %}
                <a href="{{ url_for('logout') }}" class="px-3 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm">🚪</a>
            </div>
        </div>

        <!-- Stats Row -->
        <div class="grid grid-cols-3 md:grid-cols-6 gap-2 mb-4">
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Scans</div><div id="scanCount" class="font-bold">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Signals</div><div id="signalCount" class="font-bold text-green-400">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Last Scan</div><div id="lastScan" class="font-mono text-sm">--:--</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Session</div><div id="session" class="text-yellow-400 text-sm">-</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Buy Signals</div><div id="buyCount" class="text-green-400">0</div></div>
            <div class="bg-gray-800 rounded-lg p-2 text-center"><div class="text-gray-400 text-xs">Sell Signals</div><div id="sellCount" class="text-red-400">0</div></div>
        </div>

        <!-- Alert Area -->
        <div id="alertArea" class="hidden mb-4">
            <div class="bg-gradient-to-r from-yellow-900 to-yellow-800 border-2 border-yellow-500 rounded-xl p-4 glow">
                <div class="flex items-center gap-3">
                    <span class="text-3xl">🎯</span>
                    <div><div id="alertTitle" class="text-xl font-bold">NEW SIGNAL!</div><div id="alertText" class="text-yellow-200"></div></div>
                </div>
            </div>
        </div>

        <!-- Main Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
            <!-- Prices + Analysis Column -->
            <div class="lg:col-span-1 space-y-4">
                <h2 class="font-bold flex items-center gap-2">💰 Live Prices <span class="text-xs text-green-400 pulse">● LIVE</span></h2>
                <div id="prices" class="space-y-3"></div>

                <!-- Confluence Panel -->
                <div class="bg-gray-800 rounded-xl p-4">
                    <h3 class="font-bold mb-3 text-yellow-400">🎯 Confluence Check</h3>
                    <div id="confluence" class="space-y-2 text-sm"></div>
                </div>
            </div>

            <!-- Signals Column -->
            <div class="lg:col-span-2">
                <h2 class="font-bold mb-3">🎯 Active Trading Signals</h2>
                <div id="signals" class="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <div class="bg-gray-800 rounded-xl p-6 text-center text-gray-400 col-span-full">
                        <div class="text-4xl mb-2">📡</div>
                        <div>Scanning for high-probability setups...</div>
                        <div class="text-sm mt-2">Min Confluence: 5/15 | Min R:R: 1.5:1</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- History Table -->
        <div class="bg-gray-800 rounded-xl overflow-hidden mb-4">
            <h2 class="font-bold p-4 border-b border-gray-700">📜 Signal History</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead class="bg-gray-700">
                        <tr>
                            <th class="px-3 py-2 text-left">Time</th>
                            <th class="px-3 py-2 text-left">Symbol</th>
                            <th class="px-3 py-2 text-left">Dir</th>
                            <th class="px-3 py-2 text-left">Entry</th>
                            <th class="px-3 py-2 text-left">SL</th>
                            <th class="px-3 py-2 text-left">TP1</th>
                            <th class="px-3 py-2 text-left">TP2</th>
                            <th class="px-3 py-2 text-left">R:R</th>
                            <th class="px-3 py-2 text-left">Score</th>
                            <th class="px-3 py-2 text-left">Structure</th>
                        </tr>
                    </thead>
                    <tbody id="history"></tbody>
                </table>
            </div>
        </div>

        <!-- Event Log -->
        <div class="bg-gray-800 rounded-xl p-4">
            <h2 class="font-bold mb-2">📡 Event Log</h2>
            <div id="log" class="h-32 overflow-y-auto font-mono text-xs scrollbar"></div>
        </div>

        <div class="mt-4 text-center text-gray-500 text-xs">
            ⚠️ Educational purposes only. Not financial advice. Based on UTS Pro v4 Pine Script Strategy.
        </div>
    </div>

    <script>
        const socket = io();
        let prices = {}, signals = {}, history = [], analysis = {};

        // Clock
        function updateClock() {
            const bkk = new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Bangkok"}));
            document.getElementById('clock').textContent = 'TH ' + bkk.toLocaleTimeString('en-GB');
            const h = bkk.getHours();
            document.getElementById('session').textContent = 
                h >= 19 && h <= 23 ? 'LONDON-NY ⭐⭐' : h >= 14 && h < 19 ? 'LONDON ⭐' : 
                h >= 20 || h <= 4 ? 'NEW YORK' : h >= 8 && h <= 14 ? 'ASIA' : 'OFF-PEAK';
        }
        setInterval(updateClock, 1000); updateClock();

        function log(msg, type='info') {
            const el = document.getElementById('log');
            const t = new Date().toLocaleTimeString('en-GB', {timeZone: 'Asia/Bangkok'});
            const colors = {signal:'text-green-400', price:'text-blue-400', error:'text-red-400', info:'text-gray-400'};
            el.innerHTML = `<div class="${colors[type]||'text-gray-400'}">[${t}] ${msg}</div>` + el.innerHTML;
        }

        function renderPrices() {
            const syms = {XAUUSD: ['🥇', 'GOLD'], XAGUSD: ['🥈', 'SILVER'], USOUSD: ['🛢️', 'OIL']};
            let html = '';
            for (const [sym, [emoji, name]] of Object.entries(syms)) {
                const p = prices[sym];
                const a = analysis[sym] || {};
                if (!p) { html += `<div class="bg-gray-700 rounded-lg p-3 text-gray-400">${emoji} ${name} - Loading...</div>`; continue; }
                
                const structColor = a.structure === 'BULLISH' ? 'text-green-400' : a.structure === 'BEARISH' ? 'text-red-400' : 'text-gray-400';
                const zoneColor = a.zone === 'PREMIUM' ? 'text-red-400' : a.zone === 'DISCOUNT' ? 'text-green-400' : 'text-gray-400';
                
                html += `
                <div class="bg-gray-700 rounded-lg p-3">
                    <div class="flex justify-between items-center mb-2">
                        <span class="font-bold">${emoji} ${name}</span>
                        <span class="text-2xl font-bold">$${p.price?.toLocaleString() || '-'}</span>
                    </div>
                    <div class="grid grid-cols-2 gap-2 text-xs">
                        <div>RSI: <span class="${a.rsi < 30 ? 'text-green-400' : a.rsi > 70 ? 'text-red-400' : ''}">${a.rsi || '-'}</span></div>
                        <div>ADX: <span class="${a.adx > 25 ? 'text-green-400' : ''}">${a.adx || '-'}</span></div>
                        <div>Struct: <span class="${structColor}">${a.structure || '-'}</span></div>
                        <div>Zone: <span class="${zoneColor}">${a.zone || '-'}</span></div>
                        <div class="text-green-400">Buy: ${a.buy_score || 0}/15</div>
                        <div class="text-red-400">Sell: ${a.sell_score || 0}/15</div>
                    </div>
                    <div class="mt-2 text-xs text-gray-500">SR Levels: ${a.sr_count || 0} | ${a.session || '-'}</div>
                </div>`;
            }
            document.getElementById('prices').innerHTML = html;
        }

        function renderConfluence() {
            let html = '';
            for (const [sym, a] of Object.entries(analysis)) {
                const buyOk = a.buy_score >= 5;
                const sellOk = a.sell_score >= 5;
                const emoji = Config?.SYMBOLS?.[sym]?.emoji || '📊';
                
                html += `
                <div class="flex justify-between items-center py-1 border-b border-gray-700">
                    <span>${sym}</span>
                    <div class="flex gap-2">
                        <span class="px-2 py-1 rounded ${buyOk ? 'bg-green-900 text-green-400' : 'bg-gray-700 text-gray-500'}">BUY ${a.buy_score}/15</span>
                        <span class="px-2 py-1 rounded ${sellOk ? 'bg-red-900 text-red-400' : 'bg-gray-700 text-gray-500'}">SELL ${a.sell_score}/15</span>
                    </div>
                </div>`;
            }
            document.getElementById('confluence').innerHTML = html || '<div class="text-gray-500">Waiting for analysis...</div>';
        }

        function renderSignals() {
            const list = Object.values(signals);
            if (!list.length) {
                document.getElementById('signals').innerHTML = `
                    <div class="bg-gray-800 rounded-xl p-6 text-center text-gray-400 col-span-full">
                        <div class="text-4xl mb-2">📡</div><div>Scanning for signals...</div>
                    </div>`;
                document.getElementById('signalCount').textContent = '0';
                return;
            }

            let html = '';
            for (const s of list) {
                const isBuy = s.direction === 'BUY';
                const border = isBuy ? 'border-green-500' : 'border-red-500';
                const bg = isBuy ? 'from-green-900/30' : 'from-red-900/30';
                const dir = isBuy ? 'text-green-400' : 'text-red-400';

                html += `
                <div class="bg-gradient-to-br ${bg} to-gray-800 rounded-xl p-4 border-l-4 ${border} slide-in">
                    <div class="flex justify-between items-start mb-3">
                        <div>
                            <div class="text-xl font-bold ${dir}">${isBuy ? '🟢' : '🔴'} ${s.symbol} ${s.direction}</div>
                            <div class="text-xs text-gray-400">${s.session} | ${s.market_structure} | ${s.premium_discount}</div>
                        </div>
                        <div class="text-right">
                            <span class="bg-yellow-600/80 px-2 py-1 rounded text-xs">${s.strength}</span>
                            <div class="text-xs text-gray-400 mt-1">${s.timestamp}</div>
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-2 mb-3 text-sm">
                        <div class="bg-gray-900/50 rounded p-2">
                            <div class="text-xs text-gray-400">Entry</div>
                            <div class="font-bold">$${s.entry_price}</div>
                        </div>
                        <div class="bg-red-900/30 rounded p-2 border border-red-900">
                            <div class="text-xs text-red-400">Stop Loss</div>
                            <div class="font-bold text-red-400">$${s.stop_loss}</div>
                            <div class="text-xs text-gray-500">Risk: $${s.risk_dollars}</div>
                        </div>
                    </div>

                    <div class="grid grid-cols-3 gap-1 mb-3 text-xs">
                        <div class="bg-green-900/30 rounded p-2 text-center border border-green-900">
                            <div class="text-green-400">TP1</div>
                            <div class="font-bold text-green-400">$${s.tp1}</div>
                        </div>
                        <div class="bg-green-900/40 rounded p-2 text-center border border-green-700">
                            <div class="text-green-300">TP2</div>
                            <div class="font-bold text-green-300">$${s.tp2}</div>
                        </div>
                        <div class="bg-green-900/50 rounded p-2 text-center border border-green-500">
                            <div class="text-green-200">TP3</div>
                            <div class="font-bold text-green-200">$${s.tp3}</div>
                        </div>
                    </div>

                    <div class="mb-2">
                        <div class="flex justify-between text-xs mb-1">
                            <span>Confluence Score</span>
                            <span class="font-bold">${s.confluence_score}/15 | R:R ${s.risk_reward}:1</span>
                        </div>
                        <div class="bg-gray-700 rounded-full h-2">
                            <div class="bg-yellow-500 h-2 rounded-full" style="width:${(s.confluence_score/15)*100}%"></div>
                        </div>
                    </div>

                    <div class="flex flex-wrap gap-1">
                        ${s.reasons.slice(0,6).map(r => `<span class="bg-gray-700 px-2 py-1 rounded text-xs">${r}</span>`).join('')}
                    </div>
                </div>`;
            }
            document.getElementById('signals').innerHTML = html;
            document.getElementById('signalCount').textContent = list.length;
        }

        function renderHistory() {
            if (!history.length) {
                document.getElementById('history').innerHTML = '<tr><td colspan="10" class="px-3 py-4 text-center text-gray-400">No signals yet...</td></tr>';
                return;
            }
            document.getElementById('history').innerHTML = history.slice(0, 20).map(s => `
                <tr class="border-t border-gray-700 hover:bg-gray-700/30">
                    <td class="px-3 py-2 text-xs">${s.timestamp}</td>
                    <td class="px-3 py-2 font-bold">${s.symbol}</td>
                    <td class="px-3 py-2"><span class="${s.direction === 'BUY' ? 'text-green-400 bg-green-900/30' : 'text-red-400 bg-red-900/30'} px-2 py-1 rounded">${s.direction}</span></td>
                    <td class="px-3 py-2">$${s.entry_price}</td>
                    <td class="px-3 py-2 text-red-400">$${s.stop_loss}</td>
                    <td class="px-3 py-2 text-green-400">$${s.tp1}</td>
                    <td class="px-3 py-2 text-green-300">$${s.tp2}</td>
                    <td class="px-3 py-2 font-bold">${s.risk_reward}:1</td>
                    <td class="px-3 py-2">${s.confluence_score}/15</td>
                    <td class="px-3 py-2 text-xs">${s.market_structure}</td>
                </tr>`).join('');
        }

        function showAlert(signal) {
            document.getElementById('alertTitle').textContent = `🎯 NEW ${signal.direction} SIGNAL!`;
            document.getElementById('alertText').textContent = 
                `${signal.symbol} @ $${signal.entry_price} | SL: $${signal.stop_loss} | TP2: $${signal.tp2} | Score: ${signal.confluence_score}/15 | R:R: ${signal.risk_reward}:1`;
            document.getElementById('alertArea').classList.remove('hidden');
            try {
                const ctx = new AudioContext();
                const o = ctx.createOscillator();
                const g = ctx.createGain();
                o.connect(g); g.connect(ctx.destination);
                o.frequency.value = signal.direction === 'BUY' ? 880 : 660;
                g.gain.value = 0.15;
                o.start(); o.stop(ctx.currentTime + 0.3);
            } catch(e) {}
            setTimeout(() => document.getElementById('alertArea').classList.add('hidden'), 20000);
        }

        // Socket events
        socket.on('connect', () => {
            document.getElementById('status').innerHTML = '<span class="text-green-400 pulse">● CONNECTED</span>';
            log('Connected to UTS Pro server');
        });

        socket.on('disconnect', () => {
            document.getElementById('status').innerHTML = '<span class="text-red-400">● DISCONNECTED</span>';
            log('Disconnected', 'error');
        });

        socket.on('initial_state', (d) => {
            prices = d.prices || {};
            signals = d.signals || {};
            history = d.history || [];
            document.getElementById('scanCount').textContent = d.scan_count;
            document.getElementById('lastScan').textContent = d.last_scan;
            if (d.stats) {
                document.getElementById('buyCount').textContent = d.stats.total_buy || 0;
                document.getElementById('sellCount').textContent = d.stats.total_sell || 0;
            }
            renderPrices();
            renderSignals();
            renderHistory();
            log('Initial state received');
        });

        socket.on('price_update', (d) => {
            prices[d.symbol] = d.data;
            if (d.analysis) analysis[d.symbol] = d.analysis;
            renderPrices();
            renderConfluence();
        });

        socket.on('new_signal', (d) => {
            signals[d.symbol] = d.signal;
            history.unshift(d.signal);
            history = history.slice(0, 100);
            renderSignals();
            renderHistory();
            showAlert(d.signal);
            log(`🎯 NEW: ${d.symbol} ${d.signal.direction} @ $${d.signal.entry_price} | Score: ${d.signal.confluence_score}/15`, 'signal');
        });

        socket.on('scan_update', (d) => {
            document.getElementById('scanCount').textContent = d.scan_count;
            document.getElementById('lastScan').textContent = d.last_scan;
            if (d.stats) {
                document.getElementById('buyCount').textContent = d.stats.total_buy || 0;
                document.getElementById('sellCount').textContent = d.stats.total_sell || 0;
            }
        });
    </script>
</body></html>
'''

ADMIN_TEMPLATE = '''
<!DOCTYPE html>
<html><head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>👑 UTS Pro Admin</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 min-h-screen text-white">
    <div class="container mx-auto px-4 py-8 max-w-4xl">
        <div class="flex justify-between items-center mb-8">
            <h1 class="text-2xl font-bold">👑 Admin Panel</h1>
            <div class="flex gap-4">
                <a href="{{ url_for('dashboard') }}" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg">📊 Dashboard</a>
                <a href="{{ url_for('logout') }}" class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg">🚪 Logout</a>
            </div>
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}{% for cat, msg in messages %}
        <div class="mb-4 p-4 rounded-lg {% if cat == 'error' %}bg-red-900{% else %}bg-green-900{% endif %}">{{ msg }}</div>
        {% endfor %}{% endif %}{% endwith %}
        
        <div class="bg-gray-800 rounded-xl p-6 mb-6">
            <h2 class="text-xl font-bold mb-4">➕ Create User</h2>
            <form method="POST" action="{{ url_for('admin_create_user') }}" class="grid grid-cols-1 md:grid-cols-5 gap-4">
                <input type="text" name="username" placeholder="Username" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg">
                <input type="password" name="password" placeholder="Password" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg">
                <input type="text" name="name" placeholder="Display Name" required class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg">
                <select name="role" class="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg">
                    <option value="user">User</option><option value="admin">Admin</option>
                </select>
                <button type="submit" class="px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg font-bold">✅ Create</button>
            </form>
        </div>
        
        <div class="bg-gray-800 rounded-xl overflow-hidden">
            <h2 class="text-xl font-bold p-6 border-b border-gray-700">👥 Users</h2>
            <table class="w-full">
                <thead class="bg-gray-700"><tr>
                    <th class="px-6 py-3 text-left">Username</th>
                    <th class="px-6 py-3 text-left">Name</th>
                    <th class="px-6 py-3 text-left">Role</th>
                    <th class="px-6 py-3 text-left">Status</th>
                    <th class="px-6 py-3 text-left">Actions</th>
                </tr></thead>
                <tbody>
                {% for username, user in users.items() %}
                <tr class="border-t border-gray-700">
                    <td class="px-6 py-4 font-bold">{{ username }}</td>
                    <td class="px-6 py-4">{{ user.name }}</td>
                    <td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.role == 'admin' %}bg-yellow-600{% else %}bg-blue-600{% endif %}">{{ user.role }}</span></td>
                    <td class="px-6 py-4"><span class="px-2 py-1 rounded text-sm {% if user.active %}bg-green-600{% else %}bg-red-600{% endif %}">{{ 'Active' if user.active else 'Disabled' }}</span></td>
                    <td class="px-6 py-4">
                        {% if username != 'admin' %}
                        <form method="POST" action="{{ url_for('admin_toggle', username=username) }}" class="inline">
                            <button class="px-3 py-1 {% if user.active %}bg-orange-600{% else %}bg-green-600{% endif %} rounded text-sm">{{ '🔒' if user.active else '🔓' }}</button>
                        </form>
                        <form method="POST" action="{{ url_for('admin_delete', username=username) }}" class="inline ml-2" onsubmit="return confirm('Delete?')">
                            <button class="px-3 py-1 bg-red-600 rounded text-sm">🗑️</button>
                        </form>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</body></html>
'''

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES - Add to app.py
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
            session.permanent = True
            session['user'] = username
            user_manager.users[username]['last_login'] = format_datetime()
            user_manager.save_users()
            return redirect(url_for('dashboard'))
        flash('Invalid credentials', 'error')
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    user = user_manager.users.get(session['user'], {})
    return render_template_string(DASHBOARD_TEMPLATE, is_admin=user.get('role')=='admin')

@app.route('/admin')
@admin_required
def admin():
    return render_template_string(ADMIN_TEMPLATE, users=user_manager.get_all_users())

@app.route('/admin/create', methods=['POST'])
@admin_required
def admin_create_user():
    success, msg = user_manager.create_user(
        request.form.get('username','').strip().lower(),
        request.form.get('password',''),
        request.form.get('name','').strip(),
        request.form.get('role','user')
    )
    flash(msg, 'success' if success else 'error')
    return redirect(url_for('admin'))

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
        del user_manager.users[username]
        user_manager.save_users()
    return redirect(url_for('admin'))

@app.route('/health')
def health():
    return {'status': 'ok', 'time': format_datetime(), 'scans': store.scan_count}

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET EVENTS
# ═══════════════════════════════════════════════════════════════════════════════
@socketio.on('connect')
def handle_connect():
    store.connected_clients += 1
    emit('initial_state', {
        'prices': store.prices,
        'signals': {k: asdict(v) for k, v in store.signals.items()},
        'history': [asdict(s) for s in store.history[:20]],
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'stats': store.stats
    })

@socketio.on('disconnect')
def handle_disconnect():
    store.connected_clients = max(0, store.connected_clients - 1)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("=" * 60)
    print(" ⚡ UTS PRO v4 - Ultimate Trading System")
    print(" Based on Pine Script Indicator")
    print("=" * 60)
    print(f"Time: {format_datetime()}")
    print(f"Features: SMC, Multi-TF S/R, Supply/Demand, Market Maker, FVG")
    print(f"Min Confluence: {Config.MIN_CONFLUENCE_SCORE}/15")
    print(f"Min R:R: {Config.MIN_RR_RATIO}:1")
    print(f"Default login: admin / admin123")
    
    scanner = threading.Thread(target=background_scanner, daemon=True)
    scanner.start()
    
    port = int(os.environ.get('PORT', 8000))
    print(f"\n🌐 Running on port {port}")
    
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
