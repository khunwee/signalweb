from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict
import pandas as pd
import threading
import time
import os
import requests

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'signal-dashboard-2024')
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
# CONFIGURATION - Optimized for Better Signals
# ============================================================
class Config:
    SCAN_INTERVAL_SECONDS = 30  # Scan every 30 seconds
    MIN_CONFLUENCE_SCORE = 70   # Lowered for more signals
    MIN_RR_RATIO = 2.0          # Minimum 2:1 R:R
    MAX_DRAWDOWN_DOLLARS = 15.0 # Max risk per trade
    
    SYMBOLS = {
        "XAUUSD": {"name": "Gold", "emoji": "🥇", "multiplier": 1},
        "XAGUSD": {"name": "Silver", "emoji": "🥈", "multiplier": 1},
        "USOUSD": {"name": "Oil", "emoji": "🛢️", "multiplier": 1},
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
        self.errors: List[str] = []

store = Store()

# ============================================================
# RELIABLE DATA FETCHER - Multiple Sources
# ============================================================
class DataFetcher:
    """Fetch data from multiple sources for reliability"""
    
    @staticmethod
    def fetch_from_yfinance(symbol: str) -> Optional[pd.DataFrame]:
        """Fetch from Yahoo Finance"""
        try:
            import yfinance as yf
            symbol_map = {
                "XAUUSD": "GC=F",
                "XAGUSD": "SI=F", 
                "USOUSD": "CL=F"
            }
            yf_symbol = symbol_map.get(symbol, "GC=F")
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(period="5d", interval="15m")
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception as e:
            print(f"yfinance error for {symbol}: {e}")
        return None
    
    @staticmethod
    def fetch_from_yfinance_1h(symbol: str) -> Optional[pd.DataFrame]:
        """Fetch 1H data from Yahoo Finance"""
        try:
            import yfinance as yf
            symbol_map = {
                "XAUUSD": "GC=F",
                "XAGUSD": "SI=F",
                "USOUSD": "CL=F"
            }
            yf_symbol = symbol_map.get(symbol, "GC=F")
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(period="1mo", interval="1h")
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception as e:
            print(f"yfinance 1H error for {symbol}: {e}")
        return None
    
    @staticmethod
    def get_current_price(symbol: str) -> Optional[Dict]:
        """Get current price"""
        try:
            import yfinance as yf
            symbol_map = {
                "XAUUSD": "GC=F",
                "XAGUSD": "SI=F",
                "USOUSD": "CL=F"
            }
            yf_symbol = symbol_map.get(symbol, "GC=F")
            ticker = yf.Ticker(yf_symbol)
            
            # Try 1m data first
            hist = ticker.history(period="1d", interval="1m")
            if hist.empty:
                hist = ticker.history(period="1d", interval="5m")
            if hist.empty:
                hist = ticker.history(period="5d", interval="15m")
            
            if not hist.empty:
                latest = hist.iloc[-1]
                prev = hist.iloc[-2] if len(hist) > 1 else latest
                change = float(latest["Close"]) - float(prev["Close"])
                
                return {
                    "symbol": symbol,
                    "price": round(float(latest["Close"]), 2),
                    "high": round(float(hist["High"].max()), 2),
                    "low": round(float(hist["Low"].min()), 2),
                    "open": round(float(hist.iloc[0]["Open"]), 2),
                    "change": round(change, 2),
                    "change_pct": round((change / float(prev["Close"])) * 100, 3) if prev["Close"] != 0 else 0,
                    "time": format_bangkok_time(),
                }
        except Exception as e:
            print(f"Price fetch error for {symbol}: {e}")
        return None

fetcher = DataFetcher()

# ============================================================
# ADVANCED SIGNAL ANALYZER
# ============================================================
class SignalAnalyzer:
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators"""
        df = df.copy()
        
        # EMAs
        for period in [9, 21, 50, 100, 200]:
            df[f'ema{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        
        # RSI (14)
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / (loss + 0.0001)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # ATR (14)
        high = df['high']
        low = df['low']
        close_prev = df['close'].shift(1)
        tr1 = high - low
        tr2 = (high - close_prev).abs()
        tr3 = (low - close_prev).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        
        # ADX
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
        
        atr14 = df['atr']
        plus_di = 100 * (plus_dm.rolling(14).mean() / (atr14 + 0.0001))
        minus_di = 100 * (minus_dm.rolling(14).mean() / (atr14 + 0.0001))
        dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 0.0001))
        df['adx'] = dx.rolling(14).mean()
        df['plus_di'] = plus_di
        df['minus_di'] = minus_di
        
        # Bollinger Bands
        df['bb_mid'] = df['close'].rolling(20).mean()
        bb_std = df['close'].rolling(20).std()
        df['bb_upper'] = df['bb_mid'] + 2 * bb_std
        df['bb_lower'] = df['bb_mid'] - 2 * bb_std
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']
        
        # Stochastic
        low14 = df['low'].rolling(14).min()
        high14 = df['high'].rolling(14).max()
        df['stoch_k'] = 100 * (df['close'] - low14) / (high14 - low14 + 0.0001)
        df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        
        # Support/Resistance levels
        df['swing_high'] = df['high'].rolling(20).max()
        df['swing_low'] = df['low'].rolling(20).min()
        df['recent_high'] = df['high'].rolling(10).max()
        df['recent_low'] = df['low'].rolling(10).min()
        
        # Momentum
        df['momentum'] = df['close'] - df['close'].shift(10)
        df['roc'] = ((df['close'] - df['close'].shift(10)) / df['close'].shift(10)) * 100
        
        return df
    
    def check_session(self) -> tuple:
        """Check trading session (Bangkok Time)"""
        hour = get_bangkok_time().hour
        
        # Best sessions for commodities (Bangkok time)
        if 19 <= hour <= 23:  # London-NY overlap
            return "LONDON-NY ⭐⭐", True, 15
        elif 14 <= hour < 19:  # London session
            return "LONDON ⭐", True, 10
        elif 20 <= hour or hour <= 4:  # NY session
            return "NEW YORK", True, 8
        elif 8 <= hour <= 14:  # Asia session
            return "ASIA", True, 5
        else:
            return "OFF-PEAK", False, 0
    
    def detect_trend(self, df: pd.DataFrame) -> tuple:
        """Detect overall trend direction and strength"""
        if len(df) < 50:
            return None, 0, []
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        reasons = []
        score = 0
        direction = None
        
        # EMA Stack Analysis (Strong trend indicator)
        ema9, ema21, ema50 = latest['ema9'], latest['ema21'], latest['ema50']
        ema100 = latest.get('ema100', ema50)
        
        # Bullish EMA Stack
        if ema9 > ema21 > ema50:
            direction = "BUY"
            if ema50 > ema100:
                score += 25
                reasons.append("✅ Strong Bullish EMA Stack")
            else:
                score += 20
                reasons.append("✅ Bullish EMA Alignment")
        # Bearish EMA Stack
        elif ema9 < ema21 < ema50:
            direction = "SELL"
            if ema50 < ema100:
                score += 25
                reasons.append("✅ Strong Bearish EMA Stack")
            else:
                score += 20
                reasons.append("✅ Bearish EMA Alignment")
        else:
            # Check for EMA crossover (potential reversal)
            prev_ema9, prev_ema21 = prev['ema9'], prev['ema21']
            
            # Bullish crossover
            if prev_ema9 <= prev_ema21 and ema9 > ema21:
                direction = "BUY"
                score += 15
                reasons.append("🔄 Bullish EMA Crossover")
            # Bearish crossover
            elif prev_ema9 >= prev_ema21 and ema9 < ema21:
                direction = "SELL"
                score += 15
                reasons.append("🔄 Bearish EMA Crossover")
        
        return direction, score, reasons
    
    def analyze_momentum(self, df: pd.DataFrame, direction: str) -> tuple:
        """Analyze momentum indicators"""
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        score = 0
        reasons = []
        
        rsi = latest['rsi']
        macd_hist = latest['macd_hist']
        prev_macd_hist = prev['macd_hist']
        stoch_k = latest['stoch_k']
        stoch_d = latest['stoch_d']
        
        if direction == "BUY":
            # RSI analysis for BUY
            if 30 <= rsi <= 50:
                score += 15
                reasons.append(f"✅ RSI Pullback ({rsi:.0f})")
            elif rsi < 30:
                score += 20
                reasons.append(f"✅ RSI Oversold ({rsi:.0f})")
            elif 50 < rsi < 70:
                score += 5
                reasons.append(f"📊 RSI Neutral ({rsi:.0f})")
            
            # MACD for BUY
            if macd_hist > 0:
                score += 10
                if macd_hist > prev_macd_hist:
                    score += 5
                    reasons.append("✅ MACD Bullish ↑")
                else:
                    reasons.append("✅ MACD Bullish")
            elif macd_hist > prev_macd_hist:  # Turning up
                score += 8
                reasons.append("🔄 MACD Turning Up")
            
            # Stochastic for BUY
            if stoch_k < 30:
                score += 10
                reasons.append(f"✅ Stoch Oversold ({stoch_k:.0f})")
            elif stoch_k > stoch_d and stoch_k < 80:
                score += 5
                reasons.append("✅ Stoch Bullish Cross")
                
        elif direction == "SELL":
            # RSI analysis for SELL
            if 50 <= rsi <= 70:
                score += 15
                reasons.append(f"✅ RSI Pullback ({rsi:.0f})")
            elif rsi > 70:
                score += 20
                reasons.append(f"✅ RSI Overbought ({rsi:.0f})")
            elif 30 < rsi < 50:
                score += 5
                reasons.append(f"📊 RSI Neutral ({rsi:.0f})")
            
            # MACD for SELL
            if macd_hist < 0:
                score += 10
                if macd_hist < prev_macd_hist:
                    score += 5
                    reasons.append("✅ MACD Bearish ↓")
                else:
                    reasons.append("✅ MACD Bearish")
            elif macd_hist < prev_macd_hist:  # Turning down
                score += 8
                reasons.append("🔄 MACD Turning Down")
            
            # Stochastic for SELL
            if stoch_k > 70:
                score += 10
                reasons.append(f"✅ Stoch Overbought ({stoch_k:.0f})")
            elif stoch_k < stoch_d and stoch_k > 20:
                score += 5
                reasons.append("✅ Stoch Bearish Cross")
        
        return score, reasons
    
    def analyze_trend_strength(self, df: pd.DataFrame, direction: str) -> tuple:
        """Analyze ADX and trend strength"""
        latest = df.iloc[-1]
        score = 0
        reasons = []
        
        adx = latest['adx']
        plus_di = latest['plus_di']
        minus_di = latest['minus_di']
        
        # ADX strength
        if adx > 30:
            score += 15
            reasons.append(f"✅ Strong Trend (ADX {adx:.0f})")
        elif adx > 25:
            score += 10
            reasons.append(f"✅ Moderate Trend (ADX {adx:.0f})")
        elif adx > 20:
            score += 5
            reasons.append(f"📊 Weak Trend (ADX {adx:.0f})")
        
        # DI confirmation
        if direction == "BUY" and plus_di > minus_di:
            score += 5
            reasons.append("✅ +DI > -DI")
        elif direction == "SELL" and minus_di > plus_di:
            score += 5
            reasons.append("✅ -DI > +DI")
        
        return score, reasons
    
    def analyze_price_action(self, df: pd.DataFrame, direction: str) -> tuple:
        """Analyze price action and key levels"""
        latest = df.iloc[-1]
        score = 0
        reasons = []
        
        close = latest['close']
        bb_upper = latest['bb_upper']
        bb_lower = latest['bb_lower']
        bb_mid = latest['bb_mid']
        
        if direction == "BUY":
            # Price near lower BB = good entry
            if close <= bb_lower * 1.01:
                score += 15
                reasons.append("✅ Price at Lower BB")
            elif close <= bb_mid:
                score += 8
                reasons.append("✅ Price below BB Mid")
            
            # Price above recent low
            if close > latest['recent_low'] * 1.005:
                score += 5
                reasons.append("✅ Above Recent Support")
                
        elif direction == "SELL":
            # Price near upper BB = good entry
            if close >= bb_upper * 0.99:
                score += 15
                reasons.append("✅ Price at Upper BB")
            elif close >= bb_mid:
                score += 8
                reasons.append("✅ Price above BB Mid")
            
            # Price below recent high
            if close < latest['recent_high'] * 0.995:
                score += 5
                reasons.append("✅ Below Recent Resistance")
        
        return score, reasons
    
    def calculate_levels(self, df: pd.DataFrame, direction: str) -> tuple:
        """Calculate Entry, SL, TP levels with minimal drawdown"""
        latest = df.iloc[-1]
        entry = round(latest['close'], 2)
        atr = latest['atr']
        
        if direction == "BUY":
            # Tight SL using recent swing low
            swing_low = latest['recent_low']
            atr_sl = entry - (atr * 1.2)
            
            # Use tighter SL for minimal drawdown
            sl = round(max(swing_low - (atr * 0.2), atr_sl, entry - Config.MAX_DRAWDOWN_DOLLARS), 2)
            risk = entry - sl
            
            # Calculate TPs
            tp1 = round(entry + (risk * 1.5), 2)
            tp2 = round(entry + (risk * 2.5), 2)
            tp3 = round(entry + (risk * 4.0), 2)
            
        else:  # SELL
            # Tight SL using recent swing high
            swing_high = latest['recent_high']
            atr_sl = entry + (atr * 1.2)
            
            # Use tighter SL for minimal drawdown
            sl = round(min(swing_high + (atr * 0.2), atr_sl, entry + Config.MAX_DRAWDOWN_DOLLARS), 2)
            risk = sl - entry
            
            # Calculate TPs
            tp1 = round(entry - (risk * 1.5), 2)
            tp2 = round(entry - (risk * 2.5), 2)
            tp3 = round(entry - (risk * 4.0), 2)
        
        return entry, sl, tp1, tp2, tp3, risk
    
    def generate_signal(self, symbol: str) -> Optional[Signal]:
        """Generate trading signal with comprehensive analysis"""
        
        # Fetch data
        df_1h = fetcher.fetch_from_yfinance_1h(symbol)
        df_15m = fetcher.fetch_from_yfinance(symbol)
        
        if df_1h is None or df_15m is None:
            print(f"No data for {symbol}")
            return None
        
        if len(df_1h) < 50 or len(df_15m) < 50:
            print(f"Insufficient data for {symbol}")
            return None
        
        # Calculate indicators
        df_1h = self.calculate_indicators(df_1h)
        df_15m = self.calculate_indicators(df_15m)
        
        total_score = 0
        all_reasons = []
        
        # 1. Detect HTF trend (1H)
        direction, trend_score, trend_reasons = self.detect_trend(df_1h)
        if direction is None:
            return None
        
        total_score += trend_score
        all_reasons.extend(trend_reasons)
        
        # 2. Check LTF alignment (15M)
        ltf_dir, ltf_score, ltf_reasons = self.detect_trend(df_15m)
        if ltf_dir == direction:
            total_score += 15
            all_reasons.append("✅ LTF Aligned")
        elif ltf_dir is not None:
            # Counter-trend = reduce score
            total_score -= 10
            all_reasons.append("⚠️ LTF Not Aligned")
        
        # 3. Analyze momentum (use 15M for entry timing)
        mom_score, mom_reasons = self.analyze_momentum(df_15m, direction)
        total_score += mom_score
        all_reasons.extend(mom_reasons)
        
        # 4. Analyze trend strength
        strength_score, strength_reasons = self.analyze_trend_strength(df_1h, direction)
        total_score += strength_score
        all_reasons.extend(strength_reasons)
        
        # 5. Analyze price action
        pa_score, pa_reasons = self.analyze_price_action(df_15m, direction)
        total_score += pa_score
        all_reasons.extend(pa_reasons)
        
        # 6. Session bonus
        session, is_good, session_score = self.check_session()
        if is_good:
            total_score += session_score
            all_reasons.append(f"✅ {session}")
        
        # Check minimum score
        if total_score < Config.MIN_CONFLUENCE_SCORE:
            print(f"{symbol}: Score {total_score} < {Config.MIN_CONFLUENCE_SCORE}")
            return None
        
        # Calculate levels
        entry, sl, tp1, tp2, tp3, risk = self.calculate_levels(df_15m, direction)
        
        # Calculate rewards
        reward_tp1 = abs(tp1 - entry)
        reward_tp2 = abs(tp2 - entry)
        reward_tp3 = abs(tp3 - entry)
        
        # Check R:R
        rr = reward_tp2 / risk if risk > 0 else 0
        if rr < Config.MIN_RR_RATIO:
            print(f"{symbol}: R:R {rr:.2f} < {Config.MIN_RR_RATIO}")
            return None
        
        # Determine strength
        if total_score >= 90:
            strength = "🔥 STRONG"
        elif total_score >= 80:
            strength = "⭐ GOOD"
        else:
            strength = "📊 MODERATE"
        
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
            confluence_score=total_score,
            reasons=all_reasons,
            session=session,
            timestamp=format_bangkok_datetime(),
            timestamp_unix=time.time()
        )

analyzer = SignalAnalyzer()

# ============================================================
# BACKGROUND SCANNER
# ============================================================
def background_scanner():
    """Background thread to scan for signals"""
    print(f"🚀 Scanner started at {format_bangkok_time()}")
    
    # Initial delay to let server start
    time.sleep(5)
    
    while True:
        try:
            store.scan_count += 1
            store.last_scan = format_bangkok_time()
            print(f"\n📡 Scan #{store.scan_count} at {store.last_scan}")
            
            for symbol in Config.SYMBOLS.keys():
                try:
                    # Get current price
                    price_data = fetcher.get_current_price(symbol)
                    
                    if price_data:
                        old_price = store.prices.get(symbol, {}).get('price', 0)
                        store.prices[symbol] = price_data
                        
                        print(f"  💰 {symbol}: ${price_data['price']}")
                        
                        # Emit price update
                        socketio.emit('price_update', {
                            'symbol': symbol,
                            'data': price_data,
                            'flash': abs(price_data['price'] - old_price) > 0.01
                        })
                    else:
                        print(f"  ❌ {symbol}: No price data")
                    
                    # Generate signal
                    signal = analyzer.generate_signal(symbol)
                    
                    if signal:
                        old_signal = store.signals.get(symbol)
                        
                        # Check if it's a new signal
                        is_new = (
                            not old_signal or
                            old_signal.direction != signal.direction or
                            time.time() - old_signal.timestamp_unix > 1800  # 30 min
                        )
                        
                        if is_new:
                            store.signals[symbol] = signal
                            store.history.insert(0, signal)
                            store.history = store.history[:100]
                            
                            print(f"  🎯 NEW SIGNAL: {symbol} {signal.direction} @ ${signal.entry_price} (Score: {signal.confluence_score})")
                            
                            # Emit new signal
                            socketio.emit('new_signal', {
                                'symbol': symbol,
                                'signal': asdict(signal),
                                'is_new': True
                            })
                    
                except Exception as e:
                    print(f"  ❌ Error processing {symbol}: {e}")
                    store.errors.append(f"{format_bangkok_time()}: {symbol} - {e}")
                
                # Small delay between symbols
                time.sleep(2)
            
            # Emit scan update
            socketio.emit('scan_update', {
                'scan_count': store.scan_count,
                'last_scan': store.last_scan,
                'connected': store.connected_clients,
                'bangkok_time': format_bangkok_datetime()
            })
            
        except Exception as e:
            print(f"❌ Scanner error: {e}")
            store.errors.append(f"{format_bangkok_time()}: Scanner - {e}")
        
        # Wait before next scan
        time.sleep(Config.SCAN_INTERVAL_SECONDS)

# ============================================================
# WEBSOCKET EVENTS
# ============================================================
@socketio.on('connect')
def handle_connect():
    store.connected_clients += 1
    print(f"✅ Client connected ({store.connected_clients} total)")
    
    # Send initial state
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
    store.connected_clients = max(0, store.connected_clients - 1)
    print(f"❌ Client disconnected ({store.connected_clients} total)")

@socketio.on('request_refresh')
def handle_refresh():
    """Manual refresh request"""
    emit('initial_state', {
        'prices': store.prices,
        'signals': {k: asdict(v) for k, v in store.signals.items()},
        'history': [asdict(s) for s in store.history[:20]],
        'scan_count': store.scan_count,
        'last_scan': store.last_scan,
        'bangkok_time': format_bangkok_datetime()
    })

# ============================================================
# HTML TEMPLATE
# ============================================================
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎯 Signal Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}.pulse{animation:pulse 1s infinite}
        @keyframes flash-up{0%{background:#10b981}100%{background:transparent}}.flash-up{animation:flash-up .5s}
        @keyframes flash-down{0%{background:#ef4444}100%{background:transparent}}.flash-down{animation:flash-down .5s}
        @keyframes glow{0%,100%{box-shadow:0 0 5px #10b981}50%{box-shadow:0 0 20px #10b981}}.glow{animation:glow 1.5s infinite}
        @keyframes slideIn{from{transform:translateY(-20px);opacity:0}to{transform:translateY(0);opacity:1}}.slide-in{animation:slideIn .3s}
    </style>
</head>
<body class="bg-gray-900 text-white min-h-screen">
    <div class="container mx-auto px-4 py-6 max-w-7xl">
        <!-- Header -->
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4">
            <div>
                <h1 class="text-2xl md:text-3xl font-bold">🎯 Precision Signal Dashboard</h1>
                <p class="text-gray-400 text-sm">High-accuracy signals • Bangkok Time (UTC+7)</p>
            </div>
            <div class="flex items-center gap-4">
                <div id="clock" class="bg-gray-800 px-4 py-2 rounded-lg font-mono text-lg">TH --:--:--</div>
                <span id="status" class="text-red-400">● Connecting...</span>
            </div>
        </div>
        
        <!-- Stats -->
        <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
            <div class="bg-gray-800 rounded-lg p-3 text-center">
                <div class="text-gray-400 text-xs">Scans</div>
                <div id="scanCount" class="text-xl font-bold">0</div>
            </div>
            <div class="bg-gray-800 rounded-lg p-3 text-center">
                <div class="text-gray-400 text-xs">Active Signals</div>
                <div id="signalCount" class="text-xl font-bold text-green-400">0</div>
            </div>
            <div class="bg-gray-800 rounded-lg p-3 text-center">
                <div class="text-gray-400 text-xs">Last Scan</div>
                <div id="lastScan" class="text-lg font-mono">--:--:--</div>
            </div>
            <div class="bg-gray-800 rounded-lg p-3 text-center">
                <div class="text-gray-400 text-xs">Session</div>
                <div id="session" class="text-sm font-bold text-yellow-400">-</div>
            </div>
            <div class="bg-gray-800 rounded-lg p-3 text-center">
                <div class="text-gray-400 text-xs">Clients</div>
                <div id="clients" class="text-xl font-bold">0</div>
            </div>
        </div>
        
        <!-- Alert Area -->
        <div id="alertArea" class="hidden mb-6">
            <div class="bg-gradient-to-r from-green-900 to-green-800 border-2 border-green-500 rounded-xl p-4 glow">
                <div class="flex items-center gap-3">
                    <span class="text-3xl">🚨</span>
                    <div>
                        <div id="alertTitle" class="text-xl font-bold">NEW SIGNAL!</div>
                        <div id="alertText" class="text-green-300"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Prices -->
        <h2 class="text-lg font-bold mb-3">💰 Live Prices <span class="text-xs text-green-400 pulse">● REAL-TIME</span></h2>
        <div id="prices" class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6"></div>
        
        <!-- Signals -->
        <h2 class="text-lg font-bold mb-3">🎯 Active Trading Signals</h2>
        <div id="signals" class="grid grid-cols-1 xl:grid-cols-2 gap-4 mb-6">
            <div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full">
                <div class="text-4xl mb-2">📡</div>
                <div>Scanning for high-probability setups...</div>
                <div class="text-sm mt-2">Min Score: 70% | Min R:R: 2:1</div>
            </div>
        </div>
        
        <!-- History -->
        <h2 class="text-lg font-bold mb-3">📜 Signal History</h2>
        <div class="bg-gray-800 rounded-xl overflow-hidden mb-6">
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
                    </tr>
                </thead>
                <tbody id="history"></tbody>
            </table>
        </div>
        
        <!-- Log -->
        <h2 class="text-lg font-bold mb-3">📡 Event Log</h2>
        <div id="log" class="bg-gray-800 rounded-xl p-4 h-40 overflow-y-auto font-mono text-xs"></div>
        
        <div class="mt-6 text-center text-gray-500 text-xs">
            ⚠️ Educational purposes only. Not financial advice. Always use proper risk management.
        </div>
    </div>

    <script>
        const socket = io();
        let prices = {}, signals = {}, history = [];
        
        // Clock
        function updateClock() {
            const now = new Date();
            const bkk = new Date(now.toLocaleString("en-US", {timeZone: "Asia/Bangkok"}));
            document.getElementById('clock').textContent = 'TH ' + bkk.toLocaleTimeString('en-GB');
            
            const h = bkk.getHours();
            let sess = h >= 19 && h <= 23 ? 'LONDON-NY ⭐⭐' : 
                       h >= 14 && h < 19 ? 'LONDON ⭐' : 
                       h >= 20 || h <= 4 ? 'NEW YORK' : 
                       h >= 8 && h <= 14 ? 'ASIA' : 'OFF-PEAK';
            document.getElementById('session').textContent = sess;
        }
        setInterval(updateClock, 1000);
        updateClock();
        
        // Log
        function log(msg, type='info') {
            const el = document.getElementById('log');
            const t = new Date().toLocaleTimeString('en-GB', {timeZone: 'Asia/Bangkok'});
            const c = {signal:'text-green-400', price:'text-blue-400', error:'text-red-400', info:'text-gray-400'}[type] || 'text-gray-400';
            el.innerHTML = `<div class="${c}">[${t}] ${msg}</div>` + el.innerHTML;
        }
        
        // Render prices
        function renderPrices() {
            const syms = {XAUUSD: '🥇 GOLD', XAGUSD: '🥈 SILVER', USOUSD: '🛢️ OIL'};
            let html = '';
            for (const [sym, name] of Object.entries(syms)) {
                const p = prices[sym];
                if (!p) {
                    html += `<div class="bg-gray-800 rounded-xl p-4"><div class="text-gray-400">${name} - Loading...</div></div>`;
                    continue;
                }
                const col = p.change >= 0 ? 'text-green-400' : 'text-red-400';
                const arr = p.change >= 0 ? '▲' : '▼';
                html += `
                    <div id="price-${sym}" class="bg-gray-800 rounded-xl p-4 border border-gray-700">
                        <div class="flex justify-between items-center mb-2">
                            <span class="font-bold">${name}</span>
                            <span class="${col} text-sm">${arr} ${p.change_pct.toFixed(3)}%</span>
                        </div>
                        <div class="text-3xl font-bold font-mono">$${p.price.toLocaleString()}</div>
                        <div class="flex justify-between text-xs text-gray-400 mt-2">
                            <span>H: $${p.high}</span>
                            <span>L: $${p.low}</span>
                        </div>
                        <div class="text-xs text-gray-500 mt-1">Updated: ${p.time}</div>
                    </div>`;
            }
            document.getElementById('prices').innerHTML = html;
        }
        
        // Render signals
        function renderSignals() {
            const list = Object.values(signals);
            let html = '';
            
            if (list.length === 0) {
                html = `<div class="bg-gray-800 rounded-xl p-8 text-center text-gray-400 col-span-full">
                    <div class="text-4xl mb-2">📡</div><div>Scanning for signals...</div></div>`;
            } else {
                for (const s of list) {
                    const isBuy = s.direction === 'BUY';
                    const border = isBuy ? 'border-green-500' : 'border-red-500';
                    const bg = isBuy ? 'from-green-900/30' : 'from-red-900/30';
                    const col = isBuy ? 'text-green-400' : 'text-red-400';
                    const icon = isBuy ? '🟢' : '🔴';
                    
                    html += `
                        <div class="bg-gradient-to-br ${bg} to-gray-800 rounded-xl p-5 border-l-4 ${border} slide-in">
                            <div class="flex justify-between items-start mb-4">
                                <div>
                                    <div class="text-2xl font-bold ${col}">${icon} ${s.symbol} ${s.direction}</div>
                                    <div class="text-sm text-gray-400">${s.session}</div>
                                </div>
                                <div class="text-right">
                                    <span class="bg-yellow-600/80 px-3 py-1 rounded-full text-sm">${s.strength}</span>
                                    <div class="text-xs text-gray-400 mt-1">${s.timestamp}</div>
                                </div>
                            </div>
                            <div class="grid grid-cols-2 gap-3 mb-4">
                                <div class="bg-gray-900/50 rounded-lg p-3">
                                    <div class="text-xs text-gray-400">Entry</div>
                                    <div class="text-xl font-bold">$${s.entry_price}</div>
                                </div>
                                <div class="bg-red-900/30 rounded-lg p-3 border border-red-900">
                                    <div class="text-xs text-red-400">Stop Loss</div>
                                    <div class="text-xl font-bold text-red-400">$${s.stop_loss}</div>
                                    <div class="text-xs text-gray-500">Risk: $${s.risk_dollars}</div>
                                </div>
                            </div>
                            <div class="grid grid-cols-3 gap-2 mb-4">
                                <div class="bg-green-900/30 rounded-lg p-2 border border-green-900 text-center">
                                    <div class="text-xs text-green-400">TP1</div>
                                    <div class="font-bold text-green-400">$${s.tp1}</div>
                                    <div class="text-xs text-gray-500">+$${s.reward_tp1}</div>
                                </div>
                                <div class="bg-green-900/40 rounded-lg p-2 border border-green-700 text-center">
                                    <div class="text-xs text-green-300">TP2</div>
                                    <div class="font-bold text-green-300">$${s.tp2}</div>
                                    <div class="text-xs text-gray-500">+$${s.reward_tp2}</div>
                                </div>
                                <div class="bg-green-900/50 rounded-lg p-2 border border-green-500 text-center">
                                    <div class="text-xs text-green-200">TP3</div>
                                    <div class="font-bold text-green-200">$${s.tp3}</div>
                                    <div class="text-xs text-gray-500">+$${s.reward_tp3}</div>
                                </div>
                            </div>
                            <div class="mb-3">
                                <div class="flex justify-between text-xs mb-1">
                                    <span>Confluence</span>
                                    <span class="font-bold">${s.confluence_score}% | R:R ${s.risk_reward}:1</span>
                                </div>
                                <div class="bg-gray-700 rounded-full h-2">
                                    <div class="bg-green-500 h-2 rounded-full" style="width:${Math.min(s.confluence_score,100)}%"></div>
                                </div>
                            </div>
                            <div class="flex flex-wrap gap-1">
                                ${s.reasons.map(r => `<span class="bg-gray-700 px-2 py-1 rounded text-xs">${r}</span>`).join('')}
                            </div>
                        </div>`;
                }
            }
            document.getElementById('signals').innerHTML = html;
            document.getElementById('signalCount').textContent = list.length;
        }
        
        // Render history
        function renderHistory() {
            let html = '';
            if (history.length === 0) {
                html = '<tr><td colspan="9" class="px-3 py-4 text-center text-gray-400">No signals yet...</td></tr>';
            } else {
                for (const s of history.slice(0, 15)) {
                    const col = s.direction === 'BUY' ? 'text-green-400 bg-green-900/30' : 'text-red-400 bg-red-900/30';
                    html += `
                        <tr class="border-t border-gray-700">
                            <td class="px-3 py-2 text-xs">${s.timestamp}</td>
                            <td class="px-3 py-2 font-bold">${s.symbol}</td>
                            <td class="px-3 py-2"><span class="${col} px-2 py-1 rounded">${s.direction}</span></td>
                            <td class="px-3 py-2">$${s.entry_price}</td>
                            <td class="px-3 py-2 text-red-400">$${s.stop_loss}</td>
                            <td class="px-3 py-2 text-green-400">$${s.tp1}</td>
                            <td class="px-3 py-2 text-green-300">$${s.tp2}</td>
                            <td class="px-3 py-2 font-bold">${s.risk_reward}:1</td>
                            <td class="px-3 py-2">${s.confluence_score}%</td>
                        </tr>`;
                }
            }
            document.getElementById('history').innerHTML = html;
        }
        
        // Show alert
        function showAlert(signal) {
            const area = document.getElementById('alertArea');
            document.getElementById('alertTitle').textContent = `NEW ${signal.direction} SIGNAL!`;
            document.getElementById('alertText').textContent = 
                `${signal.symbol} @ $${signal.entry_price} | SL: $${signal.stop_loss} | TP1: $${signal.tp1} | Score: ${signal.confluence_score}%`;
            area.classList.remove('hidden');
            
            // Sound
            try {
                const ctx = new AudioContext();
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.frequency.value = signal.direction === 'BUY' ? 800 : 600;
                gain.gain.value = 0.1;
                osc.start();
                osc.stop(ctx.currentTime + 0.2);
            } catch(e) {}
            
            setTimeout(() => area.classList.add('hidden'), 15000);
        }
        
        // Socket events
        socket.on('connect', () => {
            document.getElementById('status').innerHTML = '<span class="text-green-400 pulse">● CONNECTED</span>';
            log('Connected to server', 'info');
        });
        
        socket.on('disconnect', () => {
            document.getElementById('status').innerHTML = '<span class="text-red-400">● DISCONNECTED</span>';
            log('Disconnected', 'error');
        });
        
        socket.on('initial_state', (data) => {
            prices = data.prices || {};
            signals = data.signals || {};
            history = data.history || [];
            document.getElementById('scanCount').textContent = data.scan_count;
            document.getElementById('lastScan').textContent = data.last_scan;
            renderPrices();
            renderSignals();
            renderHistory();
            log('Initial state received', 'info');
        });
        
        socket.on('price_update', (data) => {
            const old = prices[data.symbol]?.price || 0;
            prices[data.symbol] = data.data;
            renderPrices();
            log(`${data.symbol}: $${data.data.price}`, 'price');
            
            if (data.flash) {
                const el = document.getElementById(`price-${data.symbol}`);
                if (el) {
                    el.classList.add(data.data.price > old ? 'flash-up' : 'flash-down');
                    setTimeout(() => el.classList.remove('flash-up', 'flash-down'), 500);
                }
            }
        });
        
        socket.on('new_signal', (data) => {
            signals[data.symbol] = data.signal;
            history.unshift(data.signal);
            history = history.slice(0, 100);
            renderSignals();
            renderHistory();
            showAlert(data.signal);
            log(`🎯 NEW SIGNAL: ${data.symbol} ${data.signal.direction} @ $${data.signal.entry_price}`, 'signal');
        });
        
        socket.on('scan_update', (data) => {
            document.getElementById('scanCount').textContent = data.scan_count;
            document.getElementById('lastScan').textContent = data.last_scan;
            document.getElementById('clients').textContent = data.connected;
        });
    </script>
</body>
</html>
'''

# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    return {
        'status': 'ok',
        'time': format_bangkok_datetime(),
        'scans': store.scan_count,
        'signals': len(store.signals),
        'clients': store.connected_clients
    }

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("=" * 50)
    print(" 🎯 PRECISION SIGNAL DASHBOARD")
    print("=" * 50)
    print(f"Time: {format_bangkok_datetime()}")
    print(f"Scan interval: {Config.SCAN_INTERVAL_SECONDS}s")
    print(f"Min score: {Config.MIN_CONFLUENCE_SCORE}%")
    print(f"Min R:R: {Config.MIN_RR_RATIO}:1")
    
    # Start scanner thread
    scanner = threading.Thread(target=background_scanner, daemon=True)
    scanner.start()
    
    port = int(os.environ.get('PORT', 8000))
    print(f"\n🌐 Running on port {port}")
    
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
