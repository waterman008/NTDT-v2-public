#!/usr/bin/env python3
"""
NTDT Fast Risk Engine
High-speed risk validation for BUY_TO_OPEN orders only
All checks designed to complete in under 25ms total

NTDT Simplified Order Flow:
- ONLY validates BUY_TO_OPEN orders (new positions)
- SELL_TO_CLOSE orders validated by PositionValidator only
- No complex spread or multi-leg validation needed

Fast Risk Checks for BUY_TO_OPEN:
1. Session execution limits (6 new positions, 50 total executions)
2. Position dollar limits ($2,500 per position, $15,000 total portfolio)
3. Strike price bounds (based on .srt historical data + buffer)
4. Contract quantity limits (5 max per position)
5. Price reasonableness (based on .srt historical ranges)
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import json

@dataclass
class RiskResult:
    valid: bool
    reason: str
    risk_level: str  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

@dataclass
class SessionLimits:
    max_new_positions: int = 6
    max_total_executions: int = 50
    max_contracts_per_position: int = 5
    max_portfolio_exposure: float = 15000.0
    max_position_exposure: float = 2500.0

@dataclass
class MarketBounds:
    # Based on .srt analysis with safety buffers
    min_strike: float = 10.0
    max_strike: float = 600.0
    min_price: float = 0.05
    max_price: float = 15.0
    
    # Ticker-specific bounds (from historical data)
    ticker_bounds: Dict[str, Dict] = None
    
    def __post_init__(self):
        if self.ticker_bounds is None:
            # Historical bounds from .srt analysis with 20% buffer
            self.ticker_bounds = {
                'TSLA': {'min_strike': 200, 'max_strike': 400, 'typical_price_range': (1.0, 8.0)},
                'SPY': {'min_strike': 400, 'max_strike': 600, 'typical_price_range': (0.5, 12.0)},
                'QQQ': {'min_strike': 350, 'max_strike': 500, 'typical_price_range': (0.8, 10.0)},
                'AAPL': {'min_strike': 150, 'max_strike': 250, 'typical_price_range': (1.2, 9.0)},
                'NVDA': {'min_strike': 80, 'max_strike': 200, 'typical_price_range': (2.0, 15.0)},
                'AMZN': {'min_strike': 140, 'max_strike': 220, 'typical_price_range': (1.5, 8.5)},
                'MSFT': {'min_strike': 300, 'max_strike': 450, 'typical_price_range': (1.0, 7.0)},
                'META': {'min_strike': 350, 'max_strike': 550, 'typical_price_range': (2.5, 12.0)},
                'GOOGL': {'min_strike': 100, 'max_strike': 180, 'typical_price_range': (1.8, 9.5)},
                'PLTR': {'min_strike': 15, 'max_strike': 35, 'typical_price_range': (0.3, 4.0)},
                'HOOD': {'min_strike': 8, 'max_strike': 25, 'typical_price_range': (0.2, 3.5)},
                'AMD': {'min_strike': 100, 'max_strike': 200, 'typical_price_range': (1.5, 8.0)},
                'UBER': {'min_strike': 50, 'max_strike': 85, 'typical_price_range': (0.8, 5.0)},
                'IWM': {'min_strike': 180, 'max_strike': 250, 'typical_price_range': (1.0, 6.0)},
                'SPX': {'min_strike': 5200, 'max_strike': 6000, 'typical_price_range': (5.0, 50.0)}
            }

class FastRiskEngine:
    def __init__(self, db_path: str = ':memory:'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.session_limits = SessionLimits()
        self.market_bounds = MarketBounds()
        self.session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M')}"
        self.setup_logging()
        self.init_db()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('FastRiskEngine')
        
    def init_db(self):
        """Initialize database with session tracking"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Positions table (reuse existing)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                strike REAL NOT NULL,
                option_type TEXT NOT NULL,
                expiration TEXT NOT NULL,
                contracts INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                exit_time TIMESTAMP NULL,
                pnl REAL DEFAULT 0.0
            )
        ''')
        
        # Session executions table (track all actions)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS session_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                execution_type TEXT NOT NULL,  -- 'OPEN', 'CLOSE', 'PARTIAL_CLOSE'
                ticker TEXT NOT NULL,
                contracts INTEGER NOT NULL,
                execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Session summary table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS session_summary (
                session_id TEXT PRIMARY KEY,
                new_positions_opened INTEGER DEFAULT 0,
                total_executions INTEGER DEFAULT 0,
                total_portfolio_exposure REAL DEFAULT 0.0,
                session_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE'
            )
        ''')
        
        self.conn.commit()
        self.logger.info("Fast Risk Engine database initialized")
        
    def validate_buy_to_open(self, ticker: str, strike: float, option_type: str, 
                           contracts: int, entry_price: float) -> RiskResult:
        """
        Validate BUY_TO_OPEN order against NTDT fast risk checks
        Target: Complete in under 25ms
        
        Only validates new position openings:
        - Session limits (positions, executions, exposure)
        - Strike price bounds (ticker-specific)
        - Contract quantity limits
        - Price reasonableness checks
        """
        start_time = datetime.now()
        warnings = []
        
        # Check 1: Session execution limits (~2ms)
        session_check = self._check_session_limits(contracts)
        if not session_check.valid:
            return session_check
            
        # Check 2: Position dollar exposure (~1ms)
        exposure_check = self._check_position_exposure(contracts, entry_price)
        if not exposure_check.valid:
            return exposure_check
        warnings.extend(exposure_check.warnings)
            
        # Check 3: Portfolio total exposure (~3ms)
        portfolio_check = self._check_portfolio_exposure(contracts, entry_price)
        if not portfolio_check.valid:
            return portfolio_check
        warnings.extend(portfolio_check.warnings)
        
        # Check 4: Strike price bounds (~1ms)
        strike_check = self._check_strike_bounds(ticker, strike)
        if not strike_check.valid:
            return strike_check
        warnings.extend(strike_check.warnings)
        
        # Check 5: Price reasonableness (~1ms)
        price_check = self._check_price_bounds(ticker, entry_price)
        if not price_check.valid:
            return price_check
        warnings.extend(price_check.warnings)
        
        # Check 6: Contract quantity (~1ms)
        quantity_check = self._check_contract_limits(contracts)
        if not quantity_check.valid:
            return quantity_check
        warnings.extend(quantity_check.warnings)
        
        # Calculate total time
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        self.logger.info(f"Fast risk checks completed in {elapsed:.1f}ms")
        
        return RiskResult(
            valid=True,
            reason="All fast risk checks passed",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_session_limits(self, contracts: int) -> RiskResult:
        """Check session-level position and execution limits"""
        # Get current session stats
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT ticker) as new_positions,
                COALESCE(SUM(contracts), 0) as total_contracts
            FROM positions 
            WHERE session_id = ? AND status = 'OPEN'
        """, (self.session_id,))
        
        result = self.cursor.fetchone()
        current_positions = result[0] if result else 0
        
        self.cursor.execute("""
            SELECT COUNT(*) FROM session_executions 
            WHERE session_id = ?
        """, (self.session_id,))
        
        total_executions = self.cursor.fetchone()[0]
        
        # Check limits
        if current_positions >= self.session_limits.max_new_positions:
            return RiskResult(
                valid=False,
                reason=f"Session limit reached: {current_positions}/{self.session_limits.max_new_positions} positions",
                risk_level="CRITICAL"
            )
            
        if total_executions >= self.session_limits.max_total_executions:
            return RiskResult(
                valid=False,
                reason=f"Execution limit reached: {total_executions}/{self.session_limits.max_total_executions} trades",
                risk_level="CRITICAL"
            )
        
        warnings = []
        if current_positions >= self.session_limits.max_new_positions * 0.8:
            warnings.append(f"Approaching position limit: {current_positions}/{self.session_limits.max_new_positions}")
            
        if total_executions >= self.session_limits.max_total_executions * 0.8:
            warnings.append(f"Approaching execution limit: {total_executions}/{self.session_limits.max_total_executions}")
        
        return RiskResult(
            valid=True,
            reason="Session limits OK",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_position_exposure(self, contracts: int, entry_price: float) -> RiskResult:
        """Check individual position dollar exposure"""
        position_value = contracts * entry_price * 100  # Options multiplier
        
        if position_value > self.session_limits.max_position_exposure:
            return RiskResult(
                valid=False,
                reason=f"Position too large: ${position_value:,.0f} > ${self.session_limits.max_position_exposure:,.0f}",
                risk_level="CRITICAL"
            )
        
        warnings = []
        if position_value > self.session_limits.max_position_exposure * 0.8:
            warnings.append(f"Large position: ${position_value:,.0f}")
            
        return RiskResult(
            valid=True,
            reason="Position size OK",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_portfolio_exposure(self, contracts: int, entry_price: float) -> RiskResult:
        """Check total portfolio exposure"""
        # Get current portfolio value
        self.cursor.execute("""
            SELECT COALESCE(SUM(contracts * entry_price * 100), 0)
            FROM positions 
            WHERE session_id = ? AND status = 'OPEN'
        """, (self.session_id,))
        
        current_exposure = self.cursor.fetchone()[0]
        new_position_value = contracts * entry_price * 100
        total_exposure = current_exposure + new_position_value
        
        if total_exposure > self.session_limits.max_portfolio_exposure:
            return RiskResult(
                valid=False,
                reason=f"Portfolio limit exceeded: ${total_exposure:,.0f} > ${self.session_limits.max_portfolio_exposure:,.0f}",
                risk_level="CRITICAL"
            )
        
        warnings = []
        if total_exposure > self.session_limits.max_portfolio_exposure * 0.8:
            warnings.append(f"High portfolio exposure: ${total_exposure:,.0f}")
            
        return RiskResult(
            valid=True,
            reason="Portfolio exposure OK",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_strike_bounds(self, ticker: str, strike: float) -> RiskResult:
        """Check if strike price is within reasonable bounds"""
        # Use ticker-specific bounds if available
        if ticker in self.market_bounds.ticker_bounds:
            bounds = self.market_bounds.ticker_bounds[ticker]
            min_strike = bounds['min_strike']
            max_strike = bounds['max_strike']
        else:
            # Fall back to general bounds
            min_strike = self.market_bounds.min_strike
            max_strike = self.market_bounds.max_strike
        
        if strike < min_strike or strike > max_strike:
            return RiskResult(
                valid=False,
                reason=f"Strike {strike} outside bounds [{min_strike}-{max_strike}] for {ticker}",
                risk_level="HIGH"
            )
        
        # Warning zones (outer 20% of range)
        range_size = max_strike - min_strike
        warning_buffer = range_size * 0.2
        
        warnings = []
        if strike < min_strike + warning_buffer or strike > max_strike - warning_buffer:
            warnings.append(f"Strike {strike} near bounds for {ticker}")
            
        return RiskResult(
            valid=True,
            reason="Strike within bounds",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_price_bounds(self, ticker: str, price: float) -> RiskResult:
        """Check if option price is reasonable"""
        # Use ticker-specific price ranges if available
        if ticker in self.market_bounds.ticker_bounds:
            bounds = self.market_bounds.ticker_bounds[ticker]
            min_price, max_price = bounds['typical_price_range']
        else:
            min_price = self.market_bounds.min_price
            max_price = self.market_bounds.max_price
        
        if price < min_price or price > max_price:
            return RiskResult(
                valid=False,
                reason=f"Price ${price} outside typical range [${min_price}-${max_price}] for {ticker}",
                risk_level="MEDIUM"  # Price errors are less critical than position size
            )
        
        warnings = []
        # Warning for prices in outer 30% of typical range
        range_size = max_price - min_price
        warning_buffer = range_size * 0.3
        
        if price < min_price + warning_buffer or price > max_price - warning_buffer:
            warnings.append(f"Price ${price} outside typical range for {ticker}")
            
        return RiskResult(
            valid=True,
            reason="Price within range",
            risk_level="LOW",
            warnings=warnings
        )
        
    def _check_contract_limits(self, contracts: int) -> RiskResult:
        """Check contract quantity limits"""
        if contracts > self.session_limits.max_contracts_per_position:
            return RiskResult(
                valid=False,
                reason=f"Too many contracts: {contracts} > {self.session_limits.max_contracts_per_position}",
                risk_level="HIGH"
            )
        
        if contracts <= 0:
            return RiskResult(
                valid=False,
                reason=f"Invalid contract quantity: {contracts}",
                risk_level="HIGH"
            )
        
        return RiskResult(
            valid=True,
            reason="Contract quantity OK",
            risk_level="LOW"
        )
        
    def record_execution(self, execution_type: str, ticker: str, contracts: int):
        """Record execution for session tracking"""
        self.cursor.execute("""
            INSERT INTO session_executions (session_id, execution_type, ticker, contracts)
            VALUES (?, ?, ?, ?)
        """, (self.session_id, execution_type, ticker, contracts))
        self.conn.commit()
        
    def get_session_risk_summary(self) -> Dict:
        """Get current session risk metrics"""
        # Position counts
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT ticker) as positions,
                COALESCE(SUM(contracts), 0) as total_contracts,
                COALESCE(SUM(contracts * entry_price * 100), 0) as total_exposure
            FROM positions 
            WHERE session_id = ? AND status = 'OPEN'
        """, (self.session_id,))
        
        pos_result = self.cursor.fetchone()
        
        # Execution count
        self.cursor.execute("""
            SELECT COUNT(*) FROM session_executions 
            WHERE session_id = ?
        """, (self.session_id,))
        
        exec_count = self.cursor.fetchone()[0]
        
        return {
            'session_id': self.session_id,
            'positions': {
                'current': pos_result[0],
                'max_allowed': self.session_limits.max_new_positions,
                'utilization_pct': (pos_result[0] / self.session_limits.max_new_positions) * 100
            },
            'executions': {
                'current': exec_count,
                'max_allowed': self.session_limits.max_total_executions,
                'utilization_pct': (exec_count / self.session_limits.max_total_executions) * 100
            },
            'exposure': {
                'current': pos_result[2],
                'max_allowed': self.session_limits.max_portfolio_exposure,
                'utilization_pct': (pos_result[2] / self.session_limits.max_portfolio_exposure) * 100
            },
            'contracts': {
                'total_open': pos_result[1]
            }
        }
        
    def __del__(self):
        if self.conn:
            self.conn.close()

# Test the fast risk engine
if __name__ == '__main__':
    risk_engine = FastRiskEngine()
    
    # Test scenarios
    test_cases = [
        # Valid trade
        ("TSLA", 340.0, "CALL", 5, 2.50),
        # Too expensive
        ("TSLA", 340.0, "CALL", 5, 25.00),
        # Strike too high
        ("TSLA", 500.0, "CALL", 5, 2.50),
        # Too many contracts
        ("TSLA", 340.0, "CALL", 10, 2.50),
        # Unknown ticker (should use general bounds)
        ("UNKNOWN", 100.0, "CALL", 5, 5.00),
    ]
    
    print("=== Fast Risk Engine Test ===")
    for i, (ticker, strike, option_type, contracts, price) in enumerate(test_cases, 1):
        print(f"\nTest {i}: {ticker} {strike} {option_type} x{contracts} @ ${price}")
        
        result = risk_engine.validate_buy_to_open(ticker, strike, option_type, contracts, price)
        
        print(f"  Valid: {result.valid}")
        print(f"  Risk Level: {result.risk_level}")
        print(f"  Reason: {result.reason}")
        if result.warnings:
            print(f"  Warnings: {', '.join(result.warnings)}")
            
        # Record execution if valid
        if result.valid:
            risk_engine.record_execution("OPEN", ticker, contracts)
    
    # Show session summary
    print(f"\n=== Session Risk Summary ===")
    summary = risk_engine.get_session_risk_summary()
    for category, data in summary.items():
        if isinstance(data, dict):
            print(f"{category.upper()}:")
            for key, value in data.items():
                if key.endswith('_pct'):
                    print(f"  {key}: {value:.1f}%")
                else:
                    print(f"  {key}: {value}")
        else:
            print(f"{category}: {data}")
