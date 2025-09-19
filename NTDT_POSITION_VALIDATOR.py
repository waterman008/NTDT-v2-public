#!/usr/bin/env python3
"""
NTDT Position Validator
Implements 1-position-per-ticker-per-session validation based on .srt analysis

Key Rules:
- Max 1 position per ticker per session
- Max 5 contracts per position
- Same strike price per ticker (no multiple strikes)
- Partial exits allowed from same position
"""

import sqlite3
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import uuid

@dataclass
class Position:
    """Represents an open position"""
    id: str
    ticker: str
    strike: float
    option_type: str  # 'CALL' or 'PUT'
    expiration: str   # '0DTE', '1DTE', '7DTE'
    contracts: int
    entry_price: float
    session_id: str
    entry_time: datetime
    status: str = 'OPEN'  # 'OPEN' or 'CLOSED'

@dataclass
class ValidationResult:
    """Result of position validation"""
    valid: bool
    reason: str
    current_contracts: int = 0
    available_contracts: int = 0
    existing_position: Optional[Position] = None

class PositionValidator:
    """
    Validates position entries against NT trading rules
    Based on actual .srt analysis patterns
    """
    
    def __init__(self, db_path: str = "ntdt_positions.db"):
        self.db_path = db_path
        self.setup_logging()
        self.setup_database()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('PositionValidator')
        
    def setup_database(self):
        """Create positions table with constraints"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    strike REAL NOT NULL,
                    option_type TEXT NOT NULL,
                    expiration TEXT NOT NULL,
                    contracts INTEGER NOT NULL,
                    entry_price REAL NOT NULL,
                    session_id TEXT NOT NULL,
                    entry_time TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'OPEN',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Unique constraint: 1 position per ticker per session
            conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_session 
                ON positions(ticker, session_id) 
                WHERE status = 'OPEN'
            ''')
            
            # Index for fast lookups
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_session_status 
                ON positions(session_id, status)
            ''')
            
        self.logger.info("Database initialized with position constraints")

    def get_current_session_id(self) -> str:
        """Get or create session ID for today"""
        today = date.today().strftime("%Y%m%d")
        return f"session_{today}_{datetime.now().strftime('%H%M')}"

    def validate_new_position(self, ticker: str, strike: float, option_type: str, 
                            contracts: int, session_id: str = None) -> ValidationResult:
        """
        Validate new position against NT rules
        
        Rules:
        1. Max 1 position per ticker per session
        2. Max 5 contracts per position
        3. Must be valid option type (CALL/PUT)
        """
        if not session_id:
            session_id = self.get_current_session_id()
            
        # Check for existing position
        existing = self.get_open_position(ticker, session_id)
        
        if existing:
            return ValidationResult(
                valid=False,
                reason=f"Already have open {ticker} position this session ({existing.strike} {existing.option_type})",
                current_contracts=existing.contracts,
                available_contracts=0,
                existing_position=existing
            )
        
        # Validate contract count
        if contracts > 5:
            return ValidationResult(
                valid=False,
                reason=f"Cannot open {contracts} contracts - max 5 per position",
                current_contracts=0,
                available_contracts=5
            )
        
        if contracts < 1:
            return ValidationResult(
                valid=False,
                reason="Must open at least 1 contract",
                current_contracts=0,
                available_contracts=5
            )
        
        # Validate option type
        if option_type not in ['CALL', 'PUT']:
            return ValidationResult(
                valid=False,
                reason=f"Invalid option type: {option_type} (must be CALL or PUT)"
            )
        
        return ValidationResult(
            valid=True,
            reason="Position allowed",
            current_contracts=0,
            available_contracts=5 - contracts
        )

    def validate_add_contracts(self, ticker: str, additional_contracts: int, 
                             session_id: str = None) -> ValidationResult:
        """
        Validate adding contracts to existing position
        
        Rules:
        1. Position must exist
        2. Total contracts cannot exceed 5
        """
        if not session_id:
            session_id = self.get_current_session_id()
            
        existing = self.get_open_position(ticker, session_id)
        
        if not existing:
            return ValidationResult(
                valid=False,
                reason=f"No open {ticker} position to add contracts to",
                current_contracts=0,
                available_contracts=5
            )
        
        total_contracts = existing.contracts + additional_contracts
        
        if total_contracts > 5:
            available = 5 - existing.contracts
            return ValidationResult(
                valid=False,
                reason=f"Would exceed 5-contract limit ({existing.contracts} + {additional_contracts} = {total_contracts})",
                current_contracts=existing.contracts,
                available_contracts=available,
                existing_position=existing
            )
        
        return ValidationResult(
            valid=True,
            reason=f"Can add {additional_contracts} contracts to {ticker}",
            current_contracts=existing.contracts,
            available_contracts=5 - total_contracts,
            existing_position=existing
        )

    def validate_close_contracts(self, ticker: str, contracts_to_close: int, 
                               session_id: str = None) -> ValidationResult:
        """
        Validate closing contracts from existing position
        
        Rules:
        1. Position must exist
        2. Cannot close more than currently held
        """
        if not session_id:
            session_id = self.get_current_session_id()
            
        existing = self.get_open_position(ticker, session_id)
        
        if not existing:
            return ValidationResult(
                valid=False,
                reason=f"No open {ticker} position to close",
                current_contracts=0
            )
        
        if contracts_to_close > existing.contracts:
            return ValidationResult(
                valid=False,
                reason=f"Cannot close {contracts_to_close} contracts - only {existing.contracts} open",
                current_contracts=existing.contracts,
                existing_position=existing
            )
        
        return ValidationResult(
            valid=True,
            reason=f"Can close {contracts_to_close} of {existing.contracts} {ticker} contracts",
            current_contracts=existing.contracts,
            available_contracts=existing.contracts - contracts_to_close,
            existing_position=existing
        )

    def get_open_position(self, ticker: str, session_id: str) -> Optional[Position]:
        """Get open position for ticker in session"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM positions 
                WHERE ticker = ? AND session_id = ? AND status = 'OPEN'
            ''', (ticker, session_id))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            return Position(
                id=row['id'],
                ticker=row['ticker'],
                strike=row['strike'],
                option_type=row['option_type'],
                expiration=row['expiration'],
                contracts=row['contracts'],
                entry_price=row['entry_price'],
                session_id=row['session_id'],
                entry_time=datetime.fromisoformat(row['entry_time']),
                status=row['status']
            )

    def get_all_open_positions(self, session_id: str = None) -> List[Position]:
        """Get all open positions for session"""
        if not session_id:
            session_id = self.get_current_session_id()
            
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM positions 
                WHERE session_id = ? AND status = 'OPEN'
                ORDER BY entry_time
            ''', (session_id,))
            
            positions = []
            for row in cursor:
                positions.append(Position(
                    id=row['id'],
                    ticker=row['ticker'],
                    strike=row['strike'],
                    option_type=row['option_type'],
                    expiration=row['expiration'],
                    contracts=row['contracts'],
                    entry_price=row['entry_price'],
                    session_id=row['session_id'],
                    entry_time=datetime.fromisoformat(row['entry_time']),
                    status=row['status']
                ))
            
            return positions

    def open_position(self, ticker: str, strike: float, option_type: str, 
                     expiration: str, contracts: int, entry_price: float, 
                     session_id: str = None) -> Tuple[bool, str, str]:
        """
        Open new position after validation
        Returns: (success, message, position_id)
        """
        if not session_id:
            session_id = self.get_current_session_id()
            
        # Validate first
        validation = self.validate_new_position(ticker, strike, option_type, contracts, session_id)
        if not validation.valid:
            return False, validation.reason, ""
        
        # Create position
        position_id = str(uuid.uuid4())
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO positions (
                        id, ticker, strike, option_type, expiration, 
                        contracts, entry_price, session_id, entry_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    position_id, ticker, strike, option_type, expiration,
                    contracts, entry_price, session_id, datetime.now().isoformat()
                ))
                
            self.logger.info(f"Opened position: {ticker} {strike} {option_type} ({contracts} contracts)")
            return True, f"Position opened: {contracts}x {ticker} {strike} {option_type}", position_id
            
        except sqlite3.IntegrityError as e:
            if "idx_ticker_session" in str(e):
                return False, f"Already have open {ticker} position this session", ""
            else:
                return False, f"Database error: {str(e)}", ""

    def add_contracts(self, ticker: str, additional_contracts: int, 
                     session_id: str = None) -> Tuple[bool, str]:
        """Add contracts to existing position"""
        if not session_id:
            session_id = self.get_current_session_id()
            
        validation = self.validate_add_contracts(ticker, additional_contracts, session_id)
        if not validation.valid:
            return False, validation.reason
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE positions 
                    SET contracts = contracts + ?
                    WHERE ticker = ? AND session_id = ? AND status = 'OPEN'
                ''', (additional_contracts, ticker, session_id))
                
            new_total = validation.current_contracts + additional_contracts
            self.logger.info(f"Added {additional_contracts} contracts to {ticker} (total: {new_total})")
            return True, f"Added {additional_contracts} contracts - {ticker} total: {new_total}"
            
        except Exception as e:
            return False, f"Error adding contracts: {str(e)}"

    def close_contracts(self, ticker: str, contracts_to_close: int, 
                       session_id: str = None) -> Tuple[bool, str]:
        """Close contracts from existing position"""
        if not session_id:
            session_id = self.get_current_session_id()
            
        validation = self.validate_close_contracts(ticker, contracts_to_close, session_id)
        if not validation.valid:
            return False, validation.reason
        
        remaining = validation.current_contracts - contracts_to_close
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                if remaining == 0:
                    # Close entire position
                    conn.execute('''
                        UPDATE positions 
                        SET status = 'CLOSED', contracts = 0
                        WHERE ticker = ? AND session_id = ? AND status = 'OPEN'
                    ''', (ticker, session_id))
                    message = f"Closed entire {ticker} position ({contracts_to_close} contracts)"
                else:
                    # Partial close
                    conn.execute('''
                        UPDATE positions 
                        SET contracts = contracts - ?
                        WHERE ticker = ? AND session_id = ? AND status = 'OPEN'
                    ''', (contracts_to_close, ticker, session_id))
                    message = f"Closed {contracts_to_close} {ticker} contracts ({remaining} remaining)"
                
            self.logger.info(message)
            return True, message
            
        except Exception as e:
            return False, f"Error closing contracts: {str(e)}"

    def get_session_summary(self, session_id: str = None) -> Dict:
        """Get summary of current session"""
        if not session_id:
            session_id = self.get_current_session_id()
            
        positions = self.get_all_open_positions(session_id)
        
        total_contracts = sum(p.contracts for p in positions)
        total_positions = len(positions)
        
        return {
            'session_id': session_id,
            'total_positions': total_positions,
            'total_contracts': total_contracts,
            'max_positions_allowed': 6,  # Based on .srt analysis
            'max_contracts_per_position': 5,
            'positions': [
                {
                    'ticker': p.ticker,
                    'strike': p.strike,
                    'option_type': p.option_type,
                    'contracts': p.contracts,
                    'entry_price': p.entry_price
                } for p in positions
            ]
        }

# Example usage and testing
if __name__ == "__main__":
    validator = PositionValidator()
    
    # Test new position
    result = validator.validate_new_position("TSLA", 340.0, "CALL", 5)
    print(f"New TSLA position: {result.valid} - {result.reason}")
    
    if result.valid:
        success, msg, pos_id = validator.open_position("TSLA", 340.0, "CALL", "0DTE", 5, 2.50)
        print(f"Open result: {success} - {msg}")
    
    # Test duplicate position (should fail)
    result2 = validator.validate_new_position("TSLA", 350.0, "CALL", 3)
    print(f"Second TSLA position: {result2.valid} - {result2.reason}")
    
    # Test session summary
    summary = validator.get_session_summary()
    print(f"Session summary: {summary}")
