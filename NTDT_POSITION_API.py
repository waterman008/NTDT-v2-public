#!/usr/bin/env python3
"""
NTDT Position API
Flask API integration for position validation with VA web interface

Endpoints:
- POST /api/validate_position - Validate new position
- POST /api/open_position - Open new position
- POST /api/close_position - Close/reduce position
- GET /api/get_positions - Get current positions
- GET /api/session_summary - Get session summary
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from NTDT_POSITION_VALIDATOR import PositionValidator, ValidationResult
from NTDT_FAST_RISK_ENGINE import FastRiskEngine

app = Flask(__name__)
CORS(app)

# Database path for shared use
DB_PATH = 'ntdt_trading_session.db'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('NTDT_API')

@app.route('/api/validate_position', methods=['POST'])
def validate_position():
    """
    Validate position entry against NT rules
    
    Request: {
        "ticker": "TSLA",
        "strike": 340.0,
        "option_type": "CALL",
        "contracts": 5,
        "action": "OPEN"  # or "ADD" or "CLOSE"
    }
    """
    try:
        data = request.get_json()
        
        ticker = data.get('ticker', '').upper()
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', '').upper()
        contracts = int(data.get('contracts', 0))
        action = data.get('action', 'OPEN').upper()
        
        if action == 'OPEN':
            # Create fresh instances per request to avoid threading issues
            risk_engine = FastRiskEngine(DB_PATH)
            validator = PositionValidator(DB_PATH)
            
            # Run fast risk checks first
            risk_result = risk_engine.validate_buy_to_open(ticker, strike, option_type, contracts, float(data.get('entry_price', 0)))
            if not risk_result.valid:
                return jsonify({
                    'valid': False,
                    'reason': risk_result.reason,
                    'risk_level': risk_result.risk_level,
                    'warnings': risk_result.warnings,
                    'error': 'RISK_CHECK_FAILED'
                }), 400
            
            # Run position-specific validation  
            result = validator.validate_buy_to_open(ticker, strike, option_type, contracts)
            
            # Add risk warnings to response
            if risk_result.warnings:
                result.warnings = getattr(result, 'warnings', []) + risk_result.warnings
                
        elif action == 'ADD':
            validator = PositionValidator(DB_PATH)
            result = validator.validate_add_contracts(ticker, contracts)
        elif action == 'CLOSE':
            validator = PositionValidator(DB_PATH)
            result = validator.validate_close_contracts(ticker, contracts)
        else:
            return jsonify({
                'valid': False,
                'reason': f"Invalid action: {action}",
                'error': 'INVALID_ACTION'
            }), 400
        
        response = {
            'valid': result.valid,
            'reason': result.reason,
            'current_contracts': result.current_contracts,
            'available_contracts': result.available_contracts,
            'warnings': getattr(result, 'warnings', [])
        }
        
        if result.existing_position:
            response['existing_position'] = {
                'ticker': result.existing_position.ticker,
                'strike': result.existing_position.strike,
                'option_type': result.existing_position.option_type,
                'contracts': result.existing_position.contracts,
                'entry_price': result.existing_position.entry_price
            }
        
        return jsonify(response)
        
    except ValueError as e:
        return jsonify({
            'valid': False,
            'reason': f"Invalid input: {str(e)}",
            'error': 'INVALID_INPUT'
        }), 400
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({
            'valid': False,
            'reason': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/open_position', methods=['POST'])
def open_position():
    """
    Open new position after validation
    
    Request: {
        "ticker": "TSLA",
        "strike": 340.0,
        "option_type": "CALL",
        "expiration": "0DTE",
        "contracts": 5,
        "entry_price": 2.50
    }
    """
    try:
        data = request.get_json()
        
        ticker = data.get('ticker', '').upper()
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', '').upper()
        expiration = data.get('expiration', '0DTE')
        contracts = int(data.get('contracts', 0))
        entry_price = float(data.get('entry_price', 0))
        
        # Create fresh instances per request to avoid threading issues
        risk_engine = FastRiskEngine(DB_PATH)
        validator = PositionValidator(DB_PATH)
        
        # Run fast risk checks first
        risk_result = risk_engine.validate_buy_to_open(ticker, strike, option_type, contracts, entry_price)
        if not risk_result.valid:
            return jsonify({
                'success': False,
                'message': risk_result.reason,
                'risk_level': risk_result.risk_level,
                'warnings': risk_result.warnings,
                'error': 'RISK_CHECK_FAILED'
            }), 400
        
        success, message, position_id = validator.open_position(
            ticker, strike, option_type, expiration, contracts, entry_price
        )
        
        if success:
            # Record execution in risk engine
            risk_engine.record_execution("OPEN", ticker, contracts)
            return jsonify({
                'success': True,
                'message': message,
                'position_id': position_id,
                'position': {
                    'ticker': ticker,
                    'strike': strike,
                    'option_type': option_type,
                    'contracts': contracts,
                    'entry_price': entry_price
                }
            })
        else:
            return jsonify({
                'success': False,
                'message': message,
                'error': 'POSITION_REJECTED'
            }), 400
            
    except ValueError as e:
        return jsonify({
            'success': False,
            'message': f"Invalid input: {str(e)}",
            'error': 'INVALID_INPUT'
        }), 400
    except Exception as e:
        logger.error(f"Position creation error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/close_position', methods=['POST'])
def close_position():
    """
    Close or reduce existing position
    
    Request: {
        "ticker": "TSLA",
        "contracts": 2,  # Number to close
        "close_all": false  # If true, close entire position
    }
    """
    try:
        data = request.get_json()
        
        ticker = data.get('ticker', '').upper()
        close_all = data.get('close_all', False)
        
        # Create fresh instance per request
        validator = PositionValidator(DB_PATH)
        
        if close_all:
            # Get current position to close all contracts
            existing = validator.get_open_position(ticker, validator.get_current_session_id())
            if not existing:
                return jsonify({
                    'success': False,
                    'message': f"No open {ticker} position to close",
                    'error': 'NO_POSITION'
                }), 400
            contracts = existing.contracts
        else:
            contracts = int(data.get('contracts', 0))
        
        success, message = validator.close_contracts(ticker, contracts)
        
        if success:
            return jsonify({
                'success': True,
                'message': message,
                'contracts_closed': contracts
            })
        else:
            return jsonify({
                'success': False,
                'message': message,
                'error': 'CLOSE_REJECTED'
            }), 400
            
    except ValueError as e:
        return jsonify({
            'success': False,
            'message': f"Invalid input: {str(e)}",
            'error': 'INVALID_INPUT'
        }), 400
    except Exception as e:
        logger.error(f"Position close error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/get_positions', methods=['GET'])
def get_positions():
    """Get all open positions for current session"""
    try:
        validator = PositionValidator(DB_PATH)
        session_id = request.args.get('session_id')
        positions = validator.get_all_open_positions(session_id)
        
        return jsonify({
            'success': True,
            'positions': [
                {
                    'id': pos.id,
                    'ticker': pos.ticker,
                    'strike': pos.strike,
                    'option_type': pos.option_type,
                    'expiration': pos.expiration,
                    'contracts': pos.contracts,
                    'entry_price': pos.entry_price,
                    'entry_time': pos.entry_time.isoformat(),
                    'status': pos.status
                } for pos in positions
            ]
        })
        
    except Exception as e:
        logger.error(f"Get positions error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/session_summary', methods=['GET'])
def session_summary():
    """Get summary of current trading session"""
    try:
        validator = PositionValidator(DB_PATH)
        session_id = request.args.get('session_id')
        summary = validator.get_session_summary(session_id)
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        logger.error(f"Session summary error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/risk_summary', methods=['GET'])
def risk_summary():
    """Get current session risk metrics"""
    try:
        risk_engine = FastRiskEngine(DB_PATH)
        summary = risk_engine.get_session_risk_summary()
        return jsonify({
            'success': True,
            'risk_summary': summary
        })
        
    except Exception as e:
        logger.error(f"Risk summary error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Internal server error',
            'error': 'SERVER_ERROR'
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'NTDT Position API',
        'version': '1.0.0'
    })

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'message': 'Endpoint not found',
        'error': 'NOT_FOUND'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'message': 'Method not allowed',
        'error': 'METHOD_NOT_ALLOWED'
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'message': 'Internal server error',
        'error': 'SERVER_ERROR'
    }), 500

@app.route('/')
def home():
    """Serve the main VA interface"""
    try:
        with open('NTDT_FULL_COMPACT_INTERFACE.html', 'r') as f:
            return f.read()
    except FileNotFoundError:
        return """
        <h1>NTDT VA Interface</h1>
        <p>Interface file not found. Please ensure NTDT_FULL_COMPACT_INTERFACE.html is deployed.</p>
        <p>API Status: Running</p>
        <p>Available endpoints:</p>
        <ul>
            <li>POST /api/validate_position</li>
            <li>POST /api/open_position</li>
            <li>POST /api/close_position</li>
            <li>GET /api/get_positions</li>
            <li>GET /api/session_summary</li>
            <li>GET /api/risk_summary</li>
        </ul>
        """

if __name__ == '__main__':
    logger.info("Starting NTDT Position API...")
    logger.info("Endpoints available:")
    logger.info("  POST /api/validate_position - Validate position entry")
    logger.info("  POST /api/open_position - Open new position")
    logger.info("  POST /api/close_position - Close position")
    logger.info("  GET /api/get_positions - Get current positions")
    logger.info("  GET /api/session_summary - Get session summary")
    logger.info("  GET /api/risk_summary - Get session risk metrics")
    
    import os
    port = int(os.environ.get('PORT', 5003))
    app.run(host='0.0.0.0', port=port, debug=False)
