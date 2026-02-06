"""
Shared JWT Authentication Utilities
Used across AI and Excel services to verify tokens from Auth service
"""
import jwt
import os
from functools import wraps
from flask import request, jsonify
from dotenv import load_dotenv

load_dotenv()

# Load JWT secrets (same as auth service)
JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
JWT_REFRESH_SECRET_KEY = os.getenv('JWT_REFRESH_SECRET_KEY')

if not JWT_SECRET_KEY:
    raise ValueError("JWT_SECRET_KEY not found in .env file!")


def verify_token(token):
    """
    Verify JWT access token
    
    Args:
        token: JWT token string
        
    Returns:
        dict: Decoded token payload if valid
        None: If token is invalid or expired
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def token_required(f):
    """
    Decorator to protect endpoints with JWT authentication
    
    Supports both:
    - Authorization header: "Bearer <token>" (for cross-origin)
    - Cookie: accessToken (for same-origin)
    
    Usage:
        @app.route('/protected')
        @token_required
        def protected_route():
            user = request.current_user
            return jsonify({'username': user['username']})
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Try to get token from Authorization header first (for cross-origin requests)
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        
        # Fallback to cookie (for same-origin requests)
        if not token:
            token = request.cookies.get('accessToken')
        
        if not token:
            return jsonify({
                'success': False,
                'error': 'Authentication required. Please log in.'
            }), 401
        
        # Verify token
        payload = verify_token(token)
        if not payload:
            return jsonify({
                'success': False,
                'error': 'Invalid or expired token. Please log in again.'
            }), 401
        
        # Attach user info to request for use in route
        request.current_user = payload
        
        return f(*args, **kwargs)
    
    return decorated



def admin_required(f):
    """
    Decorator to protect endpoints requiring admin role
    Must be used AFTER @token_required
    
    Usage:
        @app.route('/admin-only')
        @token_required
        @admin_required
        def admin_route():
            return jsonify({'message': 'Admin access granted'})
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        user = getattr(request, 'current_user', None)
        
        if not user:
            return jsonify({
                'success': False,
                'error': 'Authentication required'
            }), 401
        
        if user.get('role') != 'admin':
            return jsonify({
                'success': False,
                'error': 'Admin access required'
            }), 403
        
        return f(*args, **kwargs)
    
    return decorated


def get_current_user():
    """
    Get current authenticated user from request
    
    Returns:
        dict: User payload if authenticated
        None: If not authenticated
    """
    return getattr(request, 'current_user', None)
