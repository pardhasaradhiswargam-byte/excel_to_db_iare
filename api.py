"""
Flask REST API for Excel to Firestore integration system
Converts the CLI-based tool into a web API
NOW WITH JWT AUTHENTICATION
"""

import os
import logging
import tempfile
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

from excel_processor import process_excel_file
from student_matcher import match_students
from firebase_operations import FirestoreOperations
from excel_utils import generate_company_year_id
from auth_utils import token_required  # Import JWT authentication

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# CORS configuration - MUST allow Authorization header for cross-origin!
CORS(app, 
     resources={
         r"/*": {
             "origins": [
                 "http://localhost:5173",
                 "http://localhost:5000",
                 "https://excel-to-db-iare.onrender.com",
                 "https://ai-to-db-iare.onrender.com",
                 "https://authentication-for-iare.onrender.com"
             ],
             "allow_headers": ["Content-Type", "Authorization"],
             "supports_credentials": True
         }
     })


# Configuration
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max file size
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



@app.route('/api/health', methods=['GET'])
def health_check():
    """Basic health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Excel to Firestore API'
    })


@app.route('/api/auth/set-token', methods=['POST'])
def set_token():
    """
    Receive token from auth service and set cookie for this domain
    This allows Excel service to read its own cookie
    """
    data = request.get_json()
    access_token = data.get('accessToken')
    refresh_token = data.get('refreshToken')
    
    if not access_token:
        return jsonify({'error': 'No token provided'}), 400
    
    response = jsonify({'message': 'Token set successfully'})
    
    # Set cookie for excel-to-db domain
    response.set_cookie(
        'accessToken',
        access_token,
        httponly=True,
        max_age=900,  # 15 minutes
        samesite='None',
        secure=True,
        path='/'
    )
    
    if refresh_token:
        response.set_cookie(
            'refreshToken',
            refresh_token,
            httponly=True,
            max_age=604800,  # 7 days
            samesite='None',
            secure=True,
            path='/'
        )
    
    return response, 200


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Clear cookies for this domain"""
    response = jsonify({'message': 'Logged out successfully'})
    
    response.set_cookie(
        'accessToken',
        '',
        max_age=0,
        samesite='None',
        secure=True,
        path='/'
    )
    response.set_cookie(
        'refreshToken',
        '',
        max_age=0,
        samesite='None',
        secure=True,
        path='/'
    )
    
    return response, 200


@app.route('/api/upload-round', methods=['POST'])
@token_required  # ← Protected with JWT authentication
def upload_round():
    """
    Upload Excel file with round data to Firestore
    NOW PROTECTED WITH JWT AUTHENTICATION
    
    Request (multipart/form-data):
        - file: Excel file (required, .xlsx or .xls)
        - company: Company name (required)
        - year: Year as integer (required)
        - roundNumber: Round number (optional, auto-calculated if not provided)
        - roundName: Round name (optional)
        - isFinal: Boolean string 'true'/'false' (optional, default: false)
    
    Returns:
        JSON with upload results
        
    Authentication: Requires valid JWT token in cookie
    """
    # Get authenticated user info
    user = request.current_user
    logger.info(f"🔒 Authenticated upload request from user: {user.get('username', 'Unknown')}")
    
    try:
        # Validate file presence
        if 'file' not in request.files:
            return jsonify({
                "success": False,
                "error": "No file provided. Please upload an Excel file."
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                "success": False,
                "error": "No file selected. Please select an Excel file."
            }), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                "success": False,
                "error": f"Invalid file type. Allowed extensions: {', '.join(ALLOWED_EXTENSIONS)}"
            }), 400
        
        # Validate required fields
        company = request.form.get('company', '').strip()
        year_str = request.form.get('year', '').strip()
        
        if not company:
            return jsonify({
                "success": False,
                "error": "Company name is required."
            }), 400
        
        if not year_str:
            return jsonify({
                "success": False,
                "error": "Year is required."
            }), 400
        
        try:
            year = int(year_str)
            if not (2000 <= year <= 2100):
                return jsonify({
                    "success": False,
                    "error": "Year must be between 2000 and 2100."
                }), 400
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Year must be a valid integer."
            }), 400
        
        # Optional fields
        round_number_str = request.form.get('roundNumber', '').strip()
        round_number = None
        if round_number_str:
            try:
                round_number = int(round_number_str)
                if round_number < 1:
                    return jsonify({
                        "success": False,
                        "error": "Round number must be >= 1."
                    }), 400
            except ValueError:
                return jsonify({
                    "success": False,
                    "error": "Round number must be a valid integer."
                }), 400
        
        round_name = request.form.get('roundName', '').strip() or None
        is_final_str = request.form.get('isFinal', 'false').strip().lower()
        is_final = is_final_str in ['true', '1', 'yes']
        
        logger.info(f"Processing upload - Company: {company}, Year: {year}, "
                   f"Round: {round_number or 'auto'}, Final: {is_final}")
        
        # Save file temporarily
        temp_dir = tempfile.mkdtemp()
        filename = secure_filename(file.filename)
        temp_path = os.path.join(temp_dir, filename)
        file.save(temp_path)
        
        try:
            # Step 1: Process Excel file
            logger.info("Processing Excel file...")
            excel_students, raw_columns, missing_fields = process_excel_file(temp_path)
            
            if not excel_students:
                return jsonify({
                    "success": False,
                    "error": "No student data found in Excel file."
                }), 400
            
            logger.info(f"Extracted {len(excel_students)} students from Excel")
            
            # Step 2: Initialize Firebase
            logger.info("Initializing Firebase connection...")
            firebase_ops = FirestoreOperations()
            
            # Step 3: Match students using OPTIMIZED queries (no bulk read!)
            logger.info("Matching students with existing database using targeted queries...")
            matched_updates, new_students = match_students(excel_students, firebase_ops)
            
            logger.info(f"✓ Matched {len(matched_updates)} existing, {len(new_students)} new students")
            
            # Combine all students
            all_students = matched_updates + new_students
            
            # Step 4: Determine round number if not provided
            if round_number is None:
                company_year_id = generate_company_year_id(company, year)
                existing_company = firebase_ops.get_company(company_year_id)
                
                if existing_company:
                    round_number = existing_company.get('currentRound', 0) + 1
                else:
                    round_number = 1
                
                logger.info(f"Auto-calculated round number: {round_number}")
            
            # Step 5: Process round upload
            logger.info("Uploading data to Firestore...")
            summary = firebase_ops.process_round_upload(
                company_name=company,
                year=year,
                round_number=round_number,
                round_name=round_name,
                is_final=is_final,
                excel_students=all_students,
                raw_columns=raw_columns
            )
            
            logger.info("Upload complete!")
            
            return jsonify({
                "success": True,
                "message": "Round uploaded successfully",
                "data": {
                    "companyYearId": summary['company_year_id'],
                    "roundId": summary['round_id'],
                    "totalStudents": summary['total_students'],
                    "matchedStudents": len(matched_updates),
                    "newStudents": len(new_students),
                    "placedStudents": summary.get('placed_students', 0),
                    "isFinalRound": summary['is_final_round'],
                    "missingFields": missing_fields,
                    "rawColumns": raw_columns
                }
            })
            
        finally:
            # Clean up temporary file
            try:
                os.remove(temp_path)
                os.rmdir(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to clean up temp file: {e}")
    
    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"Server error: {str(e)}"
        }), 500


if __name__ == '__main__':
    # Startup banner removed
    # Use PORT from environment (for Render/cloud deployment) or default to 5001
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=False)
  # ← Changed port to 5005
