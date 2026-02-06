"""
Configuration file for Excel to Firestore integration system
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Firebase configuration
FIREBASE_CREDENTIALS_PATH = os.getenv('FIREBASE_CREDENTIALS_PATH', r"C:\Users\pardh\Downloads\private.json")

# Groq API configuration
GROQ_API_KEY = os.getenv('GROQ_API_KEY', '')
GROQ_MODEL = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')

# Student matching configuration
MATCHING_CONFIG = {
    'name_similarity_threshold': int(os.getenv('NAME_SIMILARITY_THRESHOLD', '80')),  # Minimum similarity score for fuzzy name matching (0-100)
    'use_fuzzy_matching': os.getenv('USE_FUZZY_MATCHING', 'True').lower() == 'true',  # Enable fuzzy matching for names
}

# Column name variations for student identifiers
COLUMN_MAPPINGS = {
    'roll_number': [
        'roll number', 'rollnumber', 'roll no', 'rollno', 'roll_no',
        'student id', 'studentid', 'student_id', 'registration number',
        'regno', 'reg no', 'reg_no', 'registration no'
    ],
    'name': [
        'name', 'student name', 'studentname', 'student_name',
        'full name', 'fullname', 'full_name'
    ],
    'email': [
        'email', 'e-mail', 'email id', 'emailid', 'email_id',
        'mail', 'student email', 'studentemail'
    ]
}

# Firestore batch size limit
FIRESTORE_BATCH_SIZE = int(os.getenv('FIRESTORE_BATCH_SIZE', '500'))

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}
