"""
Utility functions for Excel to Firestore integration system
"""

import re
import hashlib
from datetime import datetime
from typing import Optional


def normalize_text(text: Optional[str]) -> str:
    """
    Normalize text by converting to lowercase, removing extra whitespace,
    and stripping special characters
    
    Args:
        text: Input text string
        
    Returns:
        Normalized text string
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Convert to lowercase and strip whitespace
    text = text.lower().strip()
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text


def normalize_roll_number(roll_number: Optional[str]) -> str:
    """
    Normalize roll number by removing spaces and special characters,
    converting to uppercase for consistent matching
    
    Args:
        roll_number: Input roll number
        
    Returns:
        Normalized roll number
    """
    if not roll_number or not isinstance(roll_number, str):
        return ""
    
    # Remove all spaces and special characters, convert to uppercase
    normalized = re.sub(r'[^a-zA-Z0-9]', '', str(roll_number).strip())
    return normalized.upper()


def normalize_email(email: Optional[str]) -> str:
    """
    Normalize email by converting to lowercase and stripping whitespace
    
    Args:
        email: Input email address
        
    Returns:
        Normalized email address
    """
    if not email or not isinstance(email, str):
        return ""
    
    return email.lower().strip()


def normalize_name(name: Optional[str]) -> str:
    """
    Normalize name for fuzzy matching
    
    Args:
        name: Input name
        
    Returns:
        Normalized name
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to lowercase, remove extra spaces
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    
    # Remove common titles and suffixes
    name = re.sub(r'\b(mr|mrs|ms|dr|prof)\b\.?', '', name)
    
    return name.strip()


def generate_company_year_id(company_name: str, year: int) -> str:
    """
    Generate companyYearId by concatenating company name and year
    
    Args:
        company_name: Company name
        year: Year
        
    Returns:
        Company year ID (e.g., "Google2025")
    """
    # Remove spaces and special characters from company name
    clean_name = re.sub(r'[^a-zA-Z0-9]', '', company_name)
    return f"{clean_name}{year}"


def generate_round_id(company_year_id: str, round_number: int) -> str:
    """
    Generate round ID
    
    Args:
        company_year_id: Company year ID
        round_number: Round number
        
    Returns:
        Round ID (e.g., "Google2025_round_1")
    """
    return f"{company_year_id}_round_{round_number}"


def generate_student_id(roll_number: Optional[str] = None, 
                       name: Optional[str] = None, 
                       email: Optional[str] = None) -> str:
    """
    Generate unique student ID based on available identifiers
    
    Priority: roll_number > email > name
    
    Args:
        roll_number: Student roll number
        name: Student name
        email: Student email
        
    Returns:
        Unique student ID
    """
    if roll_number:
        # Use normalized roll number as ID
        return f"student_{normalize_roll_number(roll_number)}"
    elif email:
        # Use email username part as ID
        email_clean = normalize_email(email).split('@')[0]
        return f"student_{email_clean}"
    elif name:
        # Use name hash as ID
        name_hash = hashlib.md5(normalize_name(name).encode()).hexdigest()[:8]
        return f"student_{name_hash}"
    else:
        # Generate random ID
        random_hash = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
        return f"student_{random_hash}"


def generate_row_id(student_id: str, round_id: str) -> str:
    """
    Generate row ID for round data
    
    Args:
        student_id: Student ID
        round_id: Round ID
        
    Returns:
        Row ID
    """
    return f"{round_id}_{student_id}"


def is_empty_value(value) -> bool:
    """
    Check if a value is empty (None, empty string, or NaN)
    
    Args:
        value: Value to check
        
    Returns:
        True if value is empty, False otherwise
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    # Check for pandas NaN
    try:
        import pandas as pd
        if pd.isna(value):
            return True
    except:
        pass
    
    return False


def clean_dict(data: dict) -> dict:
    """
    Remove empty values from dictionary
    
    Args:
        data: Input dictionary
        
    Returns:
        Dictionary with non-empty values only
    """
    return {k: v for k, v in data.items() if not is_empty_value(v)}
