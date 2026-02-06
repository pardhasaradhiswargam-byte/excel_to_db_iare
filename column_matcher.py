"""
AI-powered column matching using Groq API
Intelligently identifies which Excel columns map to rollNumber, name, and email
"""

import json
import logging
from typing import Dict, List, Tuple, Optional
from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL, COLUMN_MAPPINGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ColumnMatcher:
    """AI-powered column matcher using Groq API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize ColumnMatcher
        
        Args:
            api_key: Groq API key (uses config if not provided)
        """
        self.api_key = api_key or GROQ_API_KEY
        self.model = GROQ_MODEL
        self.client = None
        
        if self.api_key:
            try:
                self.client = Groq(api_key=self.api_key)
                logger.info("Groq API client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Groq client: {e}")
                self.client = None
    
    def _build_prompt(self, columns: List[str], sample_rows: List[Dict]) -> str:
        """
        Build prompt for Groq API
        
        Args:
            columns: List of Excel column names
            sample_rows: Sample data rows (max 2 rows)
            
        Returns:
            Formatted prompt string
        """
        prompt = """You are a COLUMN MAPPER. Your job is to map Excel column names to student data field types.

**YOUR TASK:**
Look at the data inside each column and tell me which column contains:
1. rollNumber - Student roll/registration number
2. name - Student name
3. email - Student email

**IMPORTANT RULES:**
- The column NAME might be generic (like "Column1", "Column2") - IGNORE the column name!
- Look at the DATA VALUES inside each column to determine what it contains
- A rollNumber looks like: "22951A0516", "23951A6291", "24951A1234" (alphanumeric student ID)
- A rollNumber is NOT a serial number like: 1, 2, 3, 4, 5
- A name looks like: "AKSHAYA M S", "JOHN DOE", "MARIA GARCIA"
- An email looks like: "student@gmail.com", "user123@example.com"

**THE DATA I'M GIVING YOU:**

COLUMNS IN THE EXCEL FILE:
"""
        
        # List all columns
        for i, col in enumerate(columns, 1):
            prompt += f'{i}. "{col}"\n'
        
        prompt += "\nSAMPLE DATA FROM THE EXCEL FILE:\n"
        prompt += "(Look at what's INSIDE each column)\n\n"
        
        # Show sample data in a clearer format
        for row_idx, row in enumerate(sample_rows, 1):
            prompt += f"Row {row_idx}:\n"
            for col_name, value in row.items():
                # Show column name and its value
                prompt += f'  Column "{col_name}" contains: {value}\n'
            prompt += "\n"
        
        prompt += """**YOUR JOB:**
Based on the DATA VALUES (not column names), tell me:
- Which column contains the rollNumber (the alphanumeric student ID)?
- Which column contains the name?
- Which column contains the email?

**RESPONSE FORMAT:**
Give me ONLY a JSON object like this:
{
  "rollNumber": "exact_column_name_here",
  "name": "exact_column_name_here",
  "email": "exact_column_name_here",
  "missing": []
}

**EXAMPLE:**
If you see:
- Column "Column1" contains: 22951A0516 ← This is clearly a rollNumber (alphanumeric ID)
- Column "Candidate Name" contains: AKSHAYA M S ← This is clearly a name
- Column "Email ID" contains: akshayams121@gmail.com ← This is clearly an email

Then respond:
{
  "rollNumber": "Column1",
  "name": "Candidate Name",
  "email": "Email ID",
  "missing": []
}

**CRITICAL:**
- Use the EXACT column name as shown above (case-sensitive)
- If you can't find a column for a field, put null and add it to "missing"
- Look at the DATA not the column name
- Respond with ONLY the JSON, nothing else

NOW ANALYZE THE DATA ABOVE AND RESPOND:
"""
        return prompt
    
    def analyze_columns(self, columns: List[str], sample_rows: List[Dict]) -> Tuple[Dict[str, str], List[str]]:
        """
        Analyze columns using Groq API to identify field mappings
        
        Args:
            columns: List of Excel column names
            sample_rows: Sample data rows (2 rows recommended)
            
        Returns:
            Tuple of (column_mapping, missing_fields)
            - column_mapping: Dict mapping field names to column names
            - missing_fields: List of missing required fields
        """
        # If no API key, fall back to hardcoded matching
        if not self.client:
            logger.warning("Groq API not available, falling back to hardcoded matching")
            return self._fallback_matching(columns)
        
        try:
            # Limit sample rows to 2
            sample_rows = sample_rows[:2]
            
            # Build prompt
            prompt = self._build_prompt(columns, sample_rows)
            
            # Log what we're sending to the AI
            logger.info("=" * 80)
            logger.info("SENDING TO AI FOR ANALYSIS:")
            logger.info("=" * 80)
            logger.info(f"Columns: {columns}")
            logger.info(f"Sample Row 1: {sample_rows[0] if len(sample_rows) > 0 else 'N/A'}")
            logger.info(f"Sample Row 2: {sample_rows[1] if len(sample_rows) > 1 else 'N/A'}")
            logger.info("=" * 80)
            
            logger.info("Sending column analysis request to Groq API...")
            
            # Call Groq API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data mapping expert. Analyze the actual data values to identify field types. Respond only with valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            # Parse response
            response_text = response.choices[0].message.content.strip()
            
            logger.info("=" * 80)
            logger.info("AI RESPONSE:")
            logger.info("=" * 80)
            logger.info(response_text)
            logger.info("=" * 80)
            
            # Extract JSON from response (might have markdown code blocks)
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text)
            
            # Build column mapping (exclude null values)
            column_mapping = {}
            for field in ['rollNumber', 'name', 'email']:
                if result.get(field) and result[field] != "null":
                    column_mapping[field] = result[field]
            
            missing_fields = result.get('missing', [])
            
            logger.info("=" * 80)
            logger.info("AI IDENTIFIED MAPPINGS:")
            logger.info("=" * 80)
            for field, col_name in column_mapping.items():
                logger.info(f"  ✓ {field} → '{col_name}'")
            if missing_fields:
                logger.warning(f"  ⚠ Missing fields: {missing_fields}")
            logger.info("=" * 80)
            
            return column_mapping, missing_fields
            
        except Exception as e:
            logger.error(f"Error calling Groq API: {e}", exc_info=True)
            logger.warning("Falling back to hardcoded matching")
            return self._fallback_matching(columns)
    
    def _fallback_matching(self, columns: List[str]) -> Tuple[Dict[str, str], List[str]]:
        """
        Fallback to hardcoded column matching with intelligent data analysis
        
        Args:
            columns: List of Excel column names
            
        Returns:
            Tuple of (column_mapping, missing_fields)
        """
        from excel_utils import normalize_text
        import re
        
        column_mapping = {}
        normalized_columns = {col: normalize_text(col) for col in columns}
        
        # Try hardcoded column name matching first
        # Find roll number column
        for col, norm_col in normalized_columns.items():
            if norm_col in COLUMN_MAPPINGS['roll_number']:
                column_mapping['rollNumber'] = col
                break
        
        # Find name column
        for col, norm_col in normalized_columns.items():
            if norm_col in COLUMN_MAPPINGS['name']:
                column_mapping['name'] = col
                break
        
        # Find email column
        for col, norm_col in normalized_columns.items():
            if norm_col in COLUMN_MAPPINGS['email']:
                column_mapping['email'] = col
                break
        
        # If we didn't find all fields, try intelligent matching based on column names
        if 'rollNumber' not in column_mapping:
            # Look for columns like "Column1", "ID", "Student ID", etc.
            for col in columns:
                col_lower = col.lower()
                # Check if it's a generic column that might contain roll number
                if any(pattern in col_lower for pattern in ['column1', 'col1', 'id', 'roll', 'regno', 'reg']):
                    column_mapping['rollNumber'] = col
                    logger.info(f"Guessed rollNumber from column name: {col}")
                    break
        
        if 'name' not in column_mapping:
            # Look for columns like "Candidate Name", "Student Name", etc.
            for col in columns:
                col_lower = col.lower()
                if 'name' in col_lower and 'college' not in col_lower and 'company' not in col_lower:
                    column_mapping['name'] = col
                    logger.info(f"Guessed name from column name: {col}")
                    break
        
        # Determine missing fields
        required_fields = ['rollNumber', 'name', 'email']
        missing_fields = [field for field in required_fields if field not in column_mapping]
        
        logger.info(f"Fallback matching identified: {column_mapping}")
        if missing_fields:
            logger.warning(f"Missing fields: {missing_fields}")
        
        return column_mapping, missing_fields


def match_columns(columns: List[str], sample_rows: List[Dict], 
                  api_key: Optional[str] = None) -> Tuple[Dict[str, str], List[str]]:
    """
    Convenience function to match columns using AI
    
    Args:
        columns: List of Excel column names
        sample_rows: Sample data rows
        api_key: Optional Groq API key
        
    Returns:
        Tuple of (column_mapping, missing_fields)
    """
    matcher = ColumnMatcher(api_key)
    return matcher.analyze_columns(columns, sample_rows)
