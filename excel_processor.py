"""
Excel data extraction and processing module
"""

import pandas as pd
from typing import List, Dict, Tuple, Optional
import logging
from config import COLUMN_MAPPINGS
from excel_utils import normalize_text, is_empty_value
from column_matcher import ColumnMatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExcelProcessor:
    """Process Excel files and extract student data"""
    
    def __init__(self, excel_path: str, use_ai_matching: bool = True):
        """
        Initialize Excel processor
        
        Args:
            excel_path: Path to Excel file
            use_ai_matching: Whether to use AI-powered column matching (default: True)
        """
        self.excel_path = excel_path
        self.df = None
        self.column_map = {}
        self.use_ai_matching = use_ai_matching
        self.missing_fields = []
        
    def read_excel(self) -> bool:
        """
        Read Excel file into pandas DataFrame
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.df = pd.read_excel(self.excel_path, engine='openpyxl')
            logger.info(f"Successfully read Excel file: {self.excel_path}")
            logger.info(f"Found {len(self.df)} rows and {len(self.df.columns)} columns")
            return True
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            return False
    
    def identify_columns(self) -> None:
        """
        Identify which columns contain roll number, name, and email
        Uses AI-powered matching if enabled, otherwise falls back to hardcoded matching
        """
        if self.df is None:
            return
        
        columns = list(self.df.columns)
        
        if self.use_ai_matching:
            logger.info("Using AI-powered column matching...")
            
            # Get sample rows for AI analysis (convert to dict)
            sample_rows = []
            for idx in range(min(2, len(self.df))):
                row_dict = {}
                for col in columns:
                    val = self.df.iloc[idx][col]
                    if not is_empty_value(val):
                        if isinstance(val, (int, float)):
                            row_dict[col] = val
                        else:
                            row_dict[col] = str(val).strip()
                sample_rows.append(row_dict)
            
            # Use AI to match columns
            matcher = ColumnMatcher()
            ai_mapping, self.missing_fields = matcher.analyze_columns(columns, sample_rows)
            
            # Convert AI mapping format to internal format
            # AI returns {"rollNumber": "col", "name": "col", "email": "col"}
            # We need {"roll_number": "col", "name": "col", "email": "col"}
            if 'rollNumber' in ai_mapping:
                self.column_map['roll_number'] = ai_mapping['rollNumber']
                logger.info(f"✓ Roll Number → '{ai_mapping['rollNumber']}' column")
            
            if 'name' in ai_mapping:
                self.column_map['name'] = ai_mapping['name']
                logger.info(f"✓ Name → '{ai_mapping['name']}' column")
            
            if 'email' in ai_mapping:
                self.column_map['email'] = ai_mapping['email']
                logger.info(f"✓ Email → '{ai_mapping['email']}' column")
        
        else:
            logger.info("Using hardcoded column matching...")
            # Fallback to hardcoded matching
            normalized_columns = {col: normalize_text(col) for col in columns}
            
            for col, norm_col in normalized_columns.items():
                if norm_col in COLUMN_MAPPINGS['roll_number']:
                    self.column_map['roll_number'] = col
                    logger.info(f"Identified roll number column: {col}")
                    break
            
            for col, norm_col in normalized_columns.items():
                if norm_col in COLUMN_MAPPINGS['name']:
                    self.column_map['name'] = col
                    logger.info(f"Identified name column: {col}")
                    break
            
            for col, norm_col in normalized_columns.items():
                if norm_col in COLUMN_MAPPINGS['email']:
                    self.column_map['email'] = col
                    logger.info(f"Identified email column: {col}")
                    break
            
            # Determine missing fields
            required_fields = ['rollNumber', 'name', 'email']
            mapped_fields = []
            if 'roll_number' in self.column_map:
                mapped_fields.append('rollNumber')
            if 'name' in self.column_map:
                mapped_fields.append('name')
            if 'email' in self.column_map:
                mapped_fields.append('email')
            self.missing_fields = [f for f in required_fields if f not in mapped_fields]
        
        if not any(k in self.column_map for k in ['roll_number', 'name', 'email']):
            logger.warning("Could not identify any student identifier columns!")
        
        if self.missing_fields:
            logger.warning(f"⚠ Missing required fields: {', '.join(self.missing_fields)}")
    
    def extract_student_data(self) -> List[Dict]:
        """
        Extract student data from Excel file
        
        Returns:
            List of dictionaries containing student data and additional columns
        """
        if self.df is None:
            logger.error("Excel file not loaded")
            return []
        
        students_data = []
        
        # Get all column names for raw columns
        raw_columns = list(self.df.columns)
        
        for idx, row in self.df.iterrows():
            student = {}
            
            # Extract student identifiers
            if 'roll_number' in self.column_map:
                val = row[self.column_map['roll_number']]
                if not is_empty_value(val):
                    student['rollNumber'] = str(val).strip()
            
            if 'name' in self.column_map:
                val = row[self.column_map['name']]
                if not is_empty_value(val):
                    student['name'] = str(val).strip()
            
            if 'email' in self.column_map:
                val = row[self.column_map['email']]
                if not is_empty_value(val):
                    student['email'] = str(val).strip()
            
            # Extract all row data (including additional columns)
            row_data = {}
            for col in self.df.columns:
                val = row[col]
                if not is_empty_value(val):
                    # Convert to appropriate type
                    if isinstance(val, (int, float)):
                        row_data[col] = val
                    else:
                        row_data[col] = str(val).strip()
            
            # Store both identifiers and full row data
            student['rowData'] = row_data
            
            # Only add if at least one identifier is present
            if any(k in student for k in ['rollNumber', 'name', 'email']):
                students_data.append(student)
            else:
                logger.warning(f"Row {idx + 1} has no identifiable student data, skipping")
        
        logger.info(f"Extracted {len(students_data)} student records")
        return students_data
    
    def get_raw_columns(self) -> List[str]:
        """
        Get list of all column headers from Excel
        
        Returns:
            List of column names
        """
        if self.df is None:
            return []
        return list(self.df.columns)
    
    def get_missing_fields(self) -> List[str]:
        """
        Get list of missing required fields
        
        Returns:
            List of missing field names
        """
        return self.missing_fields
    
    def process(self) -> Tuple[List[Dict], List[str], List[str]]:
        """
        Process Excel file - read, identify columns, and extract data
        
        Returns:
            Tuple of (students_data, raw_columns, missing_fields)
        """
        if not self.read_excel():
            return [], [], []
        
        self.identify_columns()
        students_data = self.extract_student_data()
        raw_columns = self.get_raw_columns()
        missing_fields = self.get_missing_fields()
        
        return students_data, raw_columns, missing_fields


def process_excel_file(excel_path: str, use_ai_matching: bool = True) -> Tuple[List[Dict], List[str], List[str]]:
    """
    Convenience function to process an Excel file
    
    Args:
        excel_path: Path to Excel file
        use_ai_matching: Whether to use AI-powered column matching
        
    Returns:
        Tuple of (students_data, raw_columns, missing_fields)
    """
    processor = ExcelProcessor(excel_path, use_ai_matching)
    return processor.process()
