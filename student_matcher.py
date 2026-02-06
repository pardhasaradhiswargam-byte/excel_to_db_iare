"""
Intelligent student matching module with priority-based lookup
"""

from typing import List, Dict, Tuple, Optional
from fuzzywuzzy import fuzz
import logging
from config import MATCHING_CONFIG
from excel_utils import (
    normalize_roll_number, normalize_name, normalize_email,
    generate_student_id, is_empty_value
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StudentMatcher:
    """Match students from Excel with existing Firestore students using optimized queries"""
    
    def __init__(self, firestore_ops):
        """
        Initialize student matcher with Firestore operations
        
        Args:
            firestore_ops: FirestoreOperations instance for querying
        """ 
        self.firestore_ops = firestore_ops
        self.name_threshold = MATCHING_CONFIG['name_similarity_threshold']
        self.use_fuzzy = MATCHING_CONFIG['use_fuzzy_matching']
        
        logger.info("StudentMatcher initialized with query-based matching (optimized for low reads)")
    
    def match_student(self, excel_student: Dict) -> Tuple[Optional[Dict], str, int]:
        """
        Match a student from Excel with existing students using targeted queries
        Priority: roll_number > name > email
        
        OPTIMIZED: Only queries what's needed instead of loading all students!
        
        Args:
            excel_student: Student data from Excel
            
        Returns:
            Tuple of (matched_student, match_type, confidence_score)
            - matched_student: Matched student dict or None
            - match_type: 'roll_number', 'name', 'email', or 'none'
            - confidence_score: 0-100
        """
        logger.info(f"🔍 Matching student: {excel_student.get('name', 'Unknown')}")
        
        # Priority 1: Roll Number (exact match) - 1 read if exists
        if 'rollNumber' in excel_student and not is_empty_value(excel_student['rollNumber']):
            raw_roll = excel_student['rollNumber']
            norm_roll = normalize_roll_number(raw_roll)
            logger.info(f"  📋 Checking rollNumber: '{raw_roll}' → normalized: '{norm_roll}'")
            if norm_roll:
                matched = self.firestore_ops.find_student_by_roll_number(norm_roll)
                if matched:
                    logger.info(f"  ✅ MATCHED by roll number: {norm_roll} → student_id: {matched.get('id')}")
                    return matched, 'roll_number', 100
                else:
                    logger.info(f"  ❌ No match for rollNumber: '{norm_roll}'")
        else:
            logger.info(f"  ⏭️  Skipping rollNumber check (empty or missing)")
        
        # Priority 2: Name (exact match first) - 1 read if exists
        if 'name' in excel_student and not is_empty_value(excel_student['name']):
            raw_name = excel_student['name']
            norm_name = normalize_name(raw_name)
            logger.info(f"  👤 Checking name: '{raw_name}' → normalized: '{norm_name}'")
            if norm_name:
                matched = self.firestore_ops.find_student_by_name(norm_name)
                if matched:
                    logger.info(f"  ✅ MATCHED by name: {norm_name} → student_id: {matched.get('id')}")
                    return matched, 'name', 100
                else:
                    logger.info(f"  ❌ No match for name: '{norm_name}'")
        else:
            logger.info(f"  ⏭️  Skipping name check (empty or missing)")
        
        # Priority 3: Email (exact match) - 1 read if exists  
        if 'email' in excel_student and not is_empty_value(excel_student['email']):
            raw_email = excel_student['email']
            norm_email = normalize_email(raw_email)
            logger.info(f"  ✉️  Checking email: '{raw_email}' → normalized: '{norm_email}'")
            if norm_email:
                matched = self.firestore_ops.find_student_by_email(norm_email)
                if matched:
                    logger.info(f"  ✅ MATCHED by email: {norm_email} → student_id: {matched.get('id')}")
                    return matched, 'email', 100
                else:
                    logger.info(f"  ❌ No match for email: '{norm_email}'")
        else:
            logger.info(f"  ⏭️  Skipping email check (empty or missing)")
        
        # No match found - will create new student
        logger.info(f"  🆕 NO MATCH FOUND - Will create new student")
        return None, 'none', 0
    
    def merge_student_data(self, existing_student: Dict, new_data: Dict) -> Dict:
        """
        Merge new student data with existing data
        Fill in missing fields, never overwrite existing non-empty fields
        
        Args:
            existing_student: Existing student document
            new_data: New data from Excel
            
        Returns:
            Merged student data
        """
        merged = existing_student.copy()
        
        # Merge student identifiers (fill only if empty)
        for field in ['rollNumber', 'name', 'email']:
            if is_empty_value(merged.get(field)) and not is_empty_value(new_data.get(field)):
                merged[field] = new_data[field]
                logger.info(f"Filled missing field '{field}' for student {existing_student.get('id')}")
        
        return merged
    
    def process_excel_students(self, excel_students: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Process all students from Excel and match with existing students
        
        Args:
            excel_students: List of student data from Excel
            
        Returns:
            Tuple of (matched_updates, new_students)
            - matched_updates: List of updates for existing students
            - new_students: List of new students to create
        """
        matched_updates = []
        new_students = []
        
        for excel_student in excel_students:
            matched_student, match_type, confidence = self.match_student(excel_student)
            
            if matched_student:
                # Student exists - merge data
                student_id = matched_student.get('id')
                merged_data = self.merge_student_data(matched_student, excel_student)
                
                matched_updates.append({
                    'id': student_id,
                    'data': merged_data,
                    'match_type': match_type,
                    'confidence': confidence,
                    'excel_data': excel_student
                })
                logger.info(f"Matched student {student_id} via {match_type} "
                           f"(confidence: {confidence})")
            else:
                # New student - generate ID
                student_id = generate_student_id(
                    roll_number=excel_student.get('rollNumber'),
                    name=excel_student.get('name'),
                    email=excel_student.get('email')
                )
                
                new_students.append({
                    'id': student_id,
                    'data': excel_student,
                    'excel_data': excel_student
                })
                logger.info(f"New student: {student_id}")
        
        logger.info(f"Processing complete: {len(matched_updates)} matched, "
                   f"{len(new_students)} new students")
        
        return matched_updates, new_students


def match_students(excel_students: List[Dict], 
                  firestore_ops) -> Tuple[List[Dict], List[Dict]]:
    """
    Convenience function to match students using optimized queries
    
    Args:
        excel_students: List of student data from Excel
        firestore_ops: FirestoreOperations instance for querying
        
    Returns:
        Tuple of (matched_updates, new_students)
    """
    matcher = StudentMatcher(firestore_ops)
    return matcher.process_excel_students(excel_students)
