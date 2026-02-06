"""
Firebase Firestore operations module
Handles all database interactions and multi-collection updates
"""

import firebase_admin
from firebase_admin import credentials, firestore
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime
from config import FIREBASE_CREDENTIALS_PATH, FIRESTORE_BATCH_SIZE
from excel_utils import generate_company_year_id, generate_round_id, generate_row_id, clean_dict

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class FirestoreOperations:
    """Handle all Firestore database operations"""
    
    def __init__(self):
        """Initialize Firebase app and Firestore client"""
        try:
            # Initialize Firebase Admin SDK only if not already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
                firebase_admin.initialize_app(cred)
                logger.info("Firebase initialized successfully")
            
            self.db = firestore.client()
        except Exception as e:
            logger.error(f"Error initializing Firebase: {e}")
            raise
    
    def find_student_by_roll_number(self, roll_number: str) -> Optional[Dict]:
        """
        Find student by roll number using targeted query (only 1 read)
        
        Args:
            roll_number: Roll number to search for
            
        Returns:
            Student document or None
        """
        try:
            students_ref = self.db.collection('students')
            # Targeted query - only reads matching documents!
            query = students_ref.where('rollNumber', '==', roll_number).limit(1)
            docs = list(query.stream())
            
            if docs:
                student_data = docs[0].to_dict()
                student_data['id'] = docs[0].id
                return student_data
            return None
        except Exception as e:
            logger.error(f"Error finding student by roll number: {e}")
            return None
    
    def find_student_by_email(self, email: str) -> Optional[Dict]:
        """
        Find student by email using targeted query (only 1 read)
        
        Args:
            email: Email to search for
            
        Returns:
            Student document or None
        """
        try:
            students_ref = self.db.collection('students')
            # Targeted query - only reads matching documents!
            query = students_ref.where('email', '==', email).limit(1)
            docs = list(query.stream())
            
            if docs:
                student_data = docs[0].to_dict()
                student_data['id'] = docs[0].id
                return student_data
            return None
        except Exception as e:
            logger.error(f"Error finding student by email: {e}")
            return None
    
    def find_student_by_name(self, name: str) -> Optional[Dict]:
        """
        Find student by exact name match using targeted query (only 1 read)
        
        Args:
            name: Name to search for
            
        Returns:
            Student document or None
        """
        try:
            students_ref = self.db.collection('students')
            # Targeted query - only reads matching documents!
            query = students_ref.where('name', '==', name).limit(1)
            docs = list(query.stream())
            
            if docs:
                student_data = docs[0].to_dict()
                student_data['id'] = docs[0].id
                return student_data
            return None
        except Exception as e:
            logger.error(f"Error finding student by name: {e}")
            return None
    
    def get_company(self, company_year_id: str) -> Optional[Dict]:
        """
        Get company document
        
        Args:
            company_year_id: Company year ID
            
        Returns:
            Company document or None
        """
        try:
            doc_ref = self.db.collection('companies').document(company_year_id)
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            logger.error(f"Error fetching company: {e}")
            return None
    
    def create_or_update_company(self, company_name: str, year: int, 
                                 round_number: int, is_final: bool) -> str:
        """
        Create or update company document
        
        Args:
            company_name: Company name
            year: Year
            round_number: Current round number
            is_final: Whether this is the final round
            
        Returns:
            Company year ID
        """
        company_year_id = generate_company_year_id(company_name, year)
        doc_ref = self.db.collection('companies').document(company_year_id)
        
        existing = doc_ref.get()
        
        # Track changes for systemStats
        was_running_now_completed = False
        is_new_company = False
        
        if existing.exists:
            # Update existing company
            existing_data = existing.to_dict()
            old_status = existing_data.get('status', 'running')
            
            update_data = {
                'currentRound': round_number,
                'updatedAt': firestore.SERVER_TIMESTAMP
            }
            
            if is_final:
                update_data['finalRound'] = round_number
                update_data['status'] = 'completed'
                
                # Track if company transitioned from running to completed
                if old_status == 'running':
                    was_running_now_completed = True
                    logger.info(f"Company {company_year_id} status: running → completed")
            
            # Update total rounds if current round is higher
            if round_number > existing_data.get('totalRounds', 0):
                update_data['totalRounds'] = round_number
            
            doc_ref.update(update_data)
            logger.info(f"Updated company: {company_year_id}")
        else:
            # Create new company
            is_new_company = True
            company_data = {
                'companyName': company_name,
                'year': year,
                'status': 'completed' if is_final else 'running',
                'currentRound': round_number,
                'finalRound': round_number if is_final else None,
                'totalRounds': round_number,
                'totalPlaced': 0,
                'totalApplied': 0,
                'createdAt': firestore.SERVER_TIMESTAMP,
                'updatedAt': firestore.SERVER_TIMESTAMP
            }
            
            doc_ref.set(company_data)
            logger.info(f"Created company: {company_year_id}")
        
        return company_year_id
    
    def get_previous_round_students(self, company_year_id: str, round_number: int) -> List[str]:
        """
        Get list of student IDs from the previous round
        
        Args:
            company_year_id: Company year ID
            round_number: Current round number
            
        Returns:
            List of student IDs from previous round
        """
        if round_number <= 1:
            return []
        
        previous_round_number = round_number - 1
        previous_round_id = generate_round_id(company_year_id, previous_round_number)
        
        try:
            # Fetch all student IDs from previous round's data subcollection
            previous_round_data = (
                self.db.collection('companies')
                .document(company_year_id)
                .collection('rounds')
                .document(previous_round_id)
                .collection('data')
                .stream()
            )
            
            student_ids = []
            for doc in previous_round_data:
                data = doc.to_dict()
                if 'studentId' in data and data['studentId']:
                    student_ids.append(data['studentId'])
            
            logger.info(f"Found {len(student_ids)} students from previous round {previous_round_number}")
            return student_ids
        except Exception as e:
            logger.error(f"Error fetching previous round students: {e}")
            return []
    
    def mark_eliminated_students(self, eliminated_student_ids: List[str], 
                                 company_year_id: str, year: int, 
                                 round_reached: int) -> None:
        """
        Mark eliminated students as not_selected
        
        Args:
            eliminated_student_ids: List of student IDs to mark as eliminated
            company_year_id: Company year ID
            year: Year
            round_reached: The final round they reached before elimination
        """
        if not eliminated_student_ids:
            logger.info("No eliminated students to mark")
            return
        
        batch = self.db.batch()
        operation_count = 0
        
        for student_id in eliminated_student_ids:
            student_ref = self.db.collection('students').document(student_id)
            
            # Check if student exists
            existing = student_ref.get()
            if not existing.exists:
                logger.warning(f"Student {student_id} not found, skipping elimination marking")
                continue
            
            existing_data = existing.to_dict()
            company_status = existing_data.get('companyStatus', {})
            
            # Update company status to not_selected
            if company_year_id in company_status:
                company_status[company_year_id]['status'] = 'not_selected'
                company_status[company_year_id]['finalSelection'] = False
                # Keep roundReached at the last round they participated in
                
                update_data = {
                    'companyStatus': company_status,
                    'updatedAt': firestore.SERVER_TIMESTAMP
                }
                
                batch.update(student_ref, update_data)
                operation_count += 1
                logger.debug(f"Marked {student_id} as eliminated (reached round {round_reached})")
                
                # Commit batch if limit reached
                if operation_count >= FIRESTORE_BATCH_SIZE:
                    batch.commit()
                    logger.info(f"Committed batch of {operation_count} eliminated student updates")
                    batch = self.db.batch()
                    operation_count = 0
        
        # Commit remaining operations
        if operation_count > 0:
            batch.commit()
            logger.info(f"✓ Marked {operation_count} students as eliminated (not_selected)")
    

    def add_round(self, company_year_id: str, round_number: int, 
                  round_name: Optional[str], is_final: bool,
                  raw_columns: List[str], students_data: List[Dict]) -> str:
        """
        Add round to company's rounds subcollection with student data
        
        Args:
            company_year_id: Company year ID
            round_number: Round number
            round_name: Round name (optional)
            is_final: Whether this is final round
            raw_columns: List of Excel column headers
            students_data: List of student data with student IDs
            
        Returns:
            Round ID
        """
        round_id = generate_round_id(company_year_id, round_number)
        
        # Create round document
        round_ref = (self.db.collection('companies')
                    .document(company_year_id)
                    .collection('rounds')
                    .document(round_id))
        
        round_data = {
            'roundNumber': round_number,
            'roundName': round_name,
            'rawColumns': raw_columns,
            'studentCount': len(students_data),
            'isFinalRound': is_final,
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        
        round_ref.set(round_data)
        logger.info(f"Created round: {round_id} with {len(students_data)} students")
        
        # Add student data rows in batches
        self._add_round_data_batch(company_year_id, round_id, students_data, is_final)
        
        return round_id
    
    def _add_round_data_batch(self, company_year_id: str, round_id: str, 
                              students_data: List[Dict], is_final: bool) -> None:
        """
        Add student data rows to round in batches
        
        Args:
            company_year_id: Company year ID
            round_id: Round ID
            students_data: List of student data
            is_final: Whether this is final round
        """
        batch = self.db.batch()
        operation_count = 0
        
        for student in students_data:
            student_id = student['id']
            row_id = generate_row_id(student_id, round_id)
            
            row_ref = (self.db.collection('companies')
                      .document(company_year_id)
                      .collection('rounds')
                      .document(round_id)
                      .collection('data')
                      .document(row_id))
            
            row_data = {
                'rowData': student['excel_data'].get('rowData', {}),
                'studentId': student_id,
                'status': 'qualified' if is_final else 'pending'
            }
            
            batch.set(row_ref, row_data)
            operation_count += 1
            
            # Commit batch if limit reached
            if operation_count >= FIRESTORE_BATCH_SIZE:
                batch.commit()
                logger.info(f"Committed batch of {operation_count} round data rows")
                batch = self.db.batch()
                operation_count = 0
        
        # Commit remaining operations
        if operation_count > 0:
            batch.commit()
            logger.info(f"Committed final batch of {operation_count} round data rows")
    
    def add_placements(self, company_year_id: str, students_data: List[Dict]) -> None:
        """
        Add students to company's placements subcollection (for final rounds)
        
        Args:
            company_year_id: Company year ID
            students_data: List of student data
        """
        batch = self.db.batch()
        operation_count = 0
        
        for student in students_data:
            student_id = student['id']
            
            placement_ref = (self.db.collection('companies')
                           .document(company_year_id)
                           .collection('placements')
                           .document(student_id))
            
            placement_data = {
                'rowData': student['excel_data'].get('rowData', {}),
                'timestamp': firestore.SERVER_TIMESTAMP
            }
            
            batch.set(placement_ref, placement_data)
            operation_count += 1
            
            # Commit batch if limit reached
            if operation_count >= FIRESTORE_BATCH_SIZE:
                batch.commit()
                logger.info(f"Committed batch of {operation_count} placements")
                batch = self.db.batch()
                operation_count = 0
        
        # Commit remaining operations
        if operation_count > 0:
            batch.commit()
            logger.info(f"Committed final batch of {operation_count} placements")
    
    def update_students(self, students_data: List[Dict], company_year_id: str, 
                       company_name: str, year: int, round_number: int, 
                       is_final: bool) -> None:
        """
        Create or update student documents and track systemStats changes
        
        Args:
            students_data: List of student data (matched or new)
            company_year_id: Company year ID
            company_name: Company name
            year: Year
            round_number: Round number
            is_final: Whether this is final round
        """
        batch = self.db.batch()
        operation_count = 0
        
        # Track changes for systemStats
        newly_placed_count = 0  # Students who transition from not_placed to placed
        new_students_count = 0  # Brand new student documents created
        
        for student in students_data:
            student_id = student['id']
            student_ref = self.db.collection('students').document(student_id)
            
            # Check if student exists
            existing = student_ref.get()
            
            if existing.exists:
                # Update existing student
                existing_data = existing.to_dict()
                old_status = existing_data.get('currentStatus', 'not_placed')
                
                # Import normalization functions
                from excel_utils import normalize_roll_number, normalize_name, normalize_email
                
                # Update identifiers (fill missing fields with NORMALIZED values)
                update_data = {}
                for field in ['rollNumber', 'name', 'email']:
                    if field not in existing_data or not existing_data.get(field):
                        if field in student['data'] and student['data'].get(field):
                            # Normalize before saving
                            if field == 'rollNumber':
                                update_data[field] = normalize_roll_number(student['data'][field])
                            elif field == 'name':
                                update_data[field] = normalize_name(student['data'][field])
                            elif field == 'email':
                                update_data[field] = normalize_email(student['data'][field])
                
                # Update company status
                company_status = existing_data.get('companyStatus', {})
                company_status[company_year_id] = {
                    'status': 'selected' if is_final else 'in_process',
                    'roundReached': round_number,
                    'finalSelection': True if is_final else None,
                    'year': year
                }
                update_data['companyStatus'] = company_status
                
                # Update placement info if final round
                if is_final:
                    selected_companies = existing_data.get('selectedCompanies', [])
                    if company_name not in selected_companies:
                        selected_companies.append(company_name)
                    update_data['selectedCompanies'] = selected_companies
                    
                    # ✅ Track newly placed students for systemStats
                    if old_status != 'placed':
                        newly_placed_count += 1
                        logger.info(f"Student {student_id} newly placed (was: {old_status})")
                    
                    update_data['currentStatus'] = 'placed'
                    update_data['totalOffers'] = existing_data.get('totalOffers', 0) + 1
                
                update_data['updatedAt'] = firestore.SERVER_TIMESTAMP
                
                batch.update(student_ref, update_data)
                logger.debug(f"Updating student: {student_id}")
            else:
                # Create new student - NORMALIZE all fields for consistent matching!
                from excel_utils import normalize_roll_number, normalize_name, normalize_email
                
                student_data = {
                    'rollNumber': normalize_roll_number(student['data'].get('rollNumber', '')) if student['data'].get('rollNumber') else '',
                    'name': normalize_name(student['data'].get('name', '')) if student['data'].get('name') else '',
                    'email': normalize_email(student['data'].get('email', '')) if student['data'].get('email') else '',
                    'companyStatus': {
                        company_year_id: {
                            'status': 'selected' if is_final else 'in_process',
                            'roundReached': round_number,
                            'finalSelection': True if is_final else None,
                            'year': year
                        }
                    },
                    'selectedCompanies': [company_name] if is_final else [],
                    'currentStatus': 'placed' if is_final else 'not_placed',
                    'totalOffers': 1 if is_final else 0,
                    'updatedAt': firestore.SERVER_TIMESTAMP
                }
                
                # Clean empty fields
                student_data = clean_dict(student_data)
                
                batch.set(student_ref, student_data)
                logger.debug(f"Creating new student: {student_id}")
                
                # ✅ Track new students for systemStats
                new_students_count += 1
                if is_final:
                    newly_placed_count += 1  # New student who is immediately placed
            
            operation_count += 1
            
            # Commit batch if limit reached
            if operation_count >= FIRESTORE_BATCH_SIZE:
                batch.commit()
                logger.info(f"Committed batch of {operation_count} student updates")
                batch = self.db.batch()
                operation_count = 0
        
        # Commit remaining operations
        if operation_count > 0:
            batch.commit()
            logger.info(f"Committed final batch of {operation_count} student updates")

    
    def update_company_statistics(self, company_year_id: str, 
                                  total_applied: int, total_placed: int, 
                                  round_number: int) -> None:
        """
        Update company statistics
        
        Args:
            company_year_id: Company year ID
            total_applied: Total students applied (only set on round 1)
            total_placed: Total students placed
            round_number: Current round number
        """
        doc_ref = self.db.collection('companies').document(company_year_id)
        
        update_data = {
            'updatedAt': firestore.SERVER_TIMESTAMP
        }
        
        # Only set totalApplied on the first round
        # Subsequent rounds are eliminations, not new applicants
        if round_number == 1:
            update_data['totalApplied'] = total_applied
            logger.info(f"Set company totalApplied: {total_applied} (Round 1)")
        
        # Update totalPlaced if there are placements
        if total_placed > 0:
            update_data['totalPlaced'] = firestore.Increment(total_placed)
            logger.info(f"Updated company totalPlaced: +{total_placed}")
        
        doc_ref.update(update_data)
        logger.info(f"Updated company statistics for round {round_number}")
    
    def update_yearly_analytics(self, year: int, company_year_id: str, 
                                company_name: str, placed_count: int, 
                                is_new_company: bool, is_final: bool) -> None:
        """
        Update yearly analytics
        
        Args:
            year: Year
            company_year_id: Company year ID
            company_name: Company name
            placed_count: Number of students placed
            is_new_company: Whether this is a new company
            is_final: Whether this is final round
        """
        year_ref = self.db.collection('years').document(str(year))
        
        # Get existing year data
        year_doc = year_ref.get()
        
        if year_doc.exists:
            year_data = year_doc.to_dict()
        else:
            # Create new year document
            year_data = {
                'totalCompanies': 0,
                'completedCompanies': 0,
                'runningCompanies': 0,
                'totalPlaced': 0,
                'totalStudentsParticipated': 0,
                'companyWise': {}
            }
        
        # Update company-wise stats
        company_wise = year_data.get('companyWise', {})
        
        # Save previous status BEFORE updating to detect transitions
        previous_status = None
        if company_year_id in company_wise:
            previous_status = company_wise[company_year_id].get('status')
            # Update existing company entry
            company_wise[company_year_id]['placed'] += placed_count
            if is_final:
                company_wise[company_year_id]['status'] = 'completed'
        else:
            # Add new company entry
            company_wise[company_year_id] = {
                'companyName': company_name,
                'placed': placed_count,
                'status': 'completed' if is_final else 'running'
            }
        
        # Update totals
        update_data = {
            'companyWise': company_wise,
            'totalPlaced': firestore.Increment(placed_count)
        }
        
        if is_new_company:
            # New company being added
            update_data['totalCompanies'] = firestore.Increment(1)
            if is_final:
                # New company that's already completed (first round is final)
                update_data['completedCompanies'] = firestore.Increment(1)
            else:
                # New company that's still running
                update_data['runningCompanies'] = firestore.Increment(1)
        else:
            # Existing company - check if status changed from running to completed
            if is_final and previous_status == 'running':
                update_data['runningCompanies'] = firestore.Increment(-1)
                update_data['completedCompanies'] = firestore.Increment(1)
                logger.info(f"Company {company_year_id} transitioned from running to completed")
        
        year_ref.set(update_data, merge=True)
        logger.info(f"Updated yearly analytics for {year}")
    
    def process_round_upload(self, company_name: str, year: int, 
                            round_number: int, round_name: Optional[str],
                            is_final: bool, excel_students: List[Dict],
                            raw_columns: List[str]) -> Dict[str, Any]:
        """
        Main orchestration function to process round upload
        Handles all database updates across all collections
        
        Args:
            company_name: Company name
            year: Year
            round_number: Round number
            round_name: Round name (optional)
            is_final: Whether this is final round
            excel_students: List of student data from Excel (with IDs)
            raw_columns: List of Excel column headers
            
        Returns:
            Summary of operations performed
        """
        logger.info(f"Processing round upload for {company_name} {year} - Round {round_number}")
        
        # 1. Create or update company
        company_year_id = self.create_or_update_company(
            company_name, year, round_number, is_final
        )
        
        # Check if this is a new company
        existing_year_doc = self.db.collection('years').document(str(year)).get()
        is_new_company = False
        if existing_year_doc.exists:
            company_wise = existing_year_doc.to_dict().get('companyWise', {})
            is_new_company = company_year_id not in company_wise
        else:
            is_new_company = True
        
        # 2. Add round with student data
        round_id = self.add_round(
            company_year_id, round_number, round_name, is_final,
            raw_columns, excel_students
        )
        
        # 2.5. Mark eliminated students from previous round
        if round_number > 1:
            logger.info(f"Checking for eliminated students from Round {round_number - 1}...")
            previous_round_students = self.get_previous_round_students(company_year_id, round_number)
            current_round_students = {s['id'] for s in excel_students}
            eliminated_students = [sid for sid in previous_round_students if sid not in current_round_students]
            
            if eliminated_students:
                self.mark_eliminated_students(
                    eliminated_students, 
                    company_year_id, 
                    year, 
                    round_number - 1  # They reached the previous round
                )
                logger.info(f"✓ Marked {len(eliminated_students)} students as eliminated from Round {round_number - 1}")
            else:
                logger.info("All students from previous round are continuing")
        
        # 3. Add placements if final round
        placed_count = 0
        if is_final:
            self.add_placements(company_year_id, excel_students)
            placed_count = len(excel_students)
        
        # 4. Update student documents
        self.update_students(
            excel_students, company_year_id, company_name, year,
            round_number, is_final
        )
        
        # 5. Update company statistics
        total_applied = len(excel_students)
        self.update_company_statistics(company_year_id, total_applied, placed_count, round_number)
        
        # 6. Update yearly analytics
        self.update_yearly_analytics(
            year, company_year_id, company_name, placed_count,
            is_new_company, is_final
        )
        
        summary = {
            'company_year_id': company_year_id,
            'round_id': round_id,
            'total_students': len(excel_students),
            'placed_students': placed_count,
            'is_final_round': is_final
        }
        
        logger.info(f"Round upload complete: {summary}")
        return summary
