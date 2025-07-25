import streamlit as st
import pandas as pd
import json
import os
import uuid
from datetime import datetime, date, timedelta
import plotly.express as px
import plotly.graph_objects as go
import time
from pathlib import Path
import logging
import threading
import queue
import re
from io import BytesIO, StringIO
import zipfile

# Google Drive integration imports
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
    GDRIVE_AVAILABLE = True
except ImportError:
    GDRIVE_AVAILABLE = False
    st.error("Google Drive integration not available. Please install: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")

# Try to import openpyxl for Excel exports
try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

# Try to import google.generativeai, handle if not available
try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False

# Configure minimal logging - only essential events
import logging
from logging.handlers import RotatingFileHandler

# Create logs directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure main logger - minimal logging
logging.basicConfig(
    level=logging.WARNING,  # Only warnings and errors
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            LOGS_DIR / 'hedge_fund_app.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=2
        )
    ]
)

logger = logging.getLogger(__name__)

# Session tracking (minimal)
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())[:8]

SESSION_ID = st.session_state.session_id

def log_essential(message):
    """Log only essential events"""
    logger.info(f"[{SESSION_ID}] {message}")

def log_extraction_progress(step, details=""):
    """Log extraction progress only"""
    logger.info(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")

def log_extraction_step(step, details="", level="INFO"):
    """Log extraction step with level"""
    if level == "ERROR":
        logger.error(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")
    elif level == "WARNING":
        logger.warning(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")
    else:
        logger.info(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")

def log_profile_saved(profile_type, name, company=""):
    """Log when profiles are saved"""
    company_str = f" at {company}" if company else ""
    logger.info(f"[{SESSION_ID}] SAVED: {profile_type} - {name}{company_str}")

def log_user_action(action, details=""):
    """Log user actions"""
    logger.info(f"[{SESSION_ID}] USER: {action} - {details}")

# Minimal session start log
log_essential(f"Session started")

# Configure page
st.set_page_config(
    page_title="Asian Hedge Fund Talent Map",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Google Drive Integration ---
class GoogleDriveManager:
    def __init__(self):
        self.service = None
        self.folder_id = None

    def authenticate(self, credentials_dict):
        """Authenticate with Google Drive using service account credentials"""
        try:
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.service = build('drive', 'v3', credentials=credentials)
            log_essential("Google Drive authentication successful")
            return True
        except Exception as e:
            log_essential(f"Google Drive authentication failed: {e}")
            return False

    def find_or_create_folder(self, folder_name="HedgeFund_Data"):
        """Find or create the data folder in Google Drive"""
        try:
            # Search for existing folder
            results = self.service.files().list(
                q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'",
                spaces='drive'
            ).execute()

            folders = results.get('files', [])

            if folders:
                self.folder_id = folders[0]['id']
                log_essential(f"Found existing folder: {folder_name} (ID: {self.folder_id})")
            else:
                # Create new folder
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                folder = self.service.files().create(body=folder_metadata).execute()
                self.folder_id = folder.get('id')
                log_essential(f"Created new folder: {folder_name} (ID: {self.folder_id})")

            return True
        except Exception as e:
            log_essential(f"Error with folder operations: {e}")
            return False

    def upload_csv(self, df, filename):
        """Upload DataFrame as CSV to Google Drive"""
        try:
            if not self.service or not self.folder_id:
                return False

            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            # Check if file exists
            existing_files = self.service.files().list(
                q=f"name='{filename}' and parents in '{self.folder_id}'",
                spaces='drive'
            ).execute()

            files = existing_files.get('files', [])

            media = MediaIoBaseUpload(
                BytesIO(csv_data.encode('utf-8')),
                mimetype='text/csv'
            )

            if files:
                # Update existing file
                file_id = files[0]['id']
                self.service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
                log_essential(f"Updated file in Google Drive: {filename}")
            else:
                # Create new file
                file_metadata = {
                    'name': filename,
                    'parents': [self.folder_id]
                }
                self.service.files().create(
                    body=file_metadata,
                    media_body=media
                ).execute()
                log_essential(f"Created new file in Google Drive: {filename}")

            return True
        except Exception as e:
            log_essential(f"Error uploading CSV to Google Drive: {e}")
            return False

    def download_csv(self, filename):
        """Download CSV from Google Drive as DataFrame"""
        try:
            if not self.service:
                log_essential("download_csv: No service available")
                return None
    
            log_essential(f"Attempting to download: {filename}")
    
            # First try to find in our designated folder
            file_id = None
            if self.folder_id:
                results = self.service.files().list(
                    q=f"name='{filename}' and parents in '{self.folder_id}'",
                    spaces='drive',
                    fields='files(id, name, parents)'
                ).execute()
    
                files = results.get('files', [])
                if files:
                    file_id = files[0]['id']
                    log_essential(f"Found {filename} in HedgeFund_Data folder (ID: {file_id})")
    
            # If not found in folder, search globally
            if not file_id:
                log_essential(f"File {filename} not found in folder, searching globally...")
                global_results = self.service.files().list(
                    q=f"name='{filename}' and mimeType='text/csv'",
                    spaces='drive',
                    fields='files(id, name, parents)'
                ).execute()
    
                global_files = global_results.get('files', [])
                if global_files:
                    file_id = global_files[0]['id']
                    log_essential(f"Found {filename} globally (ID: {file_id})")
                    
                    # Move it to our folder for future use
                    if self.folder_id:
                        try:
                            previous_parents = ",".join(global_files[0].get('parents', []))
                            self.service.files().update(
                                fileId=file_id,
                                addParents=self.folder_id,
                                removeParents=previous_parents
                            ).execute()
                            log_essential(f"Moved {filename} to HedgeFund_Data folder")
                        except Exception as move_error:
                            log_essential(f"Could not move {filename}: {move_error}")
    
            if not file_id:
                log_essential(f"File not found anywhere: {filename}")
                return None
    
            # Download the file
            log_essential(f"Downloading file {filename} (ID: {file_id})")
            request = self.service.files().get_media(fileId=file_id)
            file_buffer = BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
    
            done = False
            while done is False:
                status, done = downloader.next_chunk()
    
            # Read CSV data
            file_buffer.seek(0)
            df = pd.read_csv(file_buffer)
            log_essential(f"Successfully downloaded {filename}: {len(df)} rows, {len(df.columns)} columns")
            
            # Log first few column names for debugging
            if len(df.columns) > 0:
                log_essential(f"CSV columns: {list(df.columns[:5])}")
            
            return df
    
        except Exception as e:
            log_essential(f"Error downloading CSV {filename}: {e}")
            return None

    def list_files(self):
        """List all files in the data folder"""
        try:
            if not self.service or not self.folder_id:
                log_essential("list_files: No service or folder_id")
                return []
    
            # First, let's see what's actually in our target folder
            results = self.service.files().list(
                q=f"parents in '{self.folder_id}'",
                spaces='drive',
                fields='files(id, name, modifiedTime, size, mimeType)'
            ).execute()
    
            files_in_folder = results.get('files', [])
            log_essential(f"Files found in HedgeFund_Data folder: {len(files_in_folder)}")
            
            for file in files_in_folder:
                log_essential(f"File in folder: {file['name']} (type: {file.get('mimeType', 'unknown')})")
    
            # If no files in folder, let's check if they're in root and move them
            if len(files_in_folder) == 0:
                log_essential("No files in HedgeFund_Data folder, checking root directory...")
                
                # Look for our CSV files in root directory
                root_results = self.service.files().list(
                    q="name contains 'hedge_fund_data' and mimeType='text/csv'",
                    spaces='drive',
                    fields='files(id, name, modifiedTime, size, parents)'
                ).execute()
                
                root_files = root_results.get('files', [])
                log_essential(f"Found {len(root_files)} hedge fund CSV files in root")
                
                # Move files to our folder
                for file in root_files:
                    try:
                        log_essential(f"Moving file {file['name']} to HedgeFund_Data folder")
                        
                        # Remove from current parents and add to our folder
                        previous_parents = ",".join(file.get('parents', []))
                        
                        self.service.files().update(
                            fileId=file['id'],
                            addParents=self.folder_id,
                            removeParents=previous_parents,
                            fields='id, parents'
                        ).execute()
                        
                        log_essential(f"Successfully moved {file['name']} to HedgeFund_Data folder")
                        
                    except Exception as move_error:
                        log_essential(f"Error moving file {file['name']}: {move_error}")
                
                # Now list files again after moving
                results = self.service.files().list(
                    q=f"parents in '{self.folder_id}'",
                    spaces='drive',
                    fields='files(id, name, modifiedTime, size)'
                ).execute()
                
                files_in_folder = results.get('files', [])
                log_essential(f"Files in folder after moving: {len(files_in_folder)}")
    
            return files_in_folder
    
        except Exception as e:
            log_essential(f"Error listing files: {e}")
            return []

# Initialize Google Drive Manager
@st.cache_resource
def get_drive_manager():
    return GoogleDriveManager()

drive_manager = get_drive_manager()

# --- Helper function to safely get string values ---
def safe_get(data, key, default='Unknown'):
    """Safely get a value from dict, ensuring it's not None"""
    try:
        if data is None:
            return default
        value = data.get(key, default)
        return value if value is not None and str(value).strip() != '' else default
    except Exception as e:
        logger.warning(f"Error in safe_get for key {key}: {e}")
        return default

# --- BULLETPROOF Duplicate Detection Functions ---
def normalize_name(name):
    """Normalize name for comparison - very thorough"""
    if not name or name == 'Unknown':
        return ""

    # Convert to lowercase and strip
    normalized = name.strip().lower()

    # Remove common punctuation and spaces
    normalized = re.sub(r'[.,\-_\'\"]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)  # Multiple spaces to single space
    normalized = normalized.strip()

    # Handle common name variations
    normalized = normalized.replace('jr.', 'jr').replace('sr.', 'sr')
    normalized = normalized.replace('dr.', 'dr').replace('mr.', 'mr').replace('ms.', 'ms')

    return normalized

def normalize_company(company):
    """Normalize company name for comparison - very thorough"""
    if not company or company == 'Unknown':
        return ""

    # Convert to lowercase and strip
    normalized = company.strip().lower()

    # Remove common punctuation
    normalized = re.sub(r'[.,\-_\'\"]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)  # Multiple spaces to single space

    # Remove common company suffixes (more comprehensive)
    suffixes = [
        ' ltd', ' limited', ' inc', ' incorporated', ' llc', ' pllc',
        ' corp', ' corporation', ' co', ' company', ' group', ' holdings',
        ' partners', ' partnership', ' lp', ' llp', ' pc', ' pllc',
        ' capital', ' management', ' fund', ' funds', ' investments',
        ' advisors', ' advisory', ' securities', ' asset management',
        ' investment management', ' hedge fund'
    ]

    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Remove common prefixes
    prefixes = ['the ', 'a ', 'an ']
    for prefix in prefixes:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()

    return normalized

def create_person_key(name, company):
    """Create a unique key for person identification with defensive programming"""
    try:
        # Handle None or empty values
        if not name or not company:
            return None

        # Convert to string and handle various None representations
        name_str = str(name).strip() if name else ""
        company_str = str(company).strip() if company else ""

        # Check for empty or 'Unknown' values
        if (not name_str or not company_str or 
            name_str.lower() in ['unknown', 'none', 'null', ''] or
            company_str.lower() in ['unknown', 'none', 'null', '']):
            return None

        norm_name = normalize_name(name_str)
        norm_company = normalize_company(company_str)

        if not norm_name or not norm_company:
            return None

        key = f"{norm_name}|{norm_company}"
        return key

    except Exception as e:
        logger.error(f"Error creating person key for {name} at {company}: {e}")
        return None

def find_existing_person_strict(name, company):
    """Find existing person with STRICT duplicate checking"""
    try:
        person_key = create_person_key(name, company)

        if not person_key:
            return None

        # Check against all existing people
        for person in st.session_state.people:
            try:
                existing_name = safe_get(person, 'name')
                existing_company = safe_get(person, 'current_company_name')
                existing_key = create_person_key(existing_name, existing_company)

                if existing_key and existing_key == person_key:
                    return person

            except Exception as e:
                continue

        return None

    except Exception as e:
        logger.error(f"Error in duplicate check: {e}")
        return None

def check_for_duplicates_in_extraction(people_data):
    """Check for duplicates within the extraction data itself"""
    seen_keys = set()
    duplicates = []
    unique_people = []

    for person in people_data:
        person_key = create_person_key(
            safe_get(person, 'name'),
            person.get('current_company', person.get('company', ''))
        )

        if person_key:
            if person_key in seen_keys:
                duplicates.append(person)
                logger.warning(f"INTERNAL DUPLICATE: {safe_get(person, 'name')} at {person.get('current_company', person.get('company', ''))} appears multiple times in extraction")
            else:
                seen_keys.add(person_key)
                unique_people.append(person)
        else:
            # Invalid data
            duplicates.append(person)

    return unique_people, duplicates

# --- Test and Debug Functions ---
def test_duplicate_detection():
    """Test function to verify duplicate detection works correctly"""
    test_cases = [
        # Test case: [name1, company1, name2, company2, should_be_duplicate]
        ["John Smith", "Goldman Sachs", "john smith", "goldman sachs", True],
        ["John Smith", "Goldman Sachs Inc", "John Smith", "Goldman Sachs", True],
        ["John Smith", "Goldman Sachs", "John Smith", "J.P. Morgan", False],
        ["John Smith", "Goldman Sachs", "Jane Smith", "Goldman Sachs", False],
        ["Li Wei Chen", "Hillhouse Capital Management", "Li Wei Chen", "Hillhouse Capital", True],
        ["Dr. John Smith", "Goldman Sachs Ltd.", "John Smith", "Goldman Sachs", True],
        ["John Smith Jr.", "Goldman Sachs Corp", "John Smith Jr", "Goldman Sachs Corporation", True],
    ]

    results = []
    for name1, company1, name2, company2, expected in test_cases:
        key1 = create_person_key(name1, company1)
        key2 = create_person_key(name2, company2)
        actual = (key1 == key2) if key1 and key2 else False

        results.append({
            'test': f"{name1} @ {company1} vs {name2} @ {company2}",
            'expected': expected,
            'actual': actual,
            'passed': expected == actual,
            'key1': key1,
            'key2': key2
        })

    return results

def debug_person_keys():
    """Debug function to show all person keys in database"""
    keys = []
    for person in st.session_state.people:
        name = safe_get(person, 'name')
        company = safe_get(person, 'current_company_name')
        key = create_person_key(name, company)
        keys.append({
            'name': name,
            'company': company,
            'key': key,
            'id': person['id']
        })
    return keys

# --- Data Persistence with Google Drive ---
def debug_dataframe_structure(df):
    """Debug function to understand CSV structure"""
    try:
        log_essential(f"DataFrame shape: {df.shape}")
        log_essential(f"DataFrame columns: {list(df.columns)}")
        
        # Show first few rows
        log_essential("First 3 rows:")
        for i, row in df.head(3).iterrows():
            log_essential(f"Row {i}: {dict(row)}")
        
        # Check Type column values
        if 'Type' in df.columns:
            type_counts = df['Type'].value_counts()
            log_essential(f"Type counts: {dict(type_counts)}")
        
        # Check for any null/empty values in key columns
        key_columns = ['Type', 'Name', 'Company', 'Title']
        for col in key_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                empty_count = (df[col] == '').sum()
                log_essential(f"Column '{col}': {null_count} nulls, {empty_count} empty strings")
        
        return True
        
    except Exception as e:
        log_essential(f"Error debugging dataframe: {e}")
        return False

# Updated dataframe_to_people_and_firms function with better error handling
def dataframe_to_people_and_firms(df):
    """Convert DataFrame back to people, firms, and employment lists with employment history reconstruction"""
    try:
        if df is None or df.empty:
            log_essential("dataframe_to_people_and_firms: DataFrame is None or empty")
            return [], [], []
        
        # Debug the dataframe structure
        debug_dataframe_structure(df)
        
        people = []
        firms = []
        employments = []
        
        # Track person IDs for employment linking
        person_id_mapping = {}  # old_id -> new_id
        
        log_essential(f"Processing {len(df)} rows from CSV")
        
        for idx, row in df.iterrows():
            try:
                row_type = str(row.get('Type', '')).strip()
                
                if not row_type:
                    continue
                
                name = str(row.get('Name', '')).strip()
                company = str(row.get('Company', '')).strip()
                
                if not name or name.lower() in ['nan', 'none', '']:
                    continue
                
                if row_type.lower() == 'person':
                    # Create new person
                    new_person_id = str(uuid.uuid4())
                    old_person_id = str(row.get('Person_ID', ''))
                    
                    # Track ID mapping for employment linking
                    if old_person_id:
                        person_id_mapping[old_person_id] = new_person_id
                    
                    person = {
                        "id": new_person_id,
                        "name": name,
                        "current_title": str(row.get('Title', 'Unknown')).strip(),
                        "current_company_name": company if company else 'Unknown',
                        "location": str(row.get('Location', 'Unknown')).strip(),
                        "email": str(row.get('Email', '')).strip() if pd.notna(row.get('Email')) else "",
                        "phone": "",
                        "education": "",
                        "expertise": str(row.get('Expertise', 'Unknown')).strip(),
                        "aum_managed": str(row.get('AUM', '')).strip(),
                        "strategy": "",
                        "created_date": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "context_mentions": [],
                        "is_asia_based": str(row.get('Asia_Based', 'No')).strip().lower() == 'yes'
                    }
                    people.append(person)
                    
                elif row_type.lower() == 'employment':
                    # Process employment record
                    old_person_id = str(row.get('Person_ID', ''))
                    
                    if old_person_id in person_id_mapping:
                        new_person_id = person_id_mapping[old_person_id]
                        
                        # Parse dates
                        start_date = None
                        end_date = None
                        
                        start_date_str = str(row.get('Start_Date', '')).strip()
                        end_date_str = str(row.get('End_Date', '')).strip()
                        
                        if start_date_str and start_date_str != 'nan':
                            try:
                                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                            except:
                                pass
                        
                        if end_date_str and end_date_str != 'nan':
                            try:
                                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                            except:
                                pass
                        
                        employment = {
                            "id": str(uuid.uuid4()),
                            "person_id": new_person_id,
                            "company_name": company,
                            "title": str(row.get('Title', 'Unknown')).strip(),
                            "start_date": start_date,
                            "end_date": end_date,
                            "location": str(row.get('Location', 'Unknown')).strip(),
                            "strategy": str(row.get('Expertise', 'Unknown')).strip(),
                            "created_date": datetime.now().isoformat()
                        }
                        employments.append(employment)
                    
                elif row_type.lower() == 'firm':
                    firm = {
                        "id": str(uuid.uuid4()),
                        "name": name,
                        "firm_type": str(row.get('Expertise', 'Unknown')).strip(),
                        "location": str(row.get('Location', 'Unknown')).strip(),
                        "headquarters": str(row.get('Location', 'Unknown')).strip(),
                        "aum": str(row.get('AUM', 'Unknown')).strip(),
                        "founded": None,
                        "strategy": str(row.get('Title', 'Unknown')).strip(),
                        "website": str(row.get('Email', '')).strip() if pd.notna(row.get('Email')) else "",
                        "description": "",
                        "performance_metrics": [],
                        "created_date": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "context_mentions": [],
                        "is_asia_based": str(row.get('Asia_Based', 'No')).strip().lower() == 'yes'
                    }
                    firms.append(firm)
                    
            except Exception as row_error:
                log_essential(f"Error processing row {idx}: {row_error}")
                continue
        
        log_essential(f"Conversion complete: {len(people)} people, {len(firms)} firms, {len(employments)} employment records")
        return people, firms, employments
        
    except Exception as e:
        log_essential(f"Error in dataframe_to_people_and_firms: {e}")
        return [], [], []

def people_and_firms_to_dataframe(people, firms):
    """Convert people and firms lists to DataFrame for CSV storage - ENHANCED with employment data"""
    all_data = []

    # Export people with their current employment info
    for person in people:
        # Get employment history for this person
        person_employments = get_employments_by_person_id(person['id'])
        
        # Add current position as main record
        all_data.append({
            'Type': 'Person',
            'Person_ID': person['id'],  # Add person ID for linking
            'Name': safe_get(person, 'name'),
            'Title': safe_get(person, 'current_title'),
            'Company': safe_get(person, 'current_company_name'),
            'Location': safe_get(person, 'location'),
            'Email': safe_get(person, 'email'),
            'Expertise': safe_get(person, 'expertise'),
            'AUM': safe_get(person, 'aum_managed'),
            'Asia_Based': 'Yes' if person.get('is_asia_based', False) else 'No',
            'Is_Current': 'Yes',
            'Start_Date': '',
            'End_Date': '',
            'Employment_ID': ''
        })
        
        # Add historical employment records
        for emp in person_employments:
            start_date_str = emp['start_date'].strftime('%Y-%m-%d') if emp.get('start_date') else ''
            end_date_str = emp['end_date'].strftime('%Y-%m-%d') if emp.get('end_date') else ''
            
            all_data.append({
                'Type': 'Employment',
                'Person_ID': person['id'],
                'Name': safe_get(person, 'name'),
                'Title': safe_get(emp, 'title'),
                'Company': safe_get(emp, 'company_name'),
                'Location': safe_get(emp, 'location'),
                'Email': '',
                'Expertise': safe_get(emp, 'strategy'),
                'AUM': '',
                'Asia_Based': 'Yes' if person.get('is_asia_based', False) else 'No',
                'Is_Current': 'No',
                'Start_Date': start_date_str,
                'End_Date': end_date_str,
                'Employment_ID': emp['id']
            })

    # Export firms
    for firm in firms:
        all_data.append({
            'Type': 'Firm',
            'Person_ID': '',
            'Name': safe_get(firm, 'name'),
            'Title': safe_get(firm, 'strategy'),
            'Company': safe_get(firm, 'name'),
            'Location': safe_get(firm, 'location'),
            'Email': safe_get(firm, 'website'),
            'Expertise': safe_get(firm, 'firm_type'),
            'AUM': safe_get(firm, 'aum'),
            'Asia_Based': 'Yes' if firm.get('is_asia_based', False) else 'No',
            'Is_Current': '',
            'Start_Date': '',
            'End_Date': '',
            'Employment_ID': ''
        })

    return pd.DataFrame(all_data)

def save_data_to_drive():
    """Save all data to Google Drive as CSV"""
    try:
        if not drive_manager.service:
            return False

        # Convert data to DataFrame
        df = people_and_firms_to_dataframe(st.session_state.people, st.session_state.firms)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"hedge_fund_data_{timestamp}.csv"

        # Upload to Google Drive
        success = drive_manager.upload_csv(df, filename)

        if success:
            # Also save as "latest" version
            drive_manager.upload_csv(df, "hedge_fund_data_latest.csv")
            log_essential(f"Data saved to Google Drive: {filename}")
            return True

        return False

    except Exception as e:
        logger.error(f"Error saving data to Google Drive: {e}")
        return False

def load_data_from_drive():
    """Load data from Google Drive CSV with employment history reconstruction"""
    try:
        if not drive_manager.service:
            log_essential("load_data_from_drive: No Google Drive service")
            return [], [], []

        # Try to load the latest version first
        log_essential("Attempting to load hedge_fund_data_latest.csv")
        df = drive_manager.download_csv("hedge_fund_data_latest.csv")

        if df is None:
            log_essential("Latest version not found, looking for timestamped versions")
            files = drive_manager.list_files()
            hedge_fund_files = [f for f in files if f['name'].startswith('hedge_fund_data_') and f['name'].endswith('.csv')]

            if hedge_fund_files:
                hedge_fund_files.sort(key=lambda x: x.get('modifiedTime', ''), reverse=True)
                latest_file = hedge_fund_files[0]['name']
                log_essential(f"Trying to load most recent file: {latest_file}")
                df = drive_manager.download_csv(latest_file)

        if df is not None and not df.empty:
            log_essential(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
            
            # Convert dataframe to people, firms, AND employments
            people, firms, employments = dataframe_to_people_and_firms(df)
            
            log_essential(f"Data loaded from Google Drive: {len(people)} people, {len(firms)} firms, {len(employments)} employment records")
            return people, firms, employments
        else:
            log_essential("No valid dataframe loaded from Google Drive")
            return [], [], []

    except Exception as e:
        log_essential(f"Error loading data from Google Drive: {e}")
        return [], [], []

# --- Asia Detection and Tagging Functions ---
def is_asia_based(location_text):
    """Check if a location is Asia-based (not EMEA)"""
    if not location_text or location_text == 'Unknown':
        return False

    location_lower = location_text.lower()

    # Asia countries and major cities
    asia_keywords = [
        # Major financial centers
        'hong kong', 'singapore', 'tokyo', 'seoul', 'shanghai', 'beijing', 'mumbai', 'delhi',
        # Other major cities
        'shenzhen', 'guangzhou', 'taipei', 'kuala lumpur', 'jakarta', 'manila', 
        'ho chi minh', 'hanoi', 'macau', 'osaka', 'kyoto', 'busan', 'bangalore', 'chennai',
        'hyderabad', 'pune', 'kolkata', 'ahmedabad', 'bangkok', 'phuket', 'chiang mai',
        'cebu', 'davao', 'surabaya', 'bandung', 'medan', 'penang', 'johor bahru',
        # Countries
        'china', 'japan', 'korea', 'south korea', 'north korea', 'india', 'thailand', 
        'malaysia', 'indonesia', 'philippines', 'vietnam', 'taiwan', 'cambodia', 
        'laos', 'myanmar', 'brunei', 'sri lanka', 'bangladesh', 'nepal', 'bhutan',
        'maldives', 'mongolia', 'kazakhstan', 'uzbekistan', 'kyrgyzstan', 'tajikistan',
        'turkmenistan', 'afghanistan', 'pakistan',
        # Regions
        'asia', 'asian', 'asia pacific', 'apac', 'southeast asia', 'east asia', 
        'south asia', 'central asia', 'far east'
    ]

    return any(keyword in location_lower for keyword in asia_keywords)

def tag_profile_as_asia(profile, profile_type='person'):
    """Tag a profile as Asia-based"""
    if profile_type == 'person':
        location = safe_get(profile, 'location', '')
        current_company_location = safe_get(profile, 'current_company_name', '')

        # Check person's location or company mentions
        is_asia = is_asia_based(location) or is_asia_based(current_company_location)

        # Also check employment history for Asia connections
        if not is_asia:
            person_employments = get_employments_by_person_id(profile['id'])
            for emp in person_employments:
                if is_asia_based(safe_get(emp, 'location', '')):
                    is_asia = True
                    break
                if is_asia_based(safe_get(emp, 'company_name', '')):
                    is_asia = True
                    break

    elif profile_type == 'firm':
        location = safe_get(profile, 'location', '')
        headquarters = safe_get(profile, 'headquarters', '')
        name = safe_get(profile, 'name', '')

        is_asia = (is_asia_based(location) or 
                  is_asia_based(headquarters) or 
                  is_asia_based(name))

    else:
        is_asia = False

    profile['is_asia_based'] = is_asia
    return is_asia

def tag_all_existing_profiles():
    """Tag all existing profiles for Asia classification"""
    asia_people_count = 0
    asia_firms_count = 0

    # Tag people
    for person in st.session_state.people:
        if tag_profile_as_asia(person, 'person'):
            asia_people_count += 1

    # Tag firms
    for firm in st.session_state.firms:
        if tag_profile_as_asia(firm, 'firm'):
            asia_firms_count += 1

    return asia_people_count, asia_firms_count

def get_asia_people():
    """Get all Asia-based people"""
    return [p for p in st.session_state.people if p.get('is_asia_based', False)]

def get_asia_firms():
    """Get all Asia-based firms"""
    return [f for f in st.session_state.firms if f.get('is_asia_based', False)]

def get_non_asia_people():
    """Get all non-Asia people"""
    return [p for p in st.session_state.people if not p.get('is_asia_based', False)]

def get_non_asia_firms():
    """Get all non-Asia firms"""
    return [f for f in st.session_state.firms if not f.get('is_asia_based', False)]

# --- Context/News tracking functions ---
def add_context_to_person(person_id, context_type, content, source_info=""):
    """Add context/news mention to a person"""
    person = get_person_by_id(person_id)
    if not person:
        return False

    if 'context_mentions' not in person:
        person['context_mentions'] = []

    context_entry = {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'type': context_type,  # 'news', 'mention', 'movement', 'performance'
        'content': content,
        'source': source_info,
        'date_added': datetime.now().isoformat()
    }

    person['context_mentions'].append(context_entry)
    person['last_updated'] = datetime.now().isoformat()

    return True

def add_context_to_firm(firm_id, context_type, content, source_info=""):
    """Add context/news mention to a firm"""
    firm = get_firm_by_id(firm_id)
    if not firm:
        return False

    if 'context_mentions' not in firm:
        firm['context_mentions'] = []

    context_entry = {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'type': context_type,  # 'news', 'mention', 'movement', 'performance'
        'content': content,
        'source': source_info,
        'date_added': datetime.now().isoformat()
    }

    firm['context_mentions'].append(context_entry)
    firm['last_updated'] = datetime.now().isoformat()

    return True

# --- JSON repair function ---
def repair_json_response(json_text):
    """Repair common JSON formatting issues from responses"""
    try:
        # Remove any text before the first {
        json_start = json_text.find('{')
        if json_start > 0:
            json_text = json_text[json_start:]

        # Remove any text after the last }
        json_end = json_text.rfind('}')
        if json_end > 0:
            json_text = json_text[:json_end + 1]

        # Fix common JSON issues
        json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)
        json_text = re.sub(r'}\s*{', r'},{', json_text)
        json_text = re.sub(r']\s*\[', r'],[', json_text)
        json_text = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_text)

        return json_text

    except Exception as e:
        logger.warning(f"JSON repair failed: {e}")
        return json_text

# --- Date overlap calculation ---
def calculate_date_overlap(start1, end1, start2, end2):
    """Calculate overlap between two date periods"""
    try:
        # Handle None end dates (current positions)
        if end1 is None:
            end1 = date.today()
        if end2 is None:
            end2 = date.today()

        # Find overlap
        overlap_start = max(start1, start2)
        overlap_end = min(end1, end2)

        # Check if there's actual overlap
        if overlap_start <= overlap_end:
            overlap_days = (overlap_end - overlap_start).days + 1
            return overlap_start, overlap_end, overlap_days
        else:
            return None

    except Exception as e:
        logger.warning(f"Error calculating date overlap: {e}")
        return None

# --- File Loading ---
def load_file_content_enhanced(uploaded_file):
    """Enhanced file loading with robust encoding detection"""
    try:
        file_size = len(uploaded_file.getvalue())
        file_size_mb = file_size / (1024 * 1024)

        logger.info(f"Loading file: {uploaded_file.name} ({file_size_mb:.1f} MB)")

        if uploaded_file.type == "text/plain" or uploaded_file.name.endswith('.txt'):
            raw_data = uploaded_file.getvalue()

            # Try common encodings
            encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1252', 'latin1', 'iso-8859-1']

            content = None
            encoding_used = None

            for encoding in encodings_to_try:
                try:
                    content = raw_data.decode(encoding)
                    encoding_used = encoding
                    logger.info(f"Successfully decoded file with {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if content is None:
                try:
                    content = raw_data.decode('utf-8', errors='replace')
                    encoding_used = 'utf-8 (with replacements)'
                except Exception as e:
                    return False, "", f"Could not decode file: {str(e)}", None

            if not content or len(content.strip()) == 0:
                return False, "", "File appears to be empty", encoding_used

            return True, content, "", encoding_used

        else:
            try:
                raw_data = uploaded_file.getvalue()
                content = raw_data.decode('utf-8', errors='replace')
                return True, content, f"Warning: Unknown file type '{uploaded_file.type}'", "utf-8 (permissive)"
            except Exception as e:
                return False, "", f"Unsupported file type: {uploaded_file.type}", None

    except Exception as e:
        return False, "", f"Error reading file: {str(e)}", None

# --- Search function ---
def enhanced_global_search(query):
    """Enhanced global search function"""
    try:
        query_lower = query.lower().strip()

        if len(query_lower) < 2:
            return [], [], []

        matching_people = []
        matching_firms = []
        matching_metrics = []

        # Search people
        for person in st.session_state.people:
            try:
                searchable_fields = [
                    safe_get(person, 'name', ''),
                    safe_get(person, 'current_title', ''),
                    safe_get(person, 'current_company_name', ''),
                    safe_get(person, 'location', ''),
                    safe_get(person, 'expertise', ''),
                    safe_get(person, 'education', ''),
                    safe_get(person, 'email', '')
                ]

                searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()

                if query_lower in searchable_text:
                    matching_people.append(person)
            except Exception as e:
                logger.warning(f"Error searching person: {e}")
                continue

        # Search firms
        for firm in st.session_state.firms:
            try:
                searchable_fields = [
                    safe_get(firm, 'name', ''),
                    safe_get(firm, 'location', ''),
                    safe_get(firm, 'strategy', ''),
                    safe_get(firm, 'description', ''),
                    safe_get(firm, 'firm_type', '')
                ]

                searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()

                if query_lower in searchable_text:
                    matching_firms.append(firm)
            except Exception as e:
                logger.warning(f"Error searching firm: {e}")
                continue

        return matching_people, matching_firms, matching_metrics

    except Exception as e:
        logger.error(f"Error in enhanced_global_search: {e}")
        return [], [], []

# --- Initialize Session State with Sample Data ---
def init_sample_data():
    """Initialize with sample data if no saved data exists"""

    sample_people = [
        {
            "id": str(uuid.uuid4()),
            "name": "Li Wei Chen",
            "current_title": "Portfolio Manager",
            "current_company_name": "Hillhouse Capital",
            "location": "Hong Kong",
            "email": "li.chen@hillhouse.com",
            "phone": "+852-1234-5678",
            "education": "Harvard Business School, Tsinghua University",
            "expertise": "Technology, Healthcare",
            "aum_managed": "2.5B USD",
            "strategy": "Long-only Growth Equity",
            "created_date": (datetime.now() - timedelta(days=30)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=5)).isoformat(),
            "context_mentions": []
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Akira Tanaka",
            "current_title": "Chief Investment Officer",
            "current_company_name": "Millennium Partners Asia",
            "location": "Singapore",
            "email": "a.tanaka@millennium.com",
            "phone": "+65-9876-5432",
            "education": "Tokyo University, Wharton",
            "expertise": "Quantitative Trading, Fixed Income",
            "aum_managed": "1.8B USD",
            "strategy": "Multi-Strategy Quantitative",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=2)).isoformat(),
            "context_mentions": []
        }
    ]

    sample_firms = [
        {
            "id": str(uuid.uuid4()),
            "name": "Hillhouse Capital",
            "firm_type": "Asset Manager",
            "location": "Hong Kong",
            "headquarters": "Beijing, China",
            "aum": "60B USD",
            "founded": 2005,
            "strategy": "Long-only, Growth Equity",
            "website": "https://hillhousecap.com",
            "description": "Asia's largest asset manager focusing on technology and healthcare investments",
            "created_date": (datetime.now() - timedelta(days=45)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=10)).isoformat(),
            "context_mentions": [],
            "performance_metrics": []
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Millennium Partners Asia",
            "firm_type": "Hedge Fund",
            "location": "Singapore",
            "headquarters": "New York, USA",
            "aum": "35B USD",
            "founded": 1989,
            "strategy": "Multi-strategy, Quantitative",
            "website": "https://millennium.com",
            "description": "Global hedge fund with significant Asian operations",
            "created_date": (datetime.now() - timedelta(days=20)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=3)).isoformat(),
            "context_mentions": [],
            "performance_metrics": []
        }
    ]

    return sample_people, sample_firms, []

def initialize_session_state():
    """Initialize session state with saved or sample data"""
    # Try to load from Google Drive first
    people, firms, employments = load_data_from_drive()

    # If no saved data, use sample data
    if not people and not firms:
        people, firms, employments = init_sample_data()

    if 'people' not in st.session_state:
        st.session_state.people = people
    if 'firms' not in st.session_state:
        st.session_state.firms = firms
    if 'employments' not in st.session_state:
        st.session_state.employments = employments
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'people'
    if 'selected_person_id' not in st.session_state:
        st.session_state.selected_person_id = None
    if 'selected_firm_id' not in st.session_state:
        st.session_state.selected_firm_id = None
    if 'show_add_person_modal' not in st.session_state:
        st.session_state.show_add_person_modal = False
    if 'show_add_firm_modal' not in st.session_state:
        st.session_state.show_add_firm_modal = False
    if 'show_edit_person_modal' not in st.session_state:
        st.session_state.show_edit_person_modal = False
    if 'show_edit_firm_modal' not in st.session_state:
        st.session_state.show_edit_firm_modal = False
    if 'show_add_employment_modal' not in st.session_state:
        st.session_state.show_add_employment_modal = False
    if 'edit_person_data' not in st.session_state:
        st.session_state.edit_person_data = None
    if 'edit_firm_data' not in st.session_state:
        st.session_state.edit_firm_data = None
    if 'global_search' not in st.session_state:
        st.session_state.global_search = ""

    # Background processing for extraction
    if 'background_processing' not in st.session_state:
        st.session_state.background_processing = {
            'is_running': False,
            'results': {'people': [], 'performance': []},
            'status_message': '',
            'progress': 0
        }

# --- Gemini API Setup with Paid Tier Limits ---
@st.cache_resource
def setup_gemini(api_key):
    """Setup Gemini with optimized settings for paid tier"""
    if not GENAI_AVAILABLE:
        return None
    try:
        genai.configure(api_key=api_key)
        # Use Flash model for optimal speed/cost ratio on paid tier
        model = genai.GenerativeModel("gemini-1.5-flash")
        model.model_id = "gemini-1.5-flash"
        return model
    except Exception as e:
        logger.error(f"Gemini setup failed: {e}")
        return None

def extract_data_from_text(text, model):
    """Extract people and performance data from text using Gemini"""
    try:
        log_extraction_progress("GEMINI_REQUEST", f"Sending to {model.model_id}")

        # Paid tier optimized prompt
        prompt = f"""
Extract financial professionals and performance data from this text.

TEXT: {text}

Return ONLY valid JSON with this structure:
{{
  "people": [
    {{
      "name": "Full Name",
      "current_company": "Company Name",
      "current_title": "Job Title",
      "location": "City, Country",
      "expertise": "Area of expertise",
      "movement_type": "hire|promotion|departure|appointment"
    }}
  ],
  "performance": [
    {{
      "fund_name": "Fund/Firm Name",
      "metric_type": "return|sharpe|aum|alpha|beta",
      "value": "numeric_value_only",
      "period": "YTD|Q1|Q2|Q3|Q4|1Y|3Y|5Y",
      "date": "YYYY"
    }}
  ]
}}
"""

        # Optimized generation config for paid tier
        generation_config = {
            'temperature': 0.1,
            'top_p': 0.8,
            'max_output_tokens': 4096,
        }

        response = model.generate_content(prompt, generation_config=generation_config)

        if not response or not response.text:
            return [], []

        response_text = response.text.strip()

        # Extract JSON
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1

        if json_start == -1 or json_end <= json_start:
            return [], []

        json_text = response_text[json_start:json_end]

        # Try parsing, with repair if needed
        try:
            result = json.loads(json_text)
        except json.JSONDecodeError:
            repaired_json = repair_json_response(json_text)
            try:
                result = json.loads(repaired_json)
            except json.JSONDecodeError:
                return [], []

        people = result.get('people', [])
        performance = result.get('performance', [])

        # Validate extracted data
        valid_people = []
        for p in people:
            name = safe_get(p, 'name', '').strip()
            company = safe_get(p, 'current_company', '').strip()

            if (name and company and 
                len(name) > 2 and len(company) > 2 and
                name.lower() not in ['name', 'full name', 'unknown'] and
                company.lower() not in ['company', 'company name', 'unknown']):
                valid_people.append(p)

        valid_performance = []
        for perf in performance:
            fund_name = safe_get(perf, 'fund_name', '').strip()
            metric_type = safe_get(perf, 'metric_type', '').strip()
            value = safe_get(perf, 'value', '').strip()

            if (fund_name and metric_type and value and
                len(fund_name) > 2 and
                fund_name.lower() not in ['fund name', 'unknown'] and
                metric_type.lower() not in ['metric type', 'unknown']):
                valid_performance.append(perf)

        log_extraction_progress("EXTRACTION_COMPLETE", f"Found {len(valid_people)} people, {len(valid_performance)} metrics")
        return valid_people, valid_performance

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return [], []

def process_extraction_with_rate_limiting(text, model):
    """Process extraction with automatic rate limiting for paid tier"""
    start_time = time.time()

    try:
        text_length = len(text)
        log_extraction_step("PROCESS_START", f"Starting extraction process with {text_length} chars")

        # Split into chunks if text is too long (paid tier can handle larger chunks)
        max_chunk_size = 100000  # 100K chars for paid tier
        chunks = []

        if len(text) <= max_chunk_size:
            chunks = [text]
            log_extraction_step("CHUNKING", f"Single chunk: {len(text)} chars")
        else:
            current_pos = 0
            chunk_count = 0
            while current_pos < len(text):
                end_pos = min(current_pos + max_chunk_size, len(text))

                # Find paragraph break for better chunking
                if end_pos < len(text):
                    break_pos = text.rfind('\n\n', current_pos, end_pos)
                    if break_pos > current_pos:
                        end_pos = break_pos + 2

                chunk = text[current_pos:end_pos].strip()
                if len(chunk) > 500:  # Minimum chunk size
                    chunks.append(chunk)
                    chunk_count += 1
                    log_extraction_step("CHUNK_CREATED", f"Chunk {chunk_count}: {len(chunk)} chars (pos: {current_pos}-{end_pos})")
                current_pos = end_pos

            log_extraction_step("CHUNKING_COMPLETE", f"Created {len(chunks)} chunks from {text_length} chars")

        all_people = []
        all_performance = []
        failed_chunks = []

        # Process chunks with paid tier rate limiting (2000 RPM = ~33 per second)
        delay_between_requests = 0.03  # 30ms delay for paid tier
        log_extraction_step("RATE_LIMITING", f"Using {delay_between_requests}s delay between requests (2000 RPM)")

        for i, chunk in enumerate(chunks):
            chunk_start_time = time.time()

            try:
                st.session_state.background_processing.update({
                    'progress': int((i / len(chunks)) * 100),
                    'status_message': f'Processing chunk {i+1}/{len(chunks)}...'
                })

                log_extraction_step("CHUNK_PROCESS_START", f"Processing chunk {i+1}/{len(chunks)} ({len(chunk)} chars)")

                people, performance = extract_data_from_text(chunk, model)

                chunk_duration = time.time() - chunk_start_time
                log_extraction_step("CHUNK_PROCESS_COMPLETE", 
                    f"Chunk {i+1}/{len(chunks)} complete: {len(people)} people, {len(performance)} metrics (duration: {chunk_duration:.2f}s)")

                all_people.extend(people)
                all_performance.extend(performance)

                # Rate limiting delay (except for last chunk)
                if i < len(chunks) - 1:
                    log_extraction_step("RATE_LIMIT_DELAY", f"Applying {delay_between_requests}s rate limit delay")
                    time.sleep(delay_between_requests)

            except Exception as e:
                chunk_duration = time.time() - chunk_start_time
                failed_chunks.append(i+1)
                log_extraction_step("CHUNK_PROCESS_ERROR", 
                    f"Chunk {i+1}/{len(chunks)} failed: {e} (duration: {chunk_duration:.2f}s)", "ERROR")
                continue

        total_duration = time.time() - start_time
        log_extraction_step("PROCESS_COMPLETE", 
            f"Extraction process complete: {len(all_people)} people, {len(all_performance)} metrics from {len(chunks)} chunks, {len(failed_chunks)} failed (total duration: {total_duration:.2f}s)")

        if failed_chunks:
            log_extraction_step("FAILED_CHUNKS", f"Failed chunks: {failed_chunks}", "WARNING")

        return all_people, all_performance

    except Exception as e:
        total_duration = time.time() - start_time
        log_extraction_step("PROCESS_ERROR", f"Processing failed: {e} (duration: {total_duration:.2f}s)", "ERROR")
        return [], []

# --- Helper Functions ---
def get_person_by_id(person_id):
    return next((p for p in st.session_state.people if p['id'] == person_id), None)

def get_firm_by_id(firm_id):
    return next((f for f in st.session_state.firms if f['id'] == firm_id), None)

def get_firm_by_name(firm_name):
    return next((f for f in st.session_state.firms if f['name'] == firm_name), None)

def get_people_by_firm(firm_name):
    return [p for p in st.session_state.people if safe_get(p, 'current_company_name') == firm_name]

def get_employments_by_person_id(person_id):
    return [e for e in st.session_state.employments if e['person_id'] == person_id]

def get_shared_work_history(person_id):
    """Get people who have overlapping work periods at the same companies - ENHANCED with debugging"""
    try:
        log_essential(f"Getting shared work history for person: {person_id}")
        
        person_employments = get_employments_by_person_id(person_id)
        log_essential(f"Found {len(person_employments)} employment records for this person")
        
        if not person_employments:
            log_essential("No employment records found, checking current company...")
            # If no employment history, try to use current company info
            person = get_person_by_id(person_id)
            if person:
                current_company = safe_get(person, 'current_company_name')
                log_essential(f"Person's current company: {current_company}")
                
                # Look for others at the same current company
                colleagues = []
                for other_person in st.session_state.people:
                    if other_person['id'] != person_id:
                        other_current_company = safe_get(other_person, 'current_company_name')
                        if other_current_company == current_company and current_company != 'Unknown':
                            colleagues.append({
                                "colleague_name": safe_get(other_person, 'name'),
                                "colleague_id": other_person['id'],
                                "shared_company": current_company,
                                "colleague_current_company": other_current_company,
                                "colleague_current_title": safe_get(other_person, 'current_title'),
                                "overlap_days": 0,
                                "overlap_duration": "Current colleagues",
                                "overlap_period": "Present",
                                "connection_strength": "Current"
                            })
                
                log_essential(f"Found {len(colleagues)} current colleagues")
                return colleagues
            
            return []
        
        shared_history = []
        
        # Get all companies this person has worked at with their periods
        person_company_periods = []
        for emp in person_employments:
            if emp.get('start_date'):
                person_company_periods.append({
                    'company': emp['company_name'],
                    'start_date': emp['start_date'],
                    'end_date': emp.get('end_date'),
                    'title': emp['title'],
                    'location': emp.get('location', 'Unknown')
                })

        log_essential(f"Person worked at {len(person_company_periods)} companies with dates")

        # Find overlapping colleagues
        for other_person in st.session_state.people:
            if other_person['id'] == person_id:
                continue

            other_employments = get_employments_by_person_id(other_person['id'])
            other_company_periods = []

            for emp in other_employments:
                if emp.get('start_date'):
                    other_company_periods.append({
                        'company': emp['company_name'],
                        'start_date': emp['start_date'],
                        'end_date': emp.get('end_date'),
                        'title': emp['title'],
                        'location': emp.get('location', 'Unknown')
                    })

            # Check for overlapping periods at same companies
            for person_period in person_company_periods:
                for other_period in other_company_periods:
                    if person_period['company'] == other_period['company']:
                        # Calculate overlap
                        overlap = calculate_date_overlap(
                            person_period['start_date'], person_period['end_date'],
                            other_period['start_date'], other_period['end_date']
                        )

                        if overlap:
                            overlap_start, overlap_end, overlap_days = overlap

                            # Calculate overlap duration
                            if overlap_days >= 365:
                                duration_str = f"{overlap_days // 365} year(s)"
                            elif overlap_days >= 30:
                                duration_str = f"{overlap_days // 30} month(s)"
                            else:
                                duration_str = f"{overlap_days} day(s)"

                            overlap_period = f"{overlap_start.strftime('%b %Y')} - {overlap_end.strftime('%b %Y')}"

                            shared_history.append({
                                "colleague_name": safe_get(other_person, 'name'),
                                "colleague_id": other_person['id'],
                                "shared_company": person_period['company'],
                                "colleague_current_company": safe_get(other_person, 'current_company_name'),
                                "colleague_current_title": safe_get(other_person, 'current_title'),
                                "overlap_days": overlap_days,
                                "overlap_duration": duration_str,
                                "overlap_period": overlap_period,
                                "connection_strength": "Strong" if overlap_days >= 365 else "Medium" if overlap_days >= 90 else "Brief"
                            })

        # Remove duplicates and sort by connection strength
        unique_shared = {}
        for item in shared_history:
            key = f"{item['colleague_id']}_{item['shared_company']}"
            if key not in unique_shared:
                unique_shared[key] = item
            else:
                existing = unique_shared[key]
                if item['overlap_days'] > existing['overlap_days']:
                    unique_shared[key] = item

        # Sort by connection strength
        def sort_key(conn):
            strength_order = {"Strong": 0, "Medium": 1, "Brief": 2, "Current": 0}
            return (
                strength_order.get(conn.get('connection_strength', 'Brief'), 2),
                -conn.get('overlap_days', 0),
                conn['colleague_name']
            )

        result = sorted(list(unique_shared.values()), key=sort_key)
        log_essential(f"Found {len(result)} shared work connections")
        return result
        
    except Exception as e:
        log_essential(f"Error in get_shared_work_history: {e}")
        return []

def add_employment_with_dates(person_id, company_name, title, start_date, end_date=None, location="Unknown", strategy="Unknown"):
    """Add employment record with proper date validation"""
    try:
        if end_date and start_date and end_date <= start_date:
            raise ValueError("End date must be after start date")

        new_employment = {
            "id": str(uuid.uuid4()),
            "person_id": person_id,
            "company_name": company_name,
            "title": title,
            "start_date": start_date,
            "end_date": end_date,
            "location": location,
            "strategy": strategy,
            "created_date": datetime.now().isoformat()
        }

        st.session_state.employments.append(new_employment)
        save_data_to_drive()  # Save to Google Drive
        return True

    except Exception as e:
        logger.error(f"Error adding employment: {e}")
        return False

# --- Navigation Functions with Logging ---
def go_to_firms():
    log_user_action("NAVIGATION", "Switched to Firms view")
    st.session_state.current_view = 'firms'
    st.session_state.selected_firm_id = None

def go_to_people():
    log_user_action("NAVIGATION", "Switched to People view")
    st.session_state.current_view = 'people'
    st.session_state.selected_person_id = None

def go_to_person_details(person_id):
    person = get_person_by_id(person_id)
    person_name = safe_get(person, 'name', 'Unknown') if person else 'Unknown'
    log_user_action("NAVIGATION", f"Viewing person details: {person_name} (ID: {person_id})")
    st.session_state.selected_person_id = person_id
    st.session_state.current_view = 'person_details'

def go_to_firm_details(firm_id):
    firm = get_firm_by_id(firm_id)
    firm_name = safe_get(firm, 'name', 'Unknown') if firm else 'Unknown'
    log_user_action("NAVIGATION", f"Viewing firm details: {firm_name} (ID: {firm_id})")
    st.session_state.selected_firm_id = firm_id
    st.session_state.current_view = 'firm_details'

# --- Export Functions with Logging ---
def export_to_csv():
    """Export all data to CSV"""
    start_time = time.time()

    try:
        people_count = len(st.session_state.people)
        firms_count = len(st.session_state.firms)
        log_user_action("EXPORT_START", f"Starting CSV export: {people_count} people, {firms_count} firms")

        df = people_and_firms_to_dataframe(st.session_state.people, st.session_state.firms)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"hedge_fund_data_{timestamp}.csv"

        duration = time.time() - start_time
        log_user_action("EXPORT_SUCCESS", f"CSV export complete: {len(df)} records, file: {filename} (duration: {duration:.2f}s)")

        return df.to_csv(index=False), filename

    except Exception as e:
        duration = time.time() - start_time
        log_user_action("EXPORT_ERROR", f"CSV export failed: {e} (duration: {duration:.2f}s)")
        return None, None

def export_asia_csv():
    """Export Asia-specific data to CSV"""
    start_time = time.time()

    try:
        asia_people = get_asia_people()
        asia_firms = get_asia_firms()

        log_user_action("ASIA_EXPORT_START", f"Starting Asia CSV export: {len(asia_people)} people, {len(asia_firms)} firms")

        asia_data = []

        # Export Asia people
        for person in asia_people:
            asia_data.append({
                'Type': 'Person',
                'Name': safe_get(person, 'name'),
                'Title': safe_get(person, 'current_title'),
                'Company': safe_get(person, 'current_company_name'),
                'Location': safe_get(person, 'location'),
                'Email': safe_get(person, 'email'),
                'Expertise': safe_get(person, 'expertise'),
                'AUM': safe_get(person, 'aum_managed'),
                'Region': 'Asia'
            })

        # Export Asia firms
        for firm in asia_firms:
            asia_data.append({
                'Type': 'Firm',
                'Name': safe_get(firm, 'name'),
                'Title': safe_get(firm, 'strategy'),
                'Company': safe_get(firm, 'name'),
                'Location': safe_get(firm, 'location'),
                'Email': safe_get(firm, 'website'),
                'Expertise': safe_get(firm, 'firm_type'),
                'AUM': safe_get(firm, 'aum'),
                'Region': 'Asia'
            })

        if not asia_data:
            log_user_action("ASIA_EXPORT_EMPTY", "No Asia-based data found for export")
            return None, None

        df = pd.DataFrame(asia_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"asia_hedge_fund_data_{timestamp}.csv"

        duration = time.time() - start_time
        log_user_action("ASIA_EXPORT_SUCCESS", f"Asia CSV export complete: {len(asia_data)} records, file: {filename} (duration: {duration:.2f}s)")

        return df.to_csv(index=False), filename

    except Exception as e:
        duration = time.time() - start_time
        log_user_action("ASIA_EXPORT_ERROR", f"Asia CSV export failed: {e} (duration: {duration:.2f}s)")
        return None, None

# Initialize session state
try:
    log_user_action("APP_INIT_START", "Initializing application")
    initialize_session_state()

    # Tag all existing profiles for Asia classification
    if 'asia_tagged' not in st.session_state:
        log_user_action("ASIA_TAGGING_START", "Starting Asia classification for existing profiles")
        asia_people_count, asia_firms_count = tag_all_existing_profiles()
        save_data_to_drive()  # Save the Asia tags to Google Drive
        st.session_state.asia_tagged = True
        log_user_action("ASIA_TAGGING_COMPLETE", f"Tagged {asia_people_count} Asia people and {asia_firms_count} Asia firms")

    log_user_action("APP_INIT_COMPLETE", f"Application initialized successfully with {len(st.session_state.people)} people, {len(st.session_state.firms)} firms")

except Exception as init_error:
    log_user_action("APP_INIT_ERROR", f"Initialization failed: {init_error}")
    st.error(f"Initialization error: {init_error}")
    st.stop()

# --- Google Drive Authentication Setup ---
if GDRIVE_AVAILABLE:
    # Try to get credentials from secrets
    credentials_dict = None
    try:
        credentials_dict = st.secrets.get("google_service_account")
        if credentials_dict:
            if drive_manager.authenticate(credentials_dict):
                if drive_manager.find_or_create_folder():
                    st.sidebar.success("🔗 Google Drive Connected")
                else:
                    st.sidebar.error("❌ Failed to setup Drive folder")
            else:
                st.sidebar.error("❌ Drive authentication failed")
        else:
            st.sidebar.warning("⚠️ Google Drive credentials not found in secrets")
            st.sidebar.info("Add your service account credentials to .streamlit/secrets.toml")
    except Exception as e:
        st.sidebar.error(f"❌ Drive setup error: {e}")

# --- HEADER ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("Asian Hedge Fund Talent Network")
    st.markdown("**Professional intelligence platform for Asia's financial industry**")

    # Show Asia-specific statistics
    asia_people_count = len(get_asia_people())
    asia_firms_count = len(get_asia_firms())
    total_people = len(st.session_state.people)
    total_firms = len(st.session_state.firms)

    if asia_people_count > 0 or asia_firms_count > 0:
        st.caption(f"🌏 Asia Focus: {asia_people_count}/{total_people} people • {asia_firms_count}/{total_firms} firms")

with col2:
    # CSV Export
    csv_data, filename = export_to_csv()
    if csv_data:
        st.download_button(
            "Export CSV",
            csv_data,
            filename,
            "text/csv",
            use_container_width=True
        )

# --- SIDEBAR: Data Extraction ---
with st.sidebar:
    st.title("Data Extraction")

    # Google Drive Status
    if drive_manager.service:
        st.success("✅ Google Drive Connected")
    else:
        st.error("❌ Google Drive Not Connected")
        st.info("Setup required - see setup guide")

    # API Key Setup
    api_key = None
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if api_key:
            st.success("🔑 API key loaded")
    except:
        pass

    if not api_key:
        api_key = st.text_input("Gemini API Key", type="password")

    # Setup model
    model = None
    if api_key and GENAI_AVAILABLE:
        model = setup_gemini(api_key)

        st.markdown("---")
        st.subheader("Extract from Content")

        input_method = st.radio("Input method:", ["Text", "File"])

        newsletter_text = ""
        if input_method == "Text":
            newsletter_text = st.text_area("Content:", height=150, 
                                         placeholder="Paste content here...")
        else:
            uploaded_file = st.file_uploader("Upload file:", type=['txt'])
            if uploaded_file:
                try:
                    success, content, error_msg, encoding_used = load_file_content_enhanced(uploaded_file)

                    if success:
                        newsletter_text = content
                        st.success(f"File loaded ({len(newsletter_text):,} characters)")

                        if error_msg:
                            st.warning(error_msg)
                    else:
                        st.error(error_msg)

                except Exception as file_error:
                    st.error(f"Error loading file: {str(file_error)}")

        # Extract button
        if st.button("Start Extraction", use_container_width=True):
            if not newsletter_text.strip():
                log_user_action("EXTRACTION_ERROR", "Attempted extraction with empty content")
                st.error("Please provide content")
            elif not model:
                log_user_action("EXTRACTION_ERROR", "Attempted extraction without API key")
                st.error("Please provide API key")
            else:
                # Start background processing
                log_user_action("EXTRACTION_START", f"Starting extraction with {len(newsletter_text)} characters using model {model.model_id}")

                st.session_state.background_processing = {
                    'is_running': True,
                    'progress': 0,
                    'status_message': 'Starting extraction...',
                    'results': {'people': [], 'performance': []}
                }

                with st.spinner("Extracting data..."):
                    try:
                        people, performance = process_extraction_with_rate_limiting(newsletter_text, model)

                        st.session_state.background_processing = {
                            'is_running': False,
                            'progress': 100,
                            'status_message': f'Complete: {len(people)} people, {len(performance)} metrics',
                            'results': {'people': people, 'performance': performance}
                        }

                        log_user_action("EXTRACTION_SUCCESS", f"Extraction complete: {len(people)} people, {len(performance)} metrics found")
                        st.success(f"Extraction complete! Found {len(people)} people and {len(performance)} metrics")

                    except Exception as e:
                        log_user_action("EXTRACTION_ERROR", f"Extraction failed: {e}")
                        st.error(f"Extraction failed: {e}")
                        st.session_state.background_processing['is_running'] = False

    elif not GENAI_AVAILABLE:
        st.error("Please install: pip install google-generativeai")

    # Show extraction results for review
    if st.session_state.background_processing.get('results', {}).get('people') or st.session_state.background_processing.get('results', {}).get('performance'):
        st.markdown("---")
        st.subheader("Review Results")

        results = st.session_state.background_processing['results']
        people_results = results.get('people', [])
        performance_results = results.get('performance', [])

        if people_results:
            st.markdown(f"**People ({len(people_results)})**")

            # Show first 3 people for review
            for i, person in enumerate(people_results[:3]):
                name = safe_get(person, 'name', '').strip()
                company = person.get('current_company', person.get('company', '')).strip()

                # Real-time duplicate check
                existing_person = find_existing_person_strict(name, company)

                with st.container(border=True):
                    if existing_person:
                        st.error(f"🚫 **DUPLICATE**: {name}")
                        st.caption(f"❌ Already exists at {company}")
                    else:
                        st.success(f"✅ **NEW**: {name}")
                        st.caption(f"✓ {safe_get(person, 'current_title')} at {company}")

            if len(people_results) > 3:
                st.info(f"Showing 3 of {len(people_results)} people...")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Save All", use_container_width=True):
                # Process all people with strict duplicate checking
                saved_count = 0
                blocked_count = 0

                for person_data in people_results:
                    name = safe_get(person_data, 'name', '').strip()
                    company_name = person_data.get('current_company', person_data.get('company', '')).strip()

                    # Skip invalid entries
                    if not name or not company_name:
                        continue

                    # STRICT duplicate check
                    existing_person = find_existing_person_strict(name, company_name)
                    if existing_person:
                        blocked_count += 1
                        continue

                    # Create new person
                    new_person_id = str(uuid.uuid4())
                    new_person = {
                        "id": new_person_id,
                        "name": name,
                        "current_title": safe_get(person_data, 'current_title', 'Unknown'),
                        "current_company_name": company_name,
                        "location": safe_get(person_data, 'location', 'Unknown'),
                        "email": "",
                        "phone": "",
                        "education": "",
                        "expertise": safe_get(person_data, 'expertise', 'Unknown'),
                        "aum_managed": "",
                        "strategy": "",
                        "created_date": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "context_mentions": []
                    }

                    st.session_state.people.append(new_person)
                    tag_profile_as_asia(new_person, 'person')

                    # Add firm if doesn't exist
                    if not get_firm_by_name(company_name):
                        new_firm = {
                            "id": str(uuid.uuid4()),
                            "name": company_name,
                            "firm_type": "Unknown",
                            "location": safe_get(person_data, 'location', 'Unknown'),
                            "headquarters": "Unknown",
                            "aum": "Unknown",
                            "founded": None,
                            "strategy": "Unknown",
                            "website": "",
                            "description": "",
                            "performance_metrics": [],
                            "created_date": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                            "context_mentions": []
                        }
                        st.session_state.firms.append(new_firm)
                        tag_profile_as_asia(new_firm, 'firm')

                    saved_count += 1

                # Save to Google Drive
                save_data_to_drive()

                if saved_count > 0:
                    st.success(f"✅ Saved {saved_count} new people!")
                if blocked_count > 0:
                    st.warning(f"🚫 Blocked {blocked_count} duplicates")

                # Clear results
                st.session_state.background_processing['results'] = {'people': [], 'performance': []}
                st.rerun()

        with col2:
            if st.button("Discard", use_container_width=True):
                st.session_state.background_processing['results'] = {'people': [], 'performance': []}
                st.rerun()

# --- MAIN CONTENT AREA ---

# Global Search Bar
col1, col2 = st.columns([4, 1])

with col1:
    search_query = st.text_input(
        "Search people, firms...", 
        value=st.session_state.global_search,
        placeholder="Search by name, company, title...",
        key="main_search_input"
    )

with col2:
    if st.button("Search", use_container_width=True) or search_query != st.session_state.global_search:
        st.session_state.global_search = search_query
        if search_query and len(search_query.strip()) >= 2:
            log_user_action("SEARCH", f"User searched for: '{search_query}'")
            st.rerun()

# Handle global search results
if st.session_state.global_search and len(st.session_state.global_search.strip()) >= 2:
    search_query = st.session_state.global_search
    matching_people, matching_firms, matching_metrics = enhanced_global_search(search_query)

    if matching_people or matching_firms:
        st.markdown("### Search Results")

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.metric("People", len(matching_people))
        with col2:
            st.metric("Firms", len(matching_firms))
        with col3:
            if st.button("Clear Search"):
                st.session_state.global_search = ""
                st.rerun()

        # Show search results
        if matching_people:
            st.markdown("**People Found**")
            cols = st.columns(3)
            for i, person in enumerate(matching_people):
                with cols[i % 3]:
                    with st.container(border=True):
                        st.markdown(f"**{safe_get(person, 'name')}**")
                        st.caption(f"{safe_get(person, 'current_title')}")
                        st.caption(f"{safe_get(person, 'current_company_name')}")
                        st.caption(f"📍 {safe_get(person, 'location')}")

                        if st.button("View", key=f"search_person_{person['id']}", use_container_width=True):
                            go_to_person_details(person['id'])
                            st.rerun()

        st.markdown("---")

# Top Navigation
col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 2])

with col1:
    if st.button("People", use_container_width=True, 
                 type="primary" if st.session_state.current_view == 'people' else "secondary"):
        go_to_people()
        st.rerun()

with col2:
    if st.button("Firms", use_container_width=True, 
                 type="primary" if st.session_state.current_view == 'firms' else "secondary"):
        go_to_firms()
        st.rerun()

with col3:
    if st.button("Add Person", use_container_width=True):
        st.session_state.show_add_person_modal = True
        st.rerun()

with col4:
    if st.button("Add Firm", use_container_width=True):
        st.session_state.show_add_firm_modal = True
        st.rerun()

with col5:
    # Quick stats with Asia breakdown
    col5a, col5b, col5c = st.columns(3)
    with col5a:
        asia_people = len(get_asia_people())
        total_people = len(st.session_state.people)
        st.metric("People", f"{total_people}", delta=f"{asia_people} Asia")
    with col5b:
        asia_firms = len(get_asia_firms())
        total_firms = len(st.session_state.firms)
        st.metric("Firms", f"{total_firms}", delta=f"{asia_firms} Asia")
    with col5c:
        if asia_people > 0:
            asia_percentage = round((asia_people / total_people) * 100) if total_people > 0 else 0
            st.metric("Asia %", f"{asia_percentage}%")
# --- MAIN VIEWS ---

if st.session_state.current_view == 'people':
    st.markdown("---")
    st.header("Financial Professionals")

    if not st.session_state.people:
        st.info("No people added yet. Use 'Add Person' button above or extract from content.")
    else:
        # Prioritize Asia profiles - show Asia first, then others
        asia_people = get_asia_people()
        non_asia_people = get_non_asia_people()

        # Show Asia section first
        if asia_people:
            st.subheader(f"🌏 Asia-Based Professionals ({len(asia_people)})")

            # Display Asia people in square tile grid (3 columns)
            for i in range(0, len(asia_people), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(asia_people):
                        person = asia_people[i + j]

                        with col:
                            # Square-like tile container with Asia indicator
                            with st.container(border=True):
                                st.markdown(f"**{safe_get(person, 'name')}** 🌏")
                                st.caption(f"{safe_get(person, 'current_title')}")
                                st.caption(f"Company: {safe_get(person, 'current_company_name')}")
                                st.caption(f"📍 {safe_get(person, 'location')}")

                                expertise = safe_get(person, 'expertise')
                                if expertise != 'Unknown':
                                    st.caption(f"🎯 {expertise}")

                                # Shared work connections
                                shared_history = get_shared_work_history(person['id'])
                                if shared_history:
                                    strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
                                    st.caption(f"🔗 {len(shared_history)} connections ({strong_connections} strong)")

                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("View", key=f"view_asia_person_{person['id']}", use_container_width=True):
                                        go_to_person_details(person['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("Edit", key=f"edit_asia_person_{person['id']}", use_container_width=True):
                                        st.session_state.edit_person_data = person
                                        st.session_state.show_edit_person_modal = True
                                        st.rerun()

        # Show other regions section
        if non_asia_people:
            if asia_people:  # Add separator if we showed Asia section
                st.markdown("---")

            st.subheader(f"🌍 Other Regions ({len(non_asia_people)})")

            # Display non-Asia people in square tile grid (3 columns)
            for i in range(0, len(non_asia_people), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(non_asia_people):
                        person = non_asia_people[i + j]

                        with col:
                            # Square-like tile container
                            with st.container(border=True):
                                st.markdown(f"**{safe_get(person, 'name')}**")
                                st.caption(f"{safe_get(person, 'current_title')}")
                                st.caption(f"Company: {safe_get(person, 'current_company_name')}")
                                st.caption(f"📍 {safe_get(person, 'location')}")

                                expertise = safe_get(person, 'expertise')
                                if expertise != 'Unknown':
                                    st.caption(f"🎯 {expertise}")

                                # Shared work connections
                                shared_history = get_shared_work_history(person['id'])
                                if shared_history:
                                    strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
                                    st.caption(f"🔗 {len(shared_history)} connections ({strong_connections} strong)")

                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("View", key=f"view_other_person_{person['id']}", use_container_width=True):
                                        go_to_person_details(person['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("Edit", key=f"edit_other_person_{person['id']}", use_container_width=True):
                                        st.session_state.edit_person_data = person
                                        st.session_state.show_edit_person_modal = True
                                        st.rerun()

elif st.session_state.current_view == 'firms':
    st.markdown("---")
    st.header("Financial Institutions")

    if not st.session_state.firms:
        st.info("No firms added yet. Use 'Add Firm' button above.")
    else:
        # Prioritize Asia profiles - show Asia first, then others
        asia_firms = get_asia_firms()
        non_asia_firms = get_non_asia_firms()

        # Show Asia section first
        if asia_firms:
            st.subheader(f"🌏 Asia-Based Institutions ({len(asia_firms)})")

            # Display Asia firms in square tile grid (3 columns)
            for i in range(0, len(asia_firms), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(asia_firms):
                        firm = asia_firms[i + j]
                        people_count = len(get_people_by_firm(safe_get(firm, 'name')))

                        with col:
                            # Square-like tile container with Asia indicator
                            with st.container(border=True):
                                st.markdown(f"**{safe_get(firm, 'name')}** 🌏")
                                st.caption(f"{safe_get(firm, 'firm_type')}")
                                st.caption(f"📍 {safe_get(firm, 'location')}")

                                aum = safe_get(firm, 'aum')
                                if aum != 'Unknown':
                                    st.caption(f"💰 {aum}")

                                st.caption(f"👥 {people_count} people")

                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("View", key=f"view_asia_firm_{firm['id']}", use_container_width=True):
                                        go_to_firm_details(firm['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("Edit", key=f"edit_asia_firm_{firm['id']}", use_container_width=True):
                                        st.session_state.edit_firm_data = firm
                                        st.session_state.show_edit_firm_modal = True
                                        st.rerun()

        # Show other regions section
        if non_asia_firms:
            if asia_firms:  # Add separator if we showed Asia section
                st.markdown("---")

            st.subheader(f"🌍 Other Regions ({len(non_asia_firms)})")

            # Display non-Asia firms in square tile grid (3 columns)
            for i in range(0, len(non_asia_firms), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(non_asia_firms):
                        firm = non_asia_firms[i + j]
                        people_count = len(get_people_by_firm(safe_get(firm, 'name')))

                        with col:
                            # Square-like tile container
                            with st.container(border=True):
                                st.markdown(f"**{safe_get(firm, 'name')}**")
                                st.caption(f"{safe_get(firm, 'firm_type')}")
                                st.caption(f"📍 {safe_get(firm, 'location')}")

                                aum = safe_get(firm, 'aum')
                                if aum != 'Unknown':
                                    st.caption(f"💰 {aum}")

                                st.caption(f"👥 {people_count} people")

                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("View", key=f"view_other_firm_{firm['id']}", use_container_width=True):
                                        go_to_firm_details(firm['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("Edit", key=f"edit_other_firm_{firm['id']}", use_container_width=True):
                                        st.session_state.edit_firm_data = firm
                                        st.session_state.show_edit_firm_modal = True
                                        st.rerun()

elif st.session_state.current_view == 'person_details' and st.session_state.selected_person_id:
    person = get_person_by_id(st.session_state.selected_person_id)
    if not person:
        st.error("Person not found")
        go_to_people()
        st.rerun()

    # Person header with actions
    col1, col2 = st.columns([3, 1])
    with col1:
        st.header(f"{safe_get(person, 'name')}")
        st.subheader(f"{safe_get(person, 'current_title')} at {safe_get(person, 'current_company_name')}")
    with col2:
        col2a, col2b, col2c = st.columns(3)
        with col2a:
            if st.button("← Back"):
                go_to_people()
                st.rerun()
        with col2b:
            if st.button("Edit"):
                st.session_state.edit_person_data = person
                st.session_state.show_edit_person_modal = True
                st.rerun()
        with col2c:
            if st.button("+ Employment"):
                st.session_state.show_add_employment_modal = True
                st.rerun()

    # Basic info
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Location:** {safe_get(person, 'location')}")
        email = safe_get(person, 'email')
        if email != 'Unknown' and email:
            st.markdown(f"**Email:** {email}")
        phone = safe_get(person, 'phone')
        if phone != 'Unknown' and phone:
            st.markdown(f"**Phone:** {phone}")

    with col2:
        education = safe_get(person, 'education')
        if education != 'Unknown' and education:
            st.markdown(f"**Education:** {education}")
        expertise = safe_get(person, 'expertise')
        if expertise != 'Unknown' and expertise:
            st.markdown(f"**Expertise:** {expertise}")
        aum = safe_get(person, 'aum_managed')
        if aum != 'Unknown' and aum:
            st.markdown(f"**AUM Managed:** {aum}")

    # Employment History
    st.markdown("---")
    st.subheader("Employment History")

    employments = get_employments_by_person_id(person['id'])
    if employments:
        sorted_employments = sorted(
            [emp for emp in employments if emp.get('start_date')], 
            key=lambda x: x['start_date'], 
            reverse=True
        )

        for emp in sorted_employments:
            end_date_str = emp['end_date'].strftime("%B %Y") if emp.get('end_date') else "Present"
            start_date_str = emp['start_date'].strftime("%B %Y") if emp.get('start_date') else "Unknown"

            # Calculate duration
            if emp.get('start_date'):
                end_for_calc = emp['end_date'] if emp.get('end_date') else date.today()
                duration_days = (end_for_calc - emp['start_date']).days
                duration_years = duration_days / 365.25

                if duration_years >= 1:
                    duration_str = f"{duration_years:.1f} years"
                else:
                    duration_str = f"{max(1, duration_days // 30)} months"
            else:
                duration_str = "Unknown duration"

            with st.container(border=True):
                st.markdown(f"**{safe_get(emp, 'title')}** at **{safe_get(emp, 'company_name')}**")
                st.caption(f"Duration: {start_date_str} → {end_date_str} ({duration_str})")
                st.caption(f"Location: {safe_get(emp, 'location')} • Strategy: {safe_get(emp, 'strategy')}")
    else:
        st.info("No employment history available.")

    # Shared Work History
    st.markdown("---")
    st.subheader("Shared Work History")

    shared_history = get_shared_work_history(person['id'])

    if shared_history:
        st.write(f"**Found {len(shared_history)} colleagues who worked at the same companies:**")

        # Summary stats
        strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
        medium_connections = len([c for c in shared_history if c.get('connection_strength') == 'Medium'])
        brief_connections = len([c for c in shared_history if c.get('connection_strength') == 'Brief'])

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Strong (1+ years)", strong_connections)
        with col2:
            st.metric("Medium (3+ months)", medium_connections)
        with col3:
            st.metric("Brief (<3 months)", brief_connections)

        # Show connections
        for connection in shared_history[:5]:  # Show first 5
            with st.container(border=True):
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    st.markdown(f"**{connection['colleague_name']}**")
                    st.caption(f"**Shared:** {connection['shared_company']}")
                    st.caption(f"**Overlap:** {connection['overlap_period']}")

                with col2:
                    st.caption(f"**Current:** {connection['colleague_current_title']}")
                    st.caption(f"at {connection['colleague_current_company']}")

                with col3:
                    strength = connection.get('connection_strength', 'Brief')
                    if strength == "Strong":
                        st.success("Strong")
                    elif strength == "Medium":
                        st.info("Medium")
                    else:
                        st.warning("Brief")

                    if st.button("View", key=f"view_colleague_{connection['colleague_id']}", use_container_width=True):
                        go_to_person_details(connection['colleague_id'])
                        st.rerun()

        if len(shared_history) > 5:
            st.info(f"Showing top 5 of {len(shared_history)} total connections")
    else:
        st.info("No shared work history found with other people in the database.")

    # Context/News Section
    st.markdown("---")
    st.subheader("Context & News")

    context_mentions = person.get('context_mentions', [])
    if context_mentions:
        for mention in context_mentions:
            with st.container(border=True):
                st.markdown(f"**{mention.get('type', 'mention').title()}**")
                st.write(mention.get('content', ''))
                st.caption(f"Source: {mention.get('source', 'Unknown')} | {mention.get('timestamp', 'Unknown date')}")
    else:
        st.info("No context or news mentions recorded.")

        # Add context manually
        with st.expander("Add Context/News"):
            context_type = st.selectbox("Type", ["news", "mention", "movement", "performance"])
            context_content = st.text_area("Content")
            context_source = st.text_input("Source")

            if st.button("Add Context"):
                if context_content:
                    success = add_context_to_person(person['id'], context_type, context_content, context_source)
                    if success:
                        save_data_to_drive()
                        st.success("Context added!")
                        st.rerun()

elif st.session_state.current_view == 'firm_details' and st.session_state.selected_firm_id:
    firm = get_firm_by_id(st.session_state.selected_firm_id)
    if not firm:
        st.error("Firm not found")
        go_to_firms()
        st.rerun()

    # Firm header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.header(f"{safe_get(firm, 'name')}")
        st.markdown(f"**{safe_get(firm, 'firm_type')} • {safe_get(firm, 'location')}**")
    with col2:
        col2a, col2b = st.columns(2)
        with col2a:
            if st.button("← Back"):
                go_to_firms()
                st.rerun()
        with col2b:
            if st.button("Edit"):
                st.session_state.edit_firm_data = firm
                st.session_state.show_edit_firm_modal = True
                st.rerun()

    # Firm metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Assets Under Management", safe_get(firm, 'aum'))
    with col2:
        st.metric("Founded", safe_get(firm, 'founded'))
    with col3:
        people_count = len(get_people_by_firm(safe_get(firm, 'name')))
        st.metric("Total People", people_count)

    # Firm details
    description = safe_get(firm, 'description')
    if description != 'Unknown' and description:
        st.markdown(f"**About:** {description}")

    website = safe_get(firm, 'website')
    if website != 'Unknown' and website:
        st.markdown(f"**Website:** [{website}]({website})")

    # People at this firm
    st.markdown("---")
    st.subheader(f"People at {safe_get(firm, 'name')}")

    firm_people = get_people_by_firm(safe_get(firm, 'name'))
    if firm_people:
        for person in firm_people:
            with st.container(border=True):
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    st.markdown(f"**{safe_get(person, 'name')}**")
                    st.caption(safe_get(person, 'current_title'))

                with col2:
                    email = safe_get(person, 'email')
                    expertise = safe_get(person, 'expertise')
                    if email != 'Unknown' and email:
                        st.caption(f"Email: {email}")
                    if expertise != 'Unknown' and expertise:
                        st.caption(f"Expertise: {expertise}")

                with col3:
                    if st.button("View Profile", key=f"view_full_{person['id']}", use_container_width=True):
                        go_to_person_details(person['id'])
                        st.rerun()
    else:
        st.info("No people added for this firm yet.")

    # Context/News Section
    st.markdown("---")
    st.subheader("Context & News")

    context_mentions = firm.get('context_mentions', [])
    if context_mentions:
        for mention in context_mentions:
            with st.container(border=True):
                st.markdown(f"**{mention.get('type', 'mention').title()}**")
                st.write(mention.get('content', ''))
                st.caption(f"Source: {mention.get('source', 'Unknown')} | {mention.get('timestamp', 'Unknown date')}")
    else:
        st.info("No context or news mentions recorded.")

        # Add context manually
        with st.expander("Add Context/News"):
            context_type = st.selectbox("Type", ["news", "mention", "movement", "performance"])
            context_content = st.text_area("Content")
            context_source = st.text_input("Source")

            if st.button("Add Context"):
                if context_content:
                    success = add_context_to_firm(firm['id'], context_type, context_content, context_source)
                    if success:
                        save_data_to_drive()
                        st.success("Context added!")
                        st.rerun()

# Handle modals for adding/editing
if st.session_state.show_add_person_modal:
    st.markdown("---")
    st.subheader("Add New Person")

    with st.form("add_person_form", clear_on_submit=True):
        col1, col2 = st.columns(2)

        with col1:
            name = st.text_input("Full Name*", placeholder="John Smith")
            title = st.text_input("Current Title*", placeholder="Portfolio Manager")
            company = st.text_input("Current Company*", placeholder="Company Name")
            location = st.text_input("Location", placeholder="Hong Kong")

            # REAL-TIME duplicate check with visual feedback
            if name and company:
                existing_person = find_existing_person_strict(name, company)
                person_key = create_person_key(name, company)

                if existing_person:
                    st.error(f"🚫 **DUPLICATE DETECTED**")
                    st.error(f"Person already exists: {safe_get(existing_person, 'name')} at {safe_get(existing_person, 'current_company_name')}")
                    st.info(f"🔑 Generated Key: `{person_key}`")

                    if st.button(f"👁️ View Existing: {safe_get(existing_person, 'name')}", key="view_existing_realtime"):
                        go_to_person_details(existing_person['id'])
                        st.session_state.show_add_person_modal = False
                        st.rerun()
                else:
                    st.success(f"✅ **NEW PERSON** - Ready to add")
                    st.info(f"🔑 Generated Key: `{person_key}`")

        with col2:
            email = st.text_input("Email", placeholder="john.smith@company.com")
            phone = st.text_input("Phone", placeholder="+852-1234-5678")
            education = st.text_input("Education", placeholder="Harvard, MIT")
            expertise = st.text_input("Expertise", placeholder="Equity Research")

        # Employment dates
        st.markdown("**Employment Dates**")
        col3, col4 = st.columns(2)
        with col3:
            start_date = st.date_input("Start Date", value=date.today())
            aum_managed = st.text_input("AUM Managed", placeholder="500M USD")
        with col4:
            is_current = st.checkbox("Current Position", value=True)
            end_date = None if is_current else st.date_input("End Date", value=date.today())
            strategy = st.text_input("Strategy", placeholder="Long/Short Equity")

        submitted = st.form_submit_button("Add Person", use_container_width=True)

        if submitted:
            if name and title and company:
                # STRICT duplicate check - completely block if exists
                existing_person = find_existing_person_strict(name, company)

                if existing_person:
                    st.error(f"🚫 **DUPLICATE BLOCKED**: Person '{name}' already exists at '{company}'")
                    st.info(f"**Existing Profile**: {safe_get(existing_person, 'name')} - {safe_get(existing_person, 'current_title')} at {safe_get(existing_person, 'current_company_name')}")
                    st.warning("❌ **Cannot create duplicate profiles**. Use the Edit function to update existing profiles.")

                    # Show link to existing profile
                    if st.button(f"👁️ View Existing Profile: {safe_get(existing_person, 'name')}", key="view_existing_duplicate"):
                        go_to_person_details(existing_person['id'])
                        st.session_state.show_add_person_modal = False
                        st.rerun()

                else:
                    # Create new person - no duplicate exists
                    new_person_id = str(uuid.uuid4())
                    new_person = {
                        "id": new_person_id,
                        "name": name,
                        "current_title": title,
                        "current_company_name": company,
                        "location": location or "Unknown",
                        "email": email or "",
                        "phone": phone or "",
                        "education": education or "",
                        "expertise": expertise or "",
                        "aum_managed": aum_managed or "",
                        "strategy": strategy or "",
                        "created_date": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "context_mentions": [{
                            'id': str(uuid.uuid4()),
                            'timestamp': datetime.now().isoformat(),
                            'type': 'mention',
                            'content': f"Profile created manually",
                            'source': 'Manual Entry',
                            'date_added': datetime.now().isoformat()
                        }]
                    }

                    # FINAL SAFETY CHECK before adding to database
                    final_check = find_existing_person_strict(new_person['name'], new_person['current_company_name'])
                    if final_check:
                        st.error(f"🚫 **FINAL SAFETY CHECK BLOCKED**: Duplicate detected just before save!")
                        st.error(f"Matches: {safe_get(final_check, 'name')} at {safe_get(final_check, 'current_company_name')}")
                    else:
                        # Only add if final check passes
                        st.session_state.people.append(new_person)

                        # Tag as Asia-based
                        tag_profile_as_asia(new_person, 'person')

                        # Add employment record
                        success = add_employment_with_dates(
                            new_person_id, company, title, start_date, end_date, location or "Unknown", strategy or "Unknown"
                        )

                        if success:
                            save_data_to_drive()
                            st.success(f"✅ Successfully added {name}!")
                            st.session_state.show_add_person_modal = False
                            st.rerun()
                        else:
                            st.error("❌ Failed to add employment record")
            else:
                st.error("❌ Please fill required fields (*)")

    if st.button("Cancel", key="cancel_add_person"):
        st.session_state.show_add_person_modal = False
        st.rerun()

# Add Firm Modal
if st.session_state.show_add_firm_modal:
    st.markdown("---")
    st.subheader("Add New Firm")

    with st.form("add_firm_form", clear_on_submit=True):
        col1, col2 = st.columns(2)

        with col1:
            firm_name = st.text_input("Firm Name*", placeholder="Goldman Sachs")
            firm_type = st.selectbox("Firm Type*", 
                ["Hedge Fund", "Asset Manager", "Investment Bank", "Private Equity", "Venture Capital", "Family Office", "Other"])
            location = st.text_input("Location*", placeholder="Hong Kong")
            headquarters = st.text_input("Headquarters", placeholder="New York, USA")

        with col2:
            aum = st.text_input("Assets Under Management", placeholder="50B USD")
            founded = st.number_input("Founded Year", min_value=1800, max_value=2025, value=2000)
            website = st.text_input("Website", placeholder="https://company.com")
            strategy = st.text_input("Strategy", placeholder="Multi-Strategy")

        description = st.text_area("Description", placeholder="Brief description of the firm...")

        submitted = st.form_submit_button("Add Firm", use_container_width=True)

        if submitted:
            if firm_name and firm_type and location:
                # Check if firm already exists
                existing_firm = get_firm_by_name(firm_name)

                if existing_firm:
                    st.error(f"🚫 **DUPLICATE**: Firm '{firm_name}' already exists")
                    if st.button(f"👁️ View Existing: {firm_name}", key="view_existing_firm"):
                        go_to_firm_details(existing_firm['id'])
                        st.session_state.show_add_firm_modal = False
                        st.rerun()
                else:
                    # Create new firm

                    st.session_state.firms.append(new_firm)

                    # Tag as Asia-based
                    tag_profile_as_asia(new_firm, 'firm')

                    save_data_to_drive()
                    st.success(f"✅ Successfully added {firm_name}!")
                    st.session_state.show_add_firm_modal = False
                    st.rerun()
            else:
                st.error("❌ Please fill required fields (*)")

    if st.button("Cancel", key="cancel_add_firm"):
        st.session_state.show_add_firm_modal = False
        st.rerun()

# Add Employment Modal
if st.session_state.show_add_employment_modal:
    st.markdown("---")
    st.subheader("Add Employment Record")

    person = get_person_by_id(st.session_state.selected_person_id)
    if person:
        st.write(f"Adding employment for: **{safe_get(person, 'name')}**")

        with st.form("add_employment_form"):
            col1, col2 = st.columns(2)

            with col1:
                company_name = st.text_input("Company Name*", placeholder="Company Name")
                title = st.text_input("Job Title*", placeholder="Portfolio Manager")
                location = st.text_input("Location", placeholder="Hong Kong")

            with col2:
                start_date = st.date_input("Start Date*", value=date.today())
                is_current = st.checkbox("Current Position", value=False)
                end_date = None if is_current else st.date_input("End Date")
                strategy = st.text_input("Strategy/Focus", placeholder="Long/Short Equity")

            submitted = st.form_submit_button("Add Employment", use_container_width=True)

            if submitted:
                if company_name and title and start_date:
                    if not is_current and end_date and end_date <= start_date:
                        st.error("End date must be after start date")
                    else:
                        success = add_employment_with_dates(
                            person['id'], company_name, title, start_date, end_date, location or "Unknown", strategy or "Unknown"
                        )

                        if success:
                            st.success("Employment record added!")
                            st.session_state.show_add_employment_modal = False
                            st.rerun()
                        else:
                            st.error("Failed to add employment record")
                else:
                    st.error("Please fill required fields (*)")

        if st.button("Cancel", key="cancel_add_employment"):
            st.session_state.show_add_employment_modal = False
            st.rerun()

# Edit Person Modal
if st.session_state.show_edit_person_modal and st.session_state.edit_person_data:
    st.markdown("---")
    st.subheader("Edit Person")

    person = st.session_state.edit_person_data

    with st.form("edit_person_form"):
        col1, col2 = st.columns(2)

        with col1:
            name = st.text_input("Full Name*", value=safe_get(person, 'name'))
            title = st.text_input("Current Title*", value=safe_get(person, 'current_title'))
            company = st.text_input("Current Company*", value=safe_get(person, 'current_company_name'))
            location = st.text_input("Location", value=safe_get(person, 'location'))

        with col2:
            email = st.text_input("Email", value=safe_get(person, 'email'))
            phone = st.text_input("Phone", value=safe_get(person, 'phone'))
            education = st.text_input("Education", value=safe_get(person, 'education'))
            expertise = st.text_input("Expertise", value=safe_get(person, 'expertise'))

        col3, col4 = st.columns(2)
        with col3:
            aum_managed = st.text_input("AUM Managed", value=safe_get(person, 'aum_managed'))
        with col4:
            strategy = st.text_input("Strategy", value=safe_get(person, 'strategy'))

        submitted = st.form_submit_button("Update Person", use_container_width=True)

        if submitted:
            if name and title and company:
                # Update the person
                person_index = next(i for i, p in enumerate(st.session_state.people) if p['id'] == person['id'])

                st.session_state.people[person_index].update({
                    'name': name,
                    'current_title': title,
                    'current_company_name': company,
                    'location': location or "Unknown",
                    'email': email or "",
                    'phone': phone or "",
                    'education': education or "",
                    'expertise': expertise or "",
                    'aum_managed': aum_managed or "",
                    'strategy': strategy or "",
                    'last_updated': datetime.now().isoformat()
                })

                # Re-tag as Asia-based
                tag_profile_as_asia(st.session_state.people[person_index], 'person')

                save_data_to_drive()
                st.success(f"✅ Successfully updated {name}!")
                st.session_state.show_edit_person_modal = False
                st.session_state.edit_person_data = None
                st.rerun()
            else:
                st.error("❌ Please fill required fields (*)")

    if st.button("Cancel", key="cancel_edit_person"):
        st.session_state.show_edit_person_modal = False
        st.session_state.edit_person_data = None
        st.rerun()

# Edit Firm Modal
if st.session_state.show_edit_firm_modal and st.session_state.edit_firm_data:
    st.markdown("---")
    st.subheader("Edit Firm")

    firm = st.session_state.edit_firm_data

    with st.form("edit_firm_form"):
        col1, col2 = st.columns(2)

        with col1:
            firm_name = st.text_input("Firm Name*", value=safe_get(firm, 'name'))
            firm_type = st.selectbox("Firm Type*", 
                ["Hedge Fund", "Asset Manager", "Investment Bank", "Private Equity", "Venture Capital", "Family Office", "Other"],
                index=["Hedge Fund", "Asset Manager", "Investment Bank", "Private Equity", "Venture Capital", "Family Office", "Other"].index(safe_get(firm, 'firm_type')) if safe_get(firm, 'firm_type') in ["Hedge Fund", "Asset Manager", "Investment Bank", "Private Equity", "Venture Capital", "Family Office", "Other"] else 0)
            location = st.text_input("Location*", value=safe_get(firm, 'location'))
            headquarters = st.text_input("Headquarters", value=safe_get(firm, 'headquarters'))

        with col2:
            aum = st.text_input("Assets Under Management", value=safe_get(firm, 'aum'))
            founded = st.number_input("Founded Year", min_value=1800, max_value=2025, 
                                     value=int(safe_get(firm, 'founded')) if safe_get(firm, 'founded') and safe_get(firm, 'founded') != 'Unknown' else 2000)
            website = st.text_input("Website", value=safe_get(firm, 'website'))
            strategy = st.text_input("Strategy", value=safe_get(firm, 'strategy'))

        description = st.text_area("Description", value=safe_get(firm, 'description'))

        submitted = st.form_submit_button("Update Firm", use_container_width=True)

        if submitted:
            if firm_name and firm_type and location:
                # Update the firm
                firm_index = next(i for i, f in enumerate(st.session_state.firms) if f['id'] == firm['id'])

                st.session_state.firms[firm_index].update({
                    'name': firm_name,
                    'firm_type': firm_type,
                    'location': location,
                    'headquarters': headquarters or location,
                    'aum': aum or "Unknown",
                    'founded': founded if founded > 1800 else None,
                    'strategy': strategy or "Unknown",
                    'website': website or "",
                    'description': description or "",
                    'last_updated': datetime.now().isoformat()
                })

                # Re-tag as Asia-based
                tag_profile_as_asia(st.session_state.firms[firm_index], 'firm')

                save_data_to_drive()
                st.success(f"✅ Successfully updated {firm_name}!")
                st.session_state.show_edit_firm_modal = False
                st.session_state.edit_firm_data = None
                st.rerun()
            else:
                st.error("❌ Please fill required fields (*)")

    if st.button("Cancel", key="cancel_edit_firm"):
        st.session_state.show_edit_firm_modal = False
        st.session_state.edit_firm_data = None
        st.rerun()

# Auto-save functionality
current_time = datetime.now()
if 'last_auto_save' not in st.session_state:
    st.session_state.last_auto_save = current_time

time_since_save = (current_time - st.session_state.last_auto_save).total_seconds()
if time_since_save > 30 and (st.session_state.people or st.session_state.firms):
    save_data_to_drive()
    st.session_state.last_auto_save = current_time

# --- ASIA-SPECIFIC EXPORT SECTION ---
st.markdown("---")
col1, col2, col3 = st.columns([2, 1, 2])

with col2:
    asia_csv_data, asia_filename = export_asia_csv()
    if asia_csv_data:
        asia_people_count = len(get_asia_people())
        asia_firms_count = len(get_asia_firms())

        st.download_button(
            f"🌏 Download Asia Database ({asia_people_count + asia_firms_count} records)",
            asia_csv_data,
            asia_filename,
            "text/csv",
            use_container_width=True,
            help=f"Export {asia_people_count} Asia-based people and {asia_firms_count} Asia-based firms to CSV"
        )
    else:
        st.info("🌏 No Asia-based profiles found yet")
        st.caption("Asia-based profiles will appear here automatically when detected")

# --- LOG FILE ACCESS FUNCTIONS ---
# def get_recent_logs(log_type="main", lines=50):
#     """Get recent log entries for monitoring"""
#     try:
#         if log_type == "main":
#             log_file = LOGS_DIR / 'hedge_fund_app.log'
#         elif log_type == "extraction":
#             log_file = LOGS_DIR / 'extraction.log'
#         elif log_type == "database":
#             log_file = LOGS_DIR / 'database.log'
#         elif log_type == "api":
#             log_file = LOGS_DIR / 'api.log'
#         elif log_type == "user_actions":
#             log_file = LOGS_DIR / 'user_actions.log'
#         else:
#             return []

#         if not log_file.exists():
#             return []

#         with open(log_file, 'r', encoding='utf-8') as f:
#             all_lines = f.readlines()
#             return all_lines[-lines:] if len(all_lines) > lines else all_lines

#     except Exception as e:
#         logger.error(f"Error reading log file {log_type}: {e}")
#         return []

def log_session_summary():
    """Log session summary statistics"""
    try:
        people_count = len(st.session_state.people)
        firms_count = len(st.session_state.firms)
        asia_people = len(get_asia_people())
        asia_firms = len(get_asia_firms())

        log_user_action("SESSION_SUMMARY", 
            f"Session {SESSION_ID} stats: {people_count} people ({asia_people} Asia), {firms_count} firms ({asia_firms} Asia)")

    except Exception as e:
        logger.error(f"Error logging session summary: {e}")

# --- COMPREHENSIVE DEBUGGING SECTION ---
if st.checkbox("🔧 Debug Mode - Show Database Details", help="Show detailed database information for debugging"):
    st.markdown("---")
    st.subheader("🔧 Database Debug Information")

    # Log debug mode access
    log_user_action("DEBUG_MODE", "User entered debug mode")

    # Show Google Drive connection status
    if drive_manager.service:
        st.success("✅ Google Drive Connected")
        files = drive_manager.list_files()
        st.info(f"📁 Files in Google Drive: {len(files)}")

        if files:
            for file in files:
                st.text(f"📄 {file['name']} - Modified: {file.get('modifiedTime', 'Unknown')}")
    else:
        st.error("❌ Google Drive Not Connected")

    # Show all current person keys
    st.markdown("**Current People in Database:**")
    if st.session_state.people:
        for i, person in enumerate(st.session_state.people):
            name = safe_get(person, 'name')
            company = safe_get(person, 'current_company_name')
            key = create_person_key(name, company)

            col1, col2, col3 = st.columns([2, 2, 2])
            with col1:
                st.write(f"**{i+1}. {name}**")
            with col2:
                st.write(f"{company}")
            with col3:
                st.code(f"{key}")
    else:
        st.info("No people in database")

    # Test duplicate detection
    st.markdown("**🧪 Test Duplicate Detection:**")
    col1, col2, col3 = st.columns(3)

    with col1:
        test_name = st.text_input("Test Name:", key="debug_name")
    with col2:
        test_company = st.text_input("Test Company:", key="debug_company")
    with col3:
        if st.button("🔍 Check Duplicate", key="debug_check"):
            if test_name and test_company:
                log_user_action("DEBUG_DUPLICATE_TEST", f"Testing duplicate for: '{test_name}' at '{test_company}'")

                existing = find_existing_person_strict(test_name, test_company)
                test_key = create_person_key(test_name, test_company)

                st.write(f"**Generated Key:** `{test_key}`")

                if existing:
                    st.error(f"🚫 DUPLICATE FOUND")
                    st.write(f"Matches: {safe_get(existing, 'name')} at {safe_get(existing, 'current_company_name')}")
                    existing_key = create_person_key(safe_get(existing, 'name'), safe_get(existing, 'current_company_name'))
                    st.write(f"Existing Key: `{existing_key}`")
                else:
                    st.success(f"✅ NO DUPLICATE - Safe to add")

    # Show normalization examples
    st.markdown("**🔄 Normalization Examples:**")
    examples = [
        ["John Smith", "Goldman Sachs Inc."],
        ["john smith", "goldman sachs"],
        ["Dr. John Smith Jr.", "Goldman Sachs Corporation"],
        ["Li Wei Chen", "Hillhouse Capital Management Ltd"]
    ]

    for name, company in examples:
        key = create_person_key(name, company)
        st.write(f"• `{name}` + `{company}` → `{key}`")

    # Show recent logs
    st.markdown("---")
    st.subheader("📋 Recent Log Entries")

    log_type = st.selectbox("Select Log Type:", 
        ["user_actions", "extraction", "database", "api", "main"])

    if st.button("Refresh Logs"):
        log_user_action("DEBUG_LOG_VIEW", f"User viewed {log_type} logs")

    recent_logs = get_recent_logs(log_type, 20)
    if recent_logs:
        st.text_area("Recent Log Entries:", 
            value="".join(recent_logs), 
            height=300,
            disabled=True)
    else:
        st.info(f"No {log_type} logs found")

# Log session summary before exit (this runs every time)
log_session_summary()

# Footer with Google Drive sync status
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if drive_manager.service:
        st.success("🔗 Data synced to Google Drive")
        st.caption("All changes are automatically saved to your Google Drive")
    else:
        st.error("⚠️ Google Drive not connected - data may be lost on restart")
        st.caption("Please configure Google Drive integration for persistent storage")
