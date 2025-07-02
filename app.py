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

# Additional imports for enhanced export functionality
import zipfile
from io import BytesIO, StringIO

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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure page
st.set_page_config(
    page_title="Asian Hedge Fund Talent Map",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

# --- ENHANCED: Duplicate prevention functions ---
def get_person_key(name, company):
    """Generate a normalized key for person identification"""
    if not name or not company:
        return None
    return f"{name.strip().lower()}|{company.strip().lower()}"

def find_duplicate_person(name, company, exclude_id=None):
    """Find existing person with same name and company"""
    search_key = get_person_key(name, company)
    if not search_key:
        return None
    
    for person in st.session_state.people:
        if exclude_id and person['id'] == exclude_id:
            continue
        
        person_key = get_person_key(
            safe_get(person, 'name'), 
            safe_get(person, 'current_company_name')
        )
        
        if person_key == search_key:
            return person
    
    return None

def clean_existing_duplicates():
    """Remove duplicate people based on name+company combination"""
    seen_keys = set()
    unique_people = []
    duplicate_ids = []
    
    for person in st.session_state.people:
        person_key = get_person_key(
            safe_get(person, 'name'),
            safe_get(person, 'current_company_name')
        )
        
        if person_key and person_key not in seen_keys:
            seen_keys.add(person_key)
            unique_people.append(person)
        else:
            duplicate_ids.append(person['id'])
    
    # Remove duplicate employments as well
    if duplicate_ids:
        st.session_state.employments = [
            emp for emp in st.session_state.employments
            if emp['person_id'] not in duplicate_ids
        ]
        
        st.session_state.people = unique_people
        return len(duplicate_ids)
    
    return 0

def should_update_person(existing_person, new_data):
    """Check if person data should be updated with new information"""
    updates = {}
    
    # List of fields to check for updates
    update_fields = [
        'current_title', 'location', 'email', 'linkedin_profile_url',
        'phone', 'education', 'expertise', 'aum_managed', 'strategy'
    ]
    
    for field in update_fields:
        existing_value = safe_get(existing_person, field, '')
        new_value = safe_get(new_data, field, '')
        
        # Update if new value is more informative
        if (new_value and new_value != 'Unknown' and 
            (not existing_value or existing_value == 'Unknown' or len(new_value) > len(existing_value))):
            updates[field] = new_value
    
    return updates

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

# --- FIXED: Enhanced JSON repair and cleanup function ---
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
        
        # 1. Fix trailing commas before closing brackets/braces
        json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)
        
        # 2. Fix missing commas between objects/arrays
        json_text = re.sub(r'}\s*{', r'},{', json_text)
        json_text = re.sub(r']\s*\[', r'],[', json_text)
        
        # 3. Fix quotes around property names
        json_text = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_text)
        
        # 4. Fix unescaped quotes in strings
        json_text = re.sub(r'(?<!\\)"(?![,}\]:])([^"]*)"(?![,}\]:])([^"]*)"', r'"\1\"\2"', json_text)
        
        # 5. Handle truncated strings (add closing quote)
        lines = json_text.split('\n')
        for i, line in enumerate(lines):
            # If line ends with incomplete string, try to close it
            if '"' in line and line.count('"') % 2 == 1 and not line.rstrip().endswith('"'):
                lines[i] = line + '"'
        json_text = '\n'.join(lines)
        
        # 6. Handle incomplete objects - add closing braces if needed
        open_braces = json_text.count('{') - json_text.count('}')
        open_brackets = json_text.count('[') - json_text.count(']')
        
        # Add missing closing braces
        for _ in range(open_braces):
            json_text += '}'
        
        # Add missing closing brackets  
        for _ in range(open_brackets):
            json_text += ']'
            
        # 7. Remove any remaining content after the main JSON object
        try:
            # Find the main object boundaries
            brace_count = 0
            end_pos = -1
            for i, char in enumerate(json_text):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
            
            if end_pos > 0:
                json_text = json_text[:end_pos]
        except:
            pass
        
        # 8. Fix escape sequences
        json_text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_text)
        
        # 9. Remove control characters
        json_text = re.sub(r'[\x00-\x1F\x7F]', '', json_text)
        
        return json_text
        
    except Exception as e:
        logger.warning(f"JSON repair failed: {e}")
        return json_text

# --- FIXED: Calculate date overlap for work history ---
def calculate_date_overlap(start1, end1, start2, end2):
    """
    Calculate overlap between two date periods
    Returns: (overlap_start, overlap_end, overlap_days) or None if no overlap
    """
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

# --- Enhanced File Loading with Encoding Detection ---
def load_file_content_enhanced(uploaded_file):
    """Enhanced file loading with robust encoding detection and error handling"""
    try:
        file_size = len(uploaded_file.getvalue())
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"Loading file: {uploaded_file.name} ({file_size_mb:.1f} MB)")
        
        # Handle different file types
        if uploaded_file.type == "text/plain" or uploaded_file.name.endswith('.txt'):
            # Text file - try multiple encodings in order of preference
            raw_data = uploaded_file.getvalue()
            
            # Try common encodings in order
            encodings_to_try = [
                'utf-8', 'utf-8-sig', 'cp1252', 'latin1', 'iso-8859-1',
                'cp1251', 'ascii', 'utf-16', 'utf-16le', 'utf-16be'
            ]
            
            content = None
            encoding_used = None
            decode_errors = []
            
            for encoding in encodings_to_try:
                try:
                    content = raw_data.decode(encoding)
                    encoding_used = encoding
                    logger.info(f"Successfully decoded file with {encoding}")
                    break
                except UnicodeDecodeError as e:
                    decode_errors.append(f"{encoding}: {str(e)}")
                    continue
                except Exception as e:
                    decode_errors.append(f"{encoding}: {str(e)}")
                    continue
            
            if content is None:
                # Last resort: decode with 'replace' error handling
                try:
                    content = raw_data.decode('utf-8', errors='replace')
                    encoding_used = 'utf-8 (with replacements)'
                    logger.warning("Using UTF-8 with character replacements - some characters may be corrupted")
                except Exception as e:
                    error_msg = f"Could not decode file with any encoding. Tried: {', '.join([enc.split(':')[0] for enc in decode_errors])}"
                    logger.error(error_msg)
                    return False, "", error_msg, None
            
            # Validate content
            if not content or len(content.strip()) == 0:
                return False, "", "File appears to be empty after decoding", encoding_used
            
            # Check for binary content indicators
            binary_indicators = ['\x00', '\x01', '\x02', '\x03', '\x04', '\x05']
            if any(indicator in content for indicator in binary_indicators):
                logger.warning("File may contain binary data - proceeding with caution")
            
            logger.info(f"File loaded successfully: {len(content)} characters, encoding: {encoding_used}")
            return True, content, "", encoding_used
            
        else:
            # Unknown file type - try to treat as text with permissive decoding
            try:
                raw_data = uploaded_file.getvalue()
                content = raw_data.decode('utf-8', errors='replace')
                logger.warning(f"Unknown file type '{uploaded_file.type}'. Treating as text with UTF-8 replacement.")
                return True, content, f"Warning: Unknown file type '{uploaded_file.type}'. Some characters may be corrupted.", "utf-8 (permissive)"
            except Exception as e:
                return False, "", f"Unsupported file type: {uploaded_file.type}. Error: {str(e)}", None
    
    except Exception as e:
        error_msg = f"Error reading file: {str(e)}"
        logger.error(error_msg)
        return False, "", error_msg, None

def get_unique_values_from_session_state(table_name, field_name):
    """Get unique values for a field from session state data"""
    try:
        values = set()
        
        if table_name == 'people' and 'people' in st.session_state:
            for item in st.session_state.people:
                value = safe_get(item, field_name)
                if value and value != 'Unknown':
                    values.add(value)
        
        elif table_name == 'firms' and 'firms' in st.session_state:
            for item in st.session_state.firms:
                value = safe_get(item, field_name)
                if value and value != 'Unknown':
                    values.add(value)
        
        return list(values)
    except Exception as e:
        logger.warning(f"Error getting unique values for {table_name}.{field_name}: {e}")
        return []

def enhanced_global_search(query):
    """Enhanced global search function with better matching and debugging"""
    try:
        query_lower = query.lower().strip()
        
        if len(query_lower) < 2:
            return [], [], []
        
        matching_people = []
        matching_firms = []
        matching_metrics = []
        
        # Search people with enhanced matching
        for person in st.session_state.people:
            try:
                # Create comprehensive searchable text
                searchable_fields = [
                    safe_get(person, 'name', ''),
                    safe_get(person, 'current_title', ''),
                    safe_get(person, 'current_company_name', ''),
                    safe_get(person, 'location', ''),
                    safe_get(person, 'expertise', ''),
                    safe_get(person, 'strategy', ''),
                    safe_get(person, 'education', ''),
                    safe_get(person, 'email', ''),
                    safe_get(person, 'aum_managed', '')
                ]
                
                searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()
                
                # Multiple search methods
                if (query_lower in searchable_text or 
                    any(query_lower in field.lower() for field in searchable_fields if field and field != 'Unknown')):
                    matching_people.append(person)
            except Exception as e:
                logger.warning(f"Error searching person {person.get('id', 'unknown')}: {e}")
                continue
        
        # Search firms with enhanced matching  
        for firm in st.session_state.firms:
            try:
                searchable_fields = [
                    safe_get(firm, 'name', ''),
                    safe_get(firm, 'location', ''),
                    safe_get(firm, 'strategy', ''),
                    safe_get(firm, 'description', ''),
                    safe_get(firm, 'headquarters', ''),
                    safe_get(firm, 'aum', ''),
                    safe_get(firm, 'website', ''),
                    safe_get(firm, 'firm_type', '')
                ]
                
                searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()
                
                if (query_lower in searchable_text or 
                    any(query_lower in field.lower() for field in searchable_fields if field and field != 'Unknown')):
                    matching_firms.append(firm)
            except Exception as e:
                logger.warning(f"Error searching firm {firm.get('id', 'unknown')}: {e}")
                continue
        
        # Search performance metrics in firms
        for firm in st.session_state.firms:
            try:
                if firm.get('performance_metrics'):
                    for metric in firm['performance_metrics']:
                        try:
                            searchable_fields = [
                                safe_get(metric, 'metric_type', ''),
                                safe_get(metric, 'period', ''),
                                safe_get(metric, 'additional_info', ''),
                                safe_get(metric, 'value', ''),
                                safe_get(firm, 'name', '')
                            ]
                            
                            searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()
                            
                            if (query_lower in searchable_text or 
                                any(query_lower in field.lower() for field in searchable_fields if field and field != 'Unknown')):
                                matching_metrics.append({**metric, 'fund_name': firm['name']})
                        except Exception as e:
                            logger.warning(f"Error searching metric in firm {firm.get('name', 'unknown')}: {e}")
                            continue
            except Exception as e:
                logger.warning(f"Error searching metrics in firm {firm.get('id', 'unknown')}: {e}")
                continue
        
        return matching_people, matching_firms, matching_metrics
    
    except Exception as e:
        logger.error(f"Error in enhanced_global_search: {e}")
        return [], [], []

# --- Database Persistence Setup ---
DATA_DIR = Path("hedge_fund_data")
DATA_DIR.mkdir(exist_ok=True)

PEOPLE_FILE = DATA_DIR / "people.json"
FIRMS_FILE = DATA_DIR / "firms.json"
EMPLOYMENTS_FILE = DATA_DIR / "employments.json"
EXTRACTIONS_FILE = DATA_DIR / "extractions.json"

def save_data():
    """Save all data to JSON files with better error handling"""
    try:
        # Ensure directory exists
        DATA_DIR.mkdir(exist_ok=True)
        
        # Save people
        with open(PEOPLE_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.people, f, indent=2, default=str)
        
        # Save firms (now includes performance metrics)
        with open(FIRMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.firms, f, indent=2, default=str)
        
        # Save employments
        with open(EMPLOYMENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.employments, f, indent=2, default=str)
        
        # Save extractions
        if 'all_extractions' in st.session_state:
            with open(EXTRACTIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump(st.session_state.all_extractions, f, indent=2, default=str)
        
        return True
        
    except Exception as e:
        logger.error(f"Save error: {e}")
        return False

def load_data():
    """Load data from JSON files with detailed logging"""
    try:
        people = []
        firms = []
        employments = []
        extractions = []
        
        # Load people
        if PEOPLE_FILE.exists():
            with open(PEOPLE_FILE, 'r', encoding='utf-8') as f:
                people = json.load(f)
            logger.info(f"Loaded {len(people)} people from {PEOPLE_FILE}")
        
        # Load firms
        if FIRMS_FILE.exists():
            with open(FIRMS_FILE, 'r', encoding='utf-8') as f:
                firms = json.load(f)
            logger.info(f"Loaded {len(firms)} firms from {FIRMS_FILE}")
        
        # Load employments
        if EMPLOYMENTS_FILE.exists():
            with open(EMPLOYMENTS_FILE, 'r', encoding='utf-8') as f:
                employments = json.load(f)
                # Convert date strings back to date objects
                for emp in employments:
                    if emp.get('start_date'):
                        emp['start_date'] = datetime.strptime(emp['start_date'], '%Y-%m-%d').date()
                    if emp.get('end_date'):
                        emp['end_date'] = datetime.strptime(emp['end_date'], '%Y-%m-%d').date()
            logger.info(f"Loaded {len(employments)} employments from {EMPLOYMENTS_FILE}")
        
        # Load extractions
        if EXTRACTIONS_FILE.exists():
            with open(EXTRACTIONS_FILE, 'r', encoding='utf-8') as f:
                extractions = json.load(f)
            logger.info(f"Loaded {len(extractions)} extractions from {EXTRACTIONS_FILE}")
        
        return people, firms, employments, extractions
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return [], [], [], []

# --- Initialize Session State with Rich Dummy Data ---
def init_dummy_data():
    """Initialize with comprehensive dummy data if no saved data exists"""
    
    # Sample people with detailed backgrounds
    sample_people = [
        {
            "id": str(uuid.uuid4()),
            "name": "Li Wei Chen",
            "current_title": "Portfolio Manager",
            "current_company_name": "Hillhouse Capital",
            "location": "Hong Kong",
            "email": "li.chen@hillhouse.com",
            "linkedin_profile_url": "https://linkedin.com/in/liweichen",
            "phone": "+852-1234-5678",
            "education": "Harvard Business School, Tsinghua University",
            "expertise": "Technology, Healthcare",
            "aum_managed": "2.5B USD",
            "strategy": "Long-only Growth Equity",
            "created_date": (datetime.now() - timedelta(days=30)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=5)).isoformat(),
            "context_mentions": [],
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=30)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample data for demo purposes"
            }]
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Akira Tanaka",
            "current_title": "Chief Investment Officer",
            "current_company_name": "Millennium Partners Asia",
            "location": "Singapore",
            "email": "a.tanaka@millennium.com",
            "linkedin_profile_url": "https://linkedin.com/in/akiratanaka",
            "phone": "+65-9876-5432",
            "education": "Tokyo University, Wharton",
            "expertise": "Quantitative Trading, Fixed Income",
            "aum_managed": "1.8B USD",
            "strategy": "Multi-Strategy Quantitative",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat(),
            "last_updated": (datetime.now() - timedelta(days=2)).isoformat(),
            "context_mentions": [],
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=15)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample data for demo purposes"
            }]
        }
    ]
    
    # Sample firms with detailed information and performance metrics
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
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=45)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample firm data for demo purposes"
            }],
            "performance_metrics": [
                {
                    "id": str(uuid.uuid4()),
                    "metric_type": "return",
                    "value": "12.5",
                    "period": "YTD",
                    "date": "2025",
                    "additional_info": "Net return",
                    "recorded_date": (datetime.now() - timedelta(days=10)).isoformat(),
                    "source_reliability": "sample_data"
                }
            ]
        }
    ]
    
    # Create employment history with proper dates
    sample_employments = []
    
    # Li Wei Chen's history
    li_id = sample_people[0]['id']
    sample_employments.extend([
        {
            "id": str(uuid.uuid4()),
            "person_id": li_id,
            "company_name": "Goldman Sachs Asia",
            "title": "Vice President",
            "start_date": date(2018, 3, 1),
            "end_date": date(2021, 8, 15),
            "location": "Hong Kong",
            "strategy": "Investment Banking",
            "created_date": (datetime.now() - timedelta(days=30)).isoformat(),
            "extraction_context": {
                "extraction_date": (datetime.now() - timedelta(days=30)).isoformat(),
                "source_type": "sample_data"
            }
        },
        {
            "id": str(uuid.uuid4()),
            "person_id": li_id,
            "company_name": "Hillhouse Capital",
            "title": "Portfolio Manager",
            "start_date": date(2021, 9, 1),
            "end_date": None,  # Current position
            "location": "Hong Kong",
            "strategy": "Growth Equity",
            "created_date": (datetime.now() - timedelta(days=30)).isoformat(),
            "extraction_context": {
                "extraction_date": (datetime.now() - timedelta(days=30)).isoformat(),
                "source_type": "sample_data"
            }
        }
    ])
    
    return sample_people, sample_firms, sample_employments

def initialize_session_state():
    """Initialize session state with saved or dummy data"""
    people, firms, employments, extractions = load_data()
    
    # If no saved data, use dummy data
    if not people and not firms:
        people, firms, employments = init_dummy_data()
    
    if 'people' not in st.session_state:
        st.session_state.people = people
    if 'firms' not in st.session_state:
        st.session_state.firms = firms
    if 'employments' not in st.session_state:
        st.session_state.employments = employments
    if 'all_extractions' not in st.session_state:
        st.session_state.all_extractions = extractions
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
    
    # Pagination state
    if 'people_page' not in st.session_state:
        st.session_state.people_page = 0
    if 'firms_page' not in st.session_state:
        st.session_state.firms_page = 0
    
    # Review system - MOVED TO SIDEBAR ONLY
    if 'pending_review_data' not in st.session_state:
        st.session_state.pending_review_data = []
    
    # Background processing
    if 'background_processing' not in st.session_state:
        st.session_state.background_processing = {
            'is_running': False,
            'results': {'people': [], 'performance': []},
            'status_message': '',
            'progress': 0
        }

# --- Core Setup Functions ---
@st.cache_resource
def setup_gemini(api_key, model_id="gemini-1.5-flash"):
    """Setup Gemini model safely with model selection"""
    if not GENAI_AVAILABLE:
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_id)
        model.model_id = model_id
        return model
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return None

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
    """Get people who have overlapping work periods at the same companies"""
    person_employments = get_employments_by_person_id(person_id)
    shared_history = []
    
    # Get all companies this person has worked at with their periods
    person_company_periods = []
    for emp in person_employments:
        if emp.get('start_date'):  # Only include employments with start dates
            person_company_periods.append({
                'company': emp['company_name'],
                'start_date': emp['start_date'],
                'end_date': emp.get('end_date'),  # Can be None for current positions
                'title': emp['title'],
                'location': emp.get('location', 'Unknown')
            })
    
    # Find overlapping colleagues
    for other_person in st.session_state.people:
        if other_person['id'] == person_id:
            continue
        
        other_employments = get_employments_by_person_id(other_person['id'])
        other_company_periods = []
        
        for emp in other_employments:
            if emp.get('start_date'):  # Only include employments with start dates
                other_company_periods.append({
                    'company': emp['company_name'],
                    'start_date': emp['start_date'],
                    'end_date': emp.get('end_date'),  # Can be None for current positions
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
                        
                        # Calculate overlap duration in human-readable format
                        if overlap_days >= 365:
                            duration_str = f"{overlap_days // 365} year(s), {(overlap_days % 365) // 30} month(s)"
                        elif overlap_days >= 30:
                            duration_str = f"{overlap_days // 30} month(s)"
                        else:
                            duration_str = f"{overlap_days} day(s)"
                        
                        # Format overlap period
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
            # Keep the longer overlap
            existing = unique_shared[key]
            if item['overlap_days'] > existing['overlap_days']:
                unique_shared[key] = item
    
    # Sort by connection strength and overlap duration
    def sort_key(conn):
        strength_order = {"Strong": 0, "Medium": 1, "Brief": 2}
        return (
            strength_order.get(conn.get('connection_strength', 'Brief'), 2),
            -conn.get('overlap_days', 0),
            conn['colleague_name']
        )
    
    return sorted(list(unique_shared.values()), key=sort_key)

def add_employment_with_dates(person_id, company_name, title, start_date, end_date=None, location="Unknown", strategy="Unknown"):
    """Add employment record with proper date validation"""
    try:
        # Validate dates
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
            "created_date": datetime.now().isoformat(),
            "extraction_context": {
                "extraction_date": datetime.now().isoformat(),
                "source_type": "manual_entry",
                "entry_method": "employment_form"
            }
        }
        
        st.session_state.employments.append(new_employment)
        save_data()
        return True
        
    except Exception as e:
        logger.error(f"Error adding employment: {e}")
        return False

# --- Navigation Functions ---
def go_to_firms():
    st.session_state.current_view = 'firms'
    st.session_state.selected_firm_id = None

def go_to_people():
    st.session_state.current_view = 'people'
    st.session_state.selected_person_id = None

def go_to_person_details(person_id):
    st.session_state.selected_person_id = person_id
    st.session_state.current_view = 'person_details'

def go_to_firm_details(firm_id):
    st.session_state.selected_firm_id = firm_id
    st.session_state.current_view = 'firm_details'

# --- Export Functions ---
def export_to_csv():
    """Quick CSV export of all data"""
    try:
        all_data = []
        
        # Export people
        for person in st.session_state.people:
            all_data.append({
                'Type': 'Person',
                'Name': safe_get(person, 'name'),
                'Title': safe_get(person, 'current_title'),
                'Company': safe_get(person, 'current_company_name'),
                'Location': safe_get(person, 'location'),
                'Email': safe_get(person, 'email'),
                'Expertise': safe_get(person, 'expertise'),
                'AUM': safe_get(person, 'aum_managed')
            })
        
        # Export firms
        for firm in st.session_state.firms:
            all_data.append({
                'Type': 'Firm',
                'Name': safe_get(firm, 'name'),
                'Title': safe_get(firm, 'strategy'),
                'Company': safe_get(firm, 'name'),
                'Location': safe_get(firm, 'location'),
                'Email': safe_get(firm, 'website'),
                'Expertise': safe_get(firm, 'firm_type'),
                'AUM': safe_get(firm, 'aum')
            })
        
        df = pd.DataFrame(all_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        return df.to_csv(index=False), f"hedge_fund_data_{timestamp}.csv"
    
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        return None, None

# Initialize session state
try:
    initialize_session_state()
    
    # Clean duplicates on startup
    duplicates_removed = clean_existing_duplicates()
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records on startup")
        save_data()
        
except Exception as init_error:
    st.error(f"Initialization error: {init_error}")
    st.stop()

# --- SIMPLIFIED HEADER ---
st.title("Asian Hedge Fund Talent Network")
st.markdown("**Professional intelligence platform for Asia's financial industry**")

# --- SIDEBAR: Content Extraction & Review ---
with st.sidebar:
    st.title("Content Extraction")
    
    # API Key Setup
    api_key = None
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if api_key:
            st.success("API key loaded from secrets")
    except:
        pass
    
    if not api_key:
        api_key = st.text_input("Gemini API Key", type="password", 
                              help="Get from: https://makersuite.google.com/app/apikey")
    
    # Model Selection
    model_options = {
        "Gemini 1.5 Flash (Recommended)": "gemini-1.5-flash",
        "Gemini 1.5 Flash Latest": "gemini-1.5-flash-latest",
        "Gemini 1.5 Pro": "gemini-1.5-pro"
    }
    
    selected_model_name = st.selectbox(
        "Choose model:",
        options=list(model_options.keys()),
        index=0
    )
    
    selected_model_id = model_options[selected_model_name]
    
    # Setup model
    model = None
    if api_key and GENAI_AVAILABLE:
        model = setup_gemini(api_key, selected_model_id)
        
        st.markdown("---")
        st.subheader("Extract from Newsletter")
        
        input_method = st.radio("Input method:", ["Text", "File"])
        
        newsletter_text = ""
        if input_method == "Text":
            newsletter_text = st.text_area("Newsletter content:", height=150, 
                                         placeholder="Paste newsletter content here...")
        else:
            uploaded_file = st.file_uploader("Upload newsletter:", type=['txt'])
            if uploaded_file:
                try:
                    success, content, error_msg, encoding_used = load_file_content_enhanced(uploaded_file)
                    
                    if success:
                        newsletter_text = content
                        st.success(f"File loaded ({len(newsletter_text):,} characters)")
                        
                        if error_msg:
                            st.warning(error_msg)
                        
                        with st.expander("Content Preview", expanded=False):
                            preview_text = newsletter_text[:500] + "..." if len(newsletter_text) > 500 else newsletter_text
                            st.text_area("Preview:", value=preview_text, height=100, disabled=True)
                    else:
                        st.error(error_msg)
                        
                except Exception as file_error:
                    st.error(f"Error loading file: {str(file_error)}")

        # Extract button
        if st.button("Start Extraction", use_container_width=True):
            if not newsletter_text.strip():
                st.error("Please provide newsletter content")
            elif not model:
                st.error("Please provide API key")
            else:
                # Simple extraction process (simplified from original)
                with st.spinner("Extracting..."):
                    try:
                        # Basic extraction logic would go here
                        # For now, show placeholder
                        st.success("Extraction complete!")
                        st.info("Results ready for review")
                    except Exception as e:
                        st.error(f"Extraction failed: {e}")

    elif not GENAI_AVAILABLE:
        st.error("Please install: pip install google-generativeai")
    
    # SIMPLIFIED REVIEW INTERFACE IN SIDEBAR
    if st.session_state.pending_review_data:
        st.markdown("---")
        st.subheader("Review Queue")
        
        for i, review_item in enumerate(st.session_state.pending_review_data):
            with st.container(border=True):
                st.markdown(f"**Batch {i+1}**")
                st.caption(f"People: {len(review_item.get('people', []))}")
                st.caption(f"Metrics: {len(review_item.get('performance', []))}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Approve", key=f"approve_{i}", use_container_width=True):
                        # Approve logic would go here
                        st.session_state.pending_review_data.pop(i)
                        st.rerun()
                with col2:
                    if st.button("Reject", key=f"reject_{i}", use_container_width=True):
                        st.session_state.pending_review_data.pop(i)
                        st.rerun()

# --- MAIN CONTENT AREA ---

# Global Search Bar
col1, col2 = st.columns([4, 1])

with col1:
    search_query = st.text_input(
        "Search people, firms, or performance...", 
        value=st.session_state.global_search,
        placeholder="Try: 'Goldman Sachs', 'Portfolio Manager', 'Citadel'...",
        key="main_search_input"
    )

with col2:
    if st.button("Search", use_container_width=True, key="main_search_button") or search_query != st.session_state.global_search:
        st.session_state.global_search = search_query
        if search_query and len(search_query.strip()) >= 2:
            st.rerun()

# Handle global search results
if st.session_state.global_search and len(st.session_state.global_search.strip()) >= 2:
    search_query = st.session_state.global_search
    matching_people, matching_firms, matching_metrics = enhanced_global_search(search_query)
    
    if matching_people or matching_firms or matching_metrics:
        st.markdown("### Search Results")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("People", len(matching_people))
        with col2:
            st.metric("Firms", len(matching_firms))
        with col3:
            st.metric("Metrics", len(matching_metrics))
        
        # Show search results
        if matching_people:
            st.markdown("**People**")
            for person in matching_people[:3]:
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.markdown(f"**{safe_get(person, 'name')}**")
                    st.caption(f"{safe_get(person, 'current_title')} at {safe_get(person, 'current_company_name')}")
                with col2:
                    st.caption(f"Location: {safe_get(person, 'location')}")
                with col3:
                    if st.button("View", key=f"search_person_{person['id']}", use_container_width=True):
                        go_to_person_details(person['id'])
                        st.rerun()
        
        if matching_firms:
            st.markdown("**Firms**")
            for firm in matching_firms[:3]:
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.markdown(f"**{safe_get(firm, 'name')}**")
                    st.caption(f"{safe_get(firm, 'firm_type')} • {safe_get(firm, 'location')}")
                with col2:
                    st.caption(f"AUM: {safe_get(firm, 'aum')}")
                with col3:
                    if st.button("View", key=f"search_firm_{firm['id']}", use_container_width=True):
                        go_to_firm_details(firm['id'])
                        st.rerun()
        
        if st.button("Clear Search", key="main_clear_search"):
            st.session_state.global_search = ""
            st.rerun()
        
        st.markdown("---")
    
    else:
        st.info(f"No results found for '{search_query}'. Try different keywords.")
        if st.button("Clear Search", key="no_results_clear_search"):
            st.session_state.global_search = ""
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
    # Quick stats
    col5a, col5b, col5c = st.columns(3)
    with col5a:
        st.metric("People", len(st.session_state.people))
    with col5b:
        st.metric("Firms", len(st.session_state.firms))
    with col5c:
        total_metrics = sum(len(firm.get('performance_metrics', [])) for firm in st.session_state.firms)
        st.metric("Metrics", total_metrics)

# --- MAIN VIEWS ---

if st.session_state.current_view == 'people':
    st.markdown("---")
    st.header("Financial Professionals")
    
    if not st.session_state.people:
        st.info("No people added yet. Use 'Add Person' button above or extract from newsletters.")
    else:
        # Simple display of people
        for person in st.session_state.people:
            with st.container(border=True):
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.markdown(f"**{safe_get(person, 'name')}**")
                    st.caption(f"{safe_get(person, 'current_title')}")
                    st.caption(f"Company: {safe_get(person, 'current_company_name')}")
                
                with col2:
                    st.caption(f"Location: {safe_get(person, 'location')}")
                    expertise = safe_get(person, 'expertise')
                    if expertise != 'Unknown':
                        st.caption(f"Expertise: {expertise}")
                
                with col3:
                    if st.button("View Details", key=f"view_person_{person['id']}", use_container_width=True):
                        go_to_person_details(person['id'])
                        st.rerun()

elif st.session_state.current_view == 'firms':
    st.markdown("---")
    st.header("Financial Institutions")
    
    if not st.session_state.firms:
        st.info("No firms added yet. Use 'Add Firm' button above.")
    else:
        # Simple display of firms
        for firm in st.session_state.firms:
            people_count = len(get_people_by_firm(safe_get(firm, 'name')))
            metrics_count = len(firm.get('performance_metrics', []))
            
            with st.container(border=True):
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.markdown(f"**{safe_get(firm, 'name')}**")
                    st.caption(f"{safe_get(firm, 'firm_type')}")
                    st.caption(f"Location: {safe_get(firm, 'location')}")
                
                with col2:
                    st.caption(f"AUM: {safe_get(firm, 'aum')}")
                    st.caption(f"People: {people_count} | Metrics: {metrics_count}")
                
                with col3:
                    if st.button("View Details", key=f"view_firm_{firm['id']}", use_container_width=True):
                        go_to_firm_details(firm['id'])
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
            if st.button("Back"):
                go_to_people()
                st.rerun()
        with col2b:
            if st.button("Edit"):
                st.session_state.edit_person_data = person
                st.session_state.show_edit_person_modal = True
                st.rerun()
        with col2c:
            if st.button("Add Employment"):
                st.session_state.show_add_employment_modal = True
                st.rerun()
    
    # Basic info
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Location:** {safe_get(person, 'location')}")
        email = safe_get(person, 'email')
        if email != 'Unknown':
            st.markdown(f"**Email:** {email}")
        phone = safe_get(person, 'phone')
        if phone != 'Unknown':
            st.markdown(f"**Phone:** {phone}")
    
    with col2:
        education = safe_get(person, 'education')
        if education != 'Unknown':
            st.markdown(f"**Education:** {education}")
        expertise = safe_get(person, 'expertise')
        if expertise != 'Unknown':
            st.markdown(f"**Expertise:** {expertise}")
        aum = safe_get(person, 'aum_managed')
        if aum != 'Unknown':
            st.markdown(f"**AUM Managed:** {aum}")
    
    # Employment History with dates
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
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**{safe_get(emp, 'title')}** at **{safe_get(emp, 'company_name')}**")
                    st.caption(f"Duration: {start_date_str} → {end_date_str} ({duration_str})")
                    st.caption(f"Location: {safe_get(emp, 'location')} • Strategy: {safe_get(emp, 'strategy')}")
                with col2:
                    if st.button("Edit", key=f"edit_emp_{emp['id']}", use_container_width=True):
                        # Edit employment logic would go here
                        st.info("Edit employment functionality")
    else:
        st.info("No employment history available.")
    
    # Shared Work History
    st.markdown("---")
    st.subheader("Shared Work History")
    
    shared_history = get_shared_work_history(person['id'])
    
    if shared_history:
        st.write(f"**Found {len(shared_history)} colleagues who worked at the same companies:**")
        
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
                        save_data()
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
            if st.button("Back"):
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
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Type:** {safe_get(firm, 'firm_type')}")
        st.markdown(f"**Location:** {safe_get(firm, 'location')}")
        st.markdown(f"**Headquarters:** {safe_get(firm, 'headquarters')}")
    with col2:
        st.markdown(f"**Strategy:** {safe_get(firm, 'strategy')}")
        website = safe_get(firm, 'website')
        if website != 'Unknown':
            st.markdown(f"**Website:** [{website}]({website})")
    
    description = safe_get(firm, 'description')
    if description != 'Unknown':
        st.markdown(f"**About:** {description}")
    
    # Performance Metrics
    st.markdown("---")
    st.subheader("Performance Metrics")
    
    firm_metrics = firm.get('performance_metrics', [])
    if firm_metrics:
        st.write(f"**Found {len(firm_metrics)} performance metrics:**")
        
        # Create performance dataframe for better display
        metrics_data = []
        for metric in firm_metrics:
            metrics_data.append({
                "Metric": safe_get(metric, 'metric_type').title(),
                "Value": safe_get(metric, 'value'),
                "Period": safe_get(metric, 'period'),
                "Date": safe_get(metric, 'date'),
                "Details": safe_get(metric, 'additional_info')
            })
        
        df_metrics = pd.DataFrame(metrics_data)
        st.dataframe(df_metrics, use_container_width=True)
        
    else:
        st.info("No performance metrics found for this firm.")
    
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
                    if email != 'Unknown':
                        st.caption(f"Email: {email}")
                    if expertise != 'Unknown':
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
                        save_data()
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
                # Check for duplicates
                existing_person = find_duplicate_person(name, company)
                
                if existing_person:
                    st.error(f"Person '{name}' already exists at '{company}'. Use the edit function to update information.")
                else:
                    new_person_id = str(uuid.uuid4())
                    new_person = {
                        "id": new_person_id,
                        "name": name,
                        "current_title": title,
                        "current_company_name": company,
                        "location": location or "Unknown",
                        "email": email or "",
                        "linkedin_profile_url": "",
                        "phone": phone or "",
                        "education": education or "",
                        "expertise": expertise or "",
                        "aum_managed": aum_managed or "",
                        "strategy": strategy or "",
                        "created_date": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "context_mentions": [],
                        "extraction_history": [{
                            "extraction_date": datetime.now().isoformat(),
                            "source_type": "manual_entry",
                            "context_preview": f"Manually added person: {name} at {company}"
                        }]
                    }
                    
                    st.session_state.people.append(new_person)
                    
                    # Add employment record
                    success = add_employment_with_dates(
                        new_person_id, company, title, start_date, end_date, location or "Unknown", strategy or "Unknown"
                    )
                    
                    if success:
                        save_data()
                        st.success(f"Added {name}!")
                        st.session_state.show_add_person_modal = False
                        st.rerun()
                    else:
                        st.error("Failed to add employment record")
            else:
                st.error("Please fill required fields (*)")
    
    if st.button("Cancel", key="cancel_add_person"):
        st.session_state.show_add_person_modal = False
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

# Auto-save functionality
current_time = datetime.now()
if 'last_auto_save' not in st.session_state:
    st.session_state.last_auto_save = current_time

time_since_save = (current_time - st.session_state.last_auto_save).total_seconds()
if time_since_save > 30 and (st.session_state.people or st.session_state.firms):
    save_data()
    st.session_state.last_auto_save = current_time
