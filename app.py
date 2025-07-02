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

# Configure minimal logging - only essential events
import logging
from logging.handlers import RotatingFileHandler
import os

# Configure main logger - minimal logging
logging.basicConfig(
    level=logging.WARNING,  # Only warnings and errors
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            'hedge_fund_app.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=2
        )
    ]
)

logger = logging.getLogger(__name__)

# Essential-only loggers
extraction_logger = logging.getLogger('extraction')
extraction_handler = RotatingFileHandler('extraction.log', maxBytes=2*1024*1024, backupCount=1)
extraction_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
extraction_logger.addHandler(extraction_handler)
extraction_logger.setLevel(logging.INFO)

# Session tracking (minimal)
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())[:8]

SESSION_ID = st.session_state.session_id

def log_essential(message):
    """Log only essential events"""
    extraction_logger.info(f"[{SESSION_ID}] {message}")

def log_extraction_progress(step, details=""):
    """Log extraction progress only"""
    extraction_logger.info(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")

def log_extraction_step(step, details="", level="INFO"):
    """Log extraction step with level"""
    if level == "ERROR":
        extraction_logger.error(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")
    elif level == "WARNING":
        extraction_logger.warning(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")
    else:
        extraction_logger.info(f"[{SESSION_ID}] EXTRACTION: {step} - {details}")

def log_profile_saved(profile_type, name, company=""):
    """Log when profiles are saved"""
    company_str = f" at {company}" if company else ""
    extraction_logger.info(f"[{SESSION_ID}] SAVED: {profile_type} - {name}{company_str}")

def log_user_action(action, details=""):
    """Log user actions"""
    extraction_logger.info(f"[{SESSION_ID}] USER: {action} - {details}")

# Minimal session start log
log_essential(f"Session started")

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

# --- TEST AND DEBUGGING FUNCTIONS ---
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
        json_text = re.sub(r'(?<!\\)"(?![,}\]:])([^"]*)"(?![,}\]:])([^"]*)"', r'"\1\"\2"', json_text)
        
        # Handle truncated strings
        lines = json_text.split('\n')
        for i, line in enumerate(lines):
            if '"' in line and line.count('"') % 2 == 1 and not line.rstrip().endswith('"'):
                lines[i] = line + '"'
        json_text = '\n'.join(lines)
        
        # Handle incomplete objects
        open_braces = json_text.count('{') - json_text.count('}')
        open_brackets = json_text.count('[') - json_text.count(']')
        
        for _ in range(open_braces):
            json_text += '}'
        for _ in range(open_brackets):
            json_text += ']'
        
        # Fix escape sequences and remove control characters
        json_text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_text)
        json_text = re.sub(r'[\x00-\x1F\x7F]', '', json_text)
        
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

# --- Database Persistence Setup ---
DATA_DIR = Path("hedge_fund_data")
DATA_DIR.mkdir(exist_ok=True)

PEOPLE_FILE = DATA_DIR / "people.json"
FIRMS_FILE = DATA_DIR / "firms.json"
EMPLOYMENTS_FILE = DATA_DIR / "employments.json"

def save_data():
    """Save all data to JSON files"""
    try:
        DATA_DIR.mkdir(exist_ok=True)
        
        with open(PEOPLE_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.people, f, indent=2, default=str)
        
        with open(FIRMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.firms, f, indent=2, default=str)
        
        with open(EMPLOYMENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.employments, f, indent=2, default=str)
        
        return True
        
    except Exception as e:
        logger.error(f"Save error: {e}")
        return False

def load_data():
    """Load data from JSON files"""
    try:
        people = []
        firms = []
        employments = []
        
        if PEOPLE_FILE.exists():
            with open(PEOPLE_FILE, 'r', encoding='utf-8') as f:
                people = json.load(f)
        
        if FIRMS_FILE.exists():
            with open(FIRMS_FILE, 'r', encoding='utf-8') as f:
                firms = json.load(f)
        
        if EMPLOYMENTS_FILE.exists():
            with open(EMPLOYMENTS_FILE, 'r', encoding='utf-8') as f:
                employments = json.load(f)
                # Convert date strings back to date objects
                for emp in employments:
                    if emp.get('start_date'):
                        emp['start_date'] = datetime.strptime(emp['start_date'], '%Y-%m-%d').date()
                    if emp.get('end_date'):
                        emp['end_date'] = datetime.strptime(emp['end_date'], '%Y-%m-%d').date()
        
        return people, firms, employments
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
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
    
    # Create employment history
    sample_employments = []
    
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
            "created_date": (datetime.now() - timedelta(days=30)).isoformat()
        },
        {
            "id": str(uuid.uuid4()),
            "person_id": li_id,
            "company_name": "Hillhouse Capital",
            "title": "Portfolio Manager",
            "start_date": date(2021, 9, 1),
            "end_date": None,
            "location": "Hong Kong",
            "strategy": "Growth Equity",
            "created_date": (datetime.now() - timedelta(days=30)).isoformat()
        }
    ])
    
    akira_id = sample_people[1]['id']
    sample_employments.extend([
        {
            "id": str(uuid.uuid4()),
            "person_id": akira_id,
            "company_name": "Goldman Sachs Asia",
            "title": "Associate",
            "start_date": date(2017, 6, 1),
            "end_date": date(2020, 12, 31),
            "location": "Singapore",
            "strategy": "Fixed Income Trading",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat()
        },
        {
            "id": str(uuid.uuid4()),
            "person_id": akira_id,
            "company_name": "Millennium Partners Asia",
            "title": "Chief Investment Officer",
            "start_date": date(2021, 1, 15),
            "end_date": None,
            "location": "Singapore",
            "strategy": "Multi-Strategy Quantitative",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat()
        }
    ])
    
    return sample_people, sample_firms, sample_employments

def initialize_session_state():
    """Initialize session state with saved or sample data"""
    people, firms, employments = load_data()
    
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
                if len(chunk) > 500: # Minimum chunk size
                    chunks.append(chunk)
                    chunk_count += 1
                    log_extraction_step("CHUNK_CREATED", f"Chunk {chunk_count}: {len(chunk)} chars (pos: {current_pos}-{end_pos})")
                current_pos = end_pos
        
        log_extraction_step("CHUNKING_COMPLETE", f"Created {len(chunks)} chunks from {text_length} chars")
        
        all_people = []
        all_performance = []
        failed_chunks = []
        
        # Process chunks with paid tier rate limiting (2000 RPM = ~33 per second)
        delay_between_requests = 0.03 # 30ms delay for paid tier
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
                log_extraction_step("CHUNK_PROCESS_COMPLETE", f"Chunk {i+1}/{len(chunks)} complete: {len(people)} people, {len(performance)} metrics (duration: {chunk_duration:.2f}s)")
                all_people.extend(people)
                all_performance.extend(performance)
                
                # Rate limiting delay (except for last chunk)
                if i < len(chunks) - 1:
                    log_extraction_step("RATE_LIMIT_DELAY", f"Applying {delay_between_requests}s rate limit delay")
                    time.sleep(delay_between_requests)
                    
            except Exception as e:
                chunk_duration = time.time() - chunk_start_time
                failed_chunks.append(i+1)
                log_extraction_step("CHUNK_PROCESS_ERROR", f"Chunk {i+1}/{len(chunks)} failed: {e} (duration: {chunk_duration:.2f}s)", "ERROR")
                continue
                
        total_duration = time.time() - start_time
        log_extraction_step("PROCESS_COMPLETE", f"Extraction process complete: {len(all_people)} people, {len(all_performance)} metrics from {len(chunks)} chunks, {len(failed_chunks)} failed (total duration: {total_duration:.2f}s)")
        
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
    """Get people who have overlapping work periods at the same companies"""
    person_employments = get_employments_by_person_id(person_id)
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
                        person_period['start_date'],
                        person_period['end_date'],
                        other_period['start_date'],
                        other_period['end_date']
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
        strength_order = {"Strong": 0, "Medium": 1, "Brief": 2}
        return (
            strength_order.get(conn.get('connection_strength', 'Brief'), 2),
            -conn.get('overlap_days', 0),
            conn['colleague_name']
        )
        
    return sorted(list(unique_shared.values()), key=sort_key)
    
def get_performance_by_firm(firm_id):
    """Get performance metrics for a given firm ID"""
    firm = get_firm_by_id(firm_id)
    if firm:
        return firm.get('performance_metrics', [])
    return []

def add_performance_metric_to_firm(firm_id, metric_type, value, period, date_str):
    """Add a performance metric to a firm's profile"""
    firm = get_firm_by_id(firm_id)
    if not firm:
        return False
    
    if 'performance_metrics' not in firm:
        firm['performance_metrics'] = []
    
    metric_entry = {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'type': metric_type,
        'value': value,
        'period': period,
        'date': date_str,
        'date_added': datetime.now().isoformat()
    }
    
    firm['performance_metrics'].append(metric_entry)
    firm['last_updated'] = datetime.now().isoformat()
    return True

# --- Data Management Functions ---
def add_person(person_data):
    """Add a new person to the session state"""
    person_data['id'] = str(uuid.uuid4())
    person_data['created_date'] = datetime.now().isoformat()
    person_data['last_updated'] = datetime.now().isoformat()
    person_data['context_mentions'] = []
    st.session_state.people.append(person_data)
    log_profile_saved("person", safe_get(person_data, 'name'), safe_get(person_data, 'current_company_name'))

def update_person(person_id, new_data):
    """Update an existing person's data"""
    person = get_person_by_id(person_id)
    if person:
        person.update(new_data)
        person['last_updated'] = datetime.now().isoformat()
        log_user_action("UPDATE_PERSON", f"Updated person: {safe_get(person, 'name')}")
        return True
    return False

def delete_person(person_id):
    """Delete a person and their associated employments"""
    person = get_person_by_id(person_id)
    if person:
        st.session_state.people = [p for p in st.session_state.people if p['id'] != person_id]
        st.session_state.employments = [e for e in st.session_state.employments if e['person_id'] != person_id]
        log_user_action("DELETE_PERSON", f"Deleted person: {safe_get(person, 'name')}")
        return True
    return False

def add_firm(firm_data):
    """Add a new firm to the session state"""
    firm_data['id'] = str(uuid.uuid4())
    firm_data['created_date'] = datetime.now().isoformat()
    firm_data['last_updated'] = datetime.now().isoformat()
    firm_data['context_mentions'] = []
    firm_data['performance_metrics'] = []
    st.session_state.firms.append(firm_data)
    log_profile_saved("firm", safe_get(firm_data, 'name'))

def update_firm(firm_id, new_data):
    """Update an existing firm's data"""
    firm = get_firm_by_id(firm_id)
    if firm:
        firm.update(new_data)
        firm['last_updated'] = datetime.now().isoformat()
        log_user_action("UPDATE_FIRM", f"Updated firm: {safe_get(firm, 'name')}")
        return True
    return False

def delete_firm(firm_id):
    """Delete a firm"""
    firm = get_firm_by_id(firm_id)
    if firm:
        st.session_state.firms = [f for f in st.session_state.firms if f['id'] != firm_id]
        log_user_action("DELETE_FIRM", f"Deleted firm: {safe_get(firm, 'name')}")
        return True
    return False

def add_employment(employment_data):
    """Add a new employment record"""
    employment_data['id'] = str(uuid.uuid4())
    employment_data['created_date'] = datetime.now().isoformat()
    # Convert date objects if necessary
    if 'start_date' in employment_data and isinstance(employment_data['start_date'], str):
        employment_data['start_date'] = datetime.strptime(employment_data['start_date'], '%Y-%m-%d').date()
    if 'end_date' in employment_data and isinstance(employment_data['end_date'], str):
        employment_data['end_date'] = datetime.strptime(employment_data['end_date'], '%Y-%m-%d').date()
    elif 'end_date' in employment_data and employment_data['end_date'] == "Present":
        employment_data['end_date'] = None # Represent "Present" as None
        
    st.session_state.employments.append(employment_data)
    log_user_action("ADD_EMPLOYMENT", f"Added employment for {employment_data.get('person_id')} at {employment_data.get('company_name')}")

def update_employment(employment_id, new_data):
    """Update an existing employment record"""
    for i, emp in enumerate(st.session_state.employments):
        if emp['id'] == employment_id:
            st.session_state.employments[i].update(new_data)
            if 'start_date' in st.session_state.employments[i] and isinstance(st.session_state.employments[i]['start_date'], str):
                st.session_state.employments[i]['start_date'] = datetime.strptime(st.session_state.employments[i]['start_date'], '%Y-%m-%d').date()
            if 'end_date' in st.session_state.employments[i] and isinstance(st.session_state.employments[i]['end_date'], str):
                st.session_state.employments[i]['end_date'] = datetime.strptime(st.session_state.employments[i]['end_date'], '%Y-%m-%d').date()
            elif 'end_date' in st.session_state.employments[i] and st.session_state.employments[i]['end_date'] == "Present":
                st.session_state.employments[i]['end_date'] = None
            log_user_action("UPDATE_EMPLOYMENT", f"Updated employment: {employment_id}")
            return True
    return False

def delete_employment(employment_id):
    """Delete an employment record"""
    original_count = len(st.session_state.employments)
    st.session_state.employments = [e for e in st.session_state.employments if e['id'] != employment_id]
    if len(st.session_state.employments) < original_count:
        log_user_action("DELETE_EMPLOYMENT", f"Deleted employment: {employment_id}")
        return True
    return False

# --- Export Functions ---
def export_data_to_excel(people_data, firms_data, employments_data, performance_data):
    """Export all data to a multi-sheet Excel file"""
    if not EXCEL_AVAILABLE:
        st.error("`openpyxl` library not found. Please install it for Excel export: `pip install openpyxl`")
        return None, "Excel export not available."
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # People Sheet
        if people_data:
            people_df = pd.DataFrame(people_data)
            # Exclude context_mentions column
            if 'context_mentions' in people_df.columns:
                people_df = people_df.drop(columns=['context_mentions'])
            people_df.to_excel(writer, sheet_name='People', index=False)
        else:
            pd.DataFrame([{"Message": "No people data available."}]).to_excel(writer, sheet_name='People', index=False)
        
        # Firms Sheet
        if firms_data:
            firms_df = pd.DataFrame(firms_data)
            # Exclude context_mentions and performance_metrics columns
            columns_to_drop = []
            if 'context_mentions' in firms_df.columns:
                columns_to_drop.append('context_mentions')
            if 'performance_metrics' in firms_df.columns:
                columns_to_drop.append('performance_metrics')
            if columns_to_drop:
                firms_df = firms_df.drop(columns=columns_to_drop)
            firms_df.to_excel(writer, sheet_name='Firms', index=False)
        else:
            pd.DataFrame([{"Message": "No firm data available."}]).to_excel(writer, sheet_name='Firms', index=False)
            
        # Employments Sheet
        if employments_data:
            employments_df = pd.DataFrame(employments_data)
            employments_df.to_excel(writer, sheet_name='Employments', index=False)
        else:
            pd.DataFrame([{"Message": "No employment data available."}]).to_excel(writer, sheet_name='Employments', index=False)
            
        # Performance Metrics Sheet
        if performance_data:
            performance_df = pd.DataFrame(performance_data)
            performance_df.to_excel(writer, sheet_name='Performance Metrics', index=False)
        else:
            pd.DataFrame([{"Message": "No performance data available."}]).to_excel(writer, sheet_name='Performance Metrics', index=False)
            
    output.seek(0)
    return output, "Excel export successful."

def export_data_to_json_zip(people_data, firms_data, employments_data, performance_data):
    """Export all data to a zip file containing separate JSON files"""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        if people_data:
            zip_file.writestr("people.json", json.dumps(people_data, indent=2, default=str))
        if firms_data:
            zip_file.writestr("firms.json", json.dumps(firms_data, indent=2, default=str))
        if employments_data:
            zip_file.writestr("employments.json", json.dumps(employments_data, indent=2, default=str))
        if performance_data:
            zip_file.writestr("performance_metrics.json", json.dumps(performance_data, indent=2, default=str))
            
    zip_buffer.seek(0)
    return zip_buffer, "JSON (ZIP) export successful."

# --- Data Analysis and Visualization ---
def get_firm_strategy_distribution():
    """Calculate and return the distribution of firm strategies"""
    strategies = [safe_get(firm, 'strategy') for firm in st.session_state.firms if safe_get(firm, 'strategy') != 'Unknown']
    strategy_counts = pd.Series(strategies).value_counts().reset_index()
    strategy_counts.columns = ['Strategy', 'Count']
    return strategy_counts

def get_people_expertise_distribution():
    """Calculate and return the distribution of people expertise"""
    expertise_list = []
    for person in st.session_state.people:
        expertise_str = safe_get(person, 'expertise')
        if expertise_str != 'Unknown':
            # Split by common delimiters
            expertises = [e.strip() for e in re.split(r'[,;/]', expertise_str) if e.strip()]
            expertise_list.extend(expertises)
    
    expertise_counts = pd.Series(expertise_list).value_counts().reset_index()
    expertise_counts.columns = ['Expertise', 'Count']
    return expertise_counts

def get_firms_by_aum_range():
    """Categorize firms by AUM ranges"""
    aum_ranges = {
        "< $1B": 0,
        "$1B - $5B": 0,
        "$5B - $20B": 0,
        "$20B - $50B": 0,
        "> $50B": 0
    }
    
    for firm in st.session_state.firms:
        aum_str = safe_get(firm, 'aum', '').upper()
        if 'B USD' in aum_str:
            try:
                aum_value = float(aum_str.replace('B USD', '').strip())
                if aum_value < 1:
                    aum_ranges["< $1B"] += 1
                elif 1 <= aum_value < 5:
                    aum_ranges["$1B - $5B"] += 1
                elif 5 <= aum_value < 20:
                    aum_ranges["$5B - $20B"] += 1
                elif 20 <= aum_value < 50:
                    aum_ranges["$20B - $50B"] += 1
                else:
                    aum_ranges["> $50B"] += 1
            except ValueError:
                continue # Ignore unparseable AUM values
    
    df = pd.DataFrame(aum_ranges.items(), columns=['AUM Range', 'Count'])
    # Sort for better visualization
    df['Sort Order'] = df['AUM Range'].map({
        "< $1B": 0,
        "$1B - $5B": 1,
        "$5B - $20B": 2,
        "$20B - $50B": 3,
        "> $50B": 4
    })
    df = df.sort_values('Sort Order').drop(columns=['Sort Order'])
    return df

def get_employments_over_time():
    """Aggregate employments by year"""
    employment_years = {}
    for emp in st.session_state.employments:
        if emp.get('start_date'):
            year = emp['start_date'].year
            employment_years[year] = employment_years.get(year, 0) + 1
    
    df = pd.DataFrame(employment_years.items(), columns=['Year', 'New Employments'])
    df = df.sort_values('Year')
    return df

def plot_firm_strategy_distribution(df):
    """Plot firm strategy distribution as a pie chart"""
    if df.empty:
        return "No data to display for Firm Strategy Distribution."
    fig = px.pie(df, values='Count', names='Strategy', title='Firm Strategy Distribution')
    return fig

def plot_people_expertise_distribution(df):
    """Plot people expertise distribution as a bar chart"""
    if df.empty:
        return "No data to display for People Expertise Distribution."
    fig = px.bar(df, x='Expertise', y='Count', title='People Expertise Distribution',
                 labels={'Expertise': 'Area of Expertise', 'Count': 'Number of People'})
    return fig

def plot_firms_by_aum_range(df):
    """Plot firms by AUM range as a bar chart"""
    if df.empty:
        return "No data to display for Firms by AUM Range."
    fig = px.bar(df, x='AUM Range', y='Count', title='Firms by AUM Range (USD)',
                 labels={'AUM Range': 'Assets Under Management', 'Count': 'Number of Firms'})
    return fig

def plot_employments_over_time(df):
    """Plot new employments over time as a line chart"""
    if df.empty:
        return "No data to display for Employments Over Time."
    fig = px.line(df, x='Year', y='New Employments', title='New Employments Over Time',
                  labels={'Year': 'Year', 'New Employments': 'Number of New Employments'})
    fig.update_traces(mode='lines+markers')
    return fig

def get_location_distribution(data_list, location_key):
    """Generic function to get location distribution"""
    locations = [safe_get(item, location_key) for item in data_list if safe_get(item, location_key) != 'Unknown']
    location_counts = pd.Series(locations).value_counts().reset_index()
    location_counts.columns = ['Location', 'Count']
    return location_counts

def plot_location_distribution(df, title_suffix):
    """Generic function to plot location distribution as a bar chart"""
    if df.empty:
        return f"No data to display for {title_suffix} Location Distribution."
    fig = px.bar(df, x='Location', y='Count', title=f'{title_suffix} Location Distribution',
                 labels={'Location': 'Location', 'Count': 'Number of Entities'})
    fig.update_layout(xaxis_tickangle=-45)
    return fig

# --- Main Streamlit App ---
def main():
    st.sidebar.title("Navigation")
    
    # User API Key input for Gemini
    if not GENAI_AVAILABLE:
        st.warning("`google.generativeai` library not found. Gemini features will be disabled.")
        gemini_api_key = None
    else:
        gemini_api_key = st.sidebar.text_input("Enter your Gemini API Key:", type="password", help="Required for AI features (e.g., data extraction). Get one at Google AI Studio.")
        
    gemini_model = None
    if gemini_api_key:
        with st.spinner("Configuring Gemini..."):
            gemini_model = setup_gemini(gemini_api_key)
            if gemini_model:
                st.sidebar.success("Gemini configured!")
            else:
                st.sidebar.error("Failed to configure Gemini. Check your API key or try again.")
    else:
        st.sidebar.info("Enter Gemini API Key to enable AI features.")

    # Initialize session state for all data
    initialize_session_state()
    
    # Sidebar for data management
    st.sidebar.header("Data Management")
    if st.sidebar.button("💾 Save All Data"):
        if save_data():
            st.sidebar.success("All data saved!")
        else:
            st.sidebar.error("Error saving data.")
    
    if st.sidebar.button("🔄 Reload Data"):
        initialize_session_state()
        st.sidebar.success("Data reloaded!")

    st.sidebar.header("Data Export")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Export to Excel"):
            excel_output, msg = export_data_to_excel(
                st.session_state.people, 
                st.session_state.firms, 
                st.session_state.employments,
                [m for f in st.session_state.firms for m in f.get('performance_metrics', [])]
            )
            if excel_output:
                st.download_button(
                    label="Download Excel",
                    data=excel_output,
                    file_name="hedge_fund_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="excel_download_button"
                )
                st.success(msg)
            else:
                st.error(msg)

    with col2:
        if st.button("Export to JSON (ZIP)"):
            json_zip_output, msg = export_data_to_json_zip(
                st.session_state.people, 
                st.session_state.firms, 
                st.session_state.employments,
                [m for f in st.session_state.firms for m in f.get('performance_metrics', [])]
            )
            if json_zip_output:
                st.download_button(
                    label="Download JSON (ZIP)",
                    data=json_zip_output,
                    file_name="hedge_fund_data.zip",
                    mime="application/zip",
                    key="json_zip_download_button"
                )
                st.success(msg)
            else:
                st.error(msg)

    st.sidebar.header("Global Search")
    st.session_state.global_search = st.sidebar.text_input("Search people, firms, etc.", st.session_state.global_search)
    
    if st.session_state.global_search:
        st.subheader(f"Search Results for '{st.session_state.global_search}'")
        search_people, search_firms, search_metrics = enhanced_global_search(st.session_state.global_search)
        
        if search_people:
            st.markdown("#### Matching People")
            for person in search_people:
                st.write(f"**{safe_get(person, 'name')}** - {safe_get(person, 'current_title')} at {safe_get(person, 'current_company_name')} ({safe_get(person, 'location')})")
        else:
            st.info("No matching people found.")
        
        if search_firms:
            st.markdown("#### Matching Firms")
            for firm in search_firms:
                st.write(f"**{safe_get(firm, 'name')}** - {safe_get(firm, 'strategy')} ({safe_get(firm, 'location')}, AUM: {safe_get(firm, 'aum')})")
        else:
            st.info("No matching firms found.")
            
        if search_metrics:
            st.markdown("#### Matching Performance Metrics")
            for metric in search_metrics:
                st.write(f"**{safe_get(metric, 'fund_name')}**: {safe_get(metric, 'metric_type')} - {safe_get(metric, 'value')} ({safe_get(metric, 'period')} {safe_get(metric, 'date')})")
        else:
            st.info("No matching metrics found.")
        
        # Clear selected entities to avoid confusion with search results
        st.session_state.selected_person_id = None
        st.session_state.selected_firm_id = None
        
    else:
        # Main content area
        st.title("🏢 Asian Hedge Fund Talent Mapper")

        st.markdown("""
        This application helps you map and analyze talent and firms within the Asian hedge fund industry.
        You can extract data from text, manage profiles, and visualize key insights.
        """)

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "👤 People", 
            "🏢 Firms", 
            "📜 Employment History", 
            "📊 Analytics", 
            "🧠 AI Extraction", 
            "🛠️ Debug & Utils"
        ])

        with tab1:
            st.header("👤 People Profiles")
            if st.button("➕ Add New Person"):
                st.session_state.show_add_person_modal = True

            people_df = pd.DataFrame(st.session_state.people)
            if not people_df.empty:
                # Add a 'Details' button column
                people_df['Actions'] = [f"Details_{i}" for i in range(len(people_df))]
                
                # Reorder columns to put Actions first for better visibility
                cols = ['Actions'] + [col for col in people_df.columns if col != 'Actions']
                people_df = people_df[cols]

                # Select columns to display
                display_cols = ['Actions', 'name', 'current_title', 'current_company_name', 'location', 'expertise', 'created_date']
                st.dataframe(people_df[display_cols], use_container_width=True, hide_index=True)
                
                # Handle details button clicks
                for i, row in people_df.iterrows():
                    if st.button("View/Edit Details", key=f"details_person_{row['id']}"):
                        st.session_state.selected_person_id = row['id']
                        st.session_state.current_view = 'person_detail'
                        st.rerun()

            else:
                st.info("No people profiles added yet.")

        with tab2:
            st.header("🏢 Firm Profiles")
            if st.button("➕ Add New Firm"):
                st.session_state.show_add_firm_modal = True

            firms_df = pd.DataFrame(st.session_state.firms)
            if not firms_df.empty:
                firms_df['Actions'] = [f"Details_{i}" for i in range(len(firms_df))]
                cols = ['Actions'] + [col for col in firms_df.columns if col != 'Actions']
                firms_df = firms_df[cols]

                display_cols = ['Actions', 'name', 'firm_type', 'location', 'aum', 'strategy', 'founded']
                st.dataframe(firms_df[display_cols], use_container_width=True, hide_index=True)
                
                for i, row in firms_df.iterrows():
                    if st.button("View/Edit Details", key=f"details_firm_{row['id']}"):
                        st.session_state.selected_firm_id = row['id']
                        st.session_state.current_view = 'firm_detail'
                        st.rerun()
            else:
                st.info("No firm profiles added yet.")

        with tab3:
            st.header("📜 Employment History")
            employments_df = pd.DataFrame(st.session_state.employments)
            if not employments_df.empty:
                # Merge with people data to show names
                merged_df = pd.merge(employments_df, 
                                     pd.DataFrame(st.session_state.people)[['id', 'name']], 
                                     left_on='person_id', 
                                     right_on='id', 
                                     suffixes=('', '_person_name'))
                merged_df['start_date'] = pd.to_datetime(merged_df['start_date']).dt.date
                merged_df['end_date'] = merged_df['end_date'].apply(lambda x: pd.to_datetime(x).dt.date if x else 'Present')
                
                display_cols = ['name', 'company_name', 'title', 'start_date', 'end_date', 'location', 'strategy']
                st.dataframe(merged_df[display_cols], use_container_width=True)
            else:
                st.info("No employment history added yet.")

        with tab4:
            st.header("📊 Data Analytics & Visualizations")
            
            st.subheader("Firm Strategy Distribution")
            firm_strategy_df = get_firm_strategy_distribution()
            fig_firm_strategy = plot_firm_strategy_distribution(firm_strategy_df)
            if isinstance(fig_firm_strategy, str):
                st.info(fig_firm_strategy)
            else:
                st.plotly_chart(fig_firm_strategy, use_container_width=True)
                
            st.subheader("People Expertise Distribution")
            people_expertise_df = get_people_expertise_distribution()
            fig_people_expertise = plot_people_expertise_distribution(people_expertise_df)
            if isinstance(fig_people_expertise, str):
                st.info(fig_people_expertise)
            else:
                st.plotly_chart(fig_people_expertise, use_container_width=True)
            
            st.subheader("Firms by AUM Range")
            firms_aum_df = get_firms_by_aum_range()
            fig_firms_aum = plot_firms_by_aum_range(firms_aum_df)
            if isinstance(fig_firms_aum, str):
                st.info(fig_firms_aum)
            else:
                st.plotly_chart(fig_firms_aum, use_container_width=True)
            
            st.subheader("New Employments Over Time")
            employments_time_df = get_employments_over_time()
            fig_employments_time = plot_employments_over_time(employments_time_df)
            if isinstance(fig_employments_time, str):
                st.info(fig_employments_time)
            else:
                st.plotly_chart(fig_employments_time, use_container_width=True)

            # Location distributions
            st.subheader("Location Distributions")
            
            person_location_df = get_location_distribution(st.session_state.people, 'location')
            fig_person_location = plot_location_distribution(person_location_df, 'Person')
            if isinstance(fig_person_location, str):
                st.info(fig_person_location)
            else:
                st.plotly_chart(fig_person_location, use_container_width=True)

            firm_location_df = get_location_distribution(st.session_state.firms, 'location')
            fig_firm_location = plot_location_distribution(firm_location_df, 'Firm')
            if isinstance(fig_firm_location, str):
                st.info(fig_firm_location)
            else:
                st.plotly_chart(fig_firm_location, use_container_width=True)

        with tab5:
            st.header("🧠 AI Data Extraction")
            st.markdown("""
            Paste raw text (e.g., news articles, LinkedIn profiles, deal announcements) below 
            to automatically extract financial professionals and performance data.
            """)
            
            if not gemini_model:
                st.warning("Please enter your Gemini API Key in the sidebar to enable AI extraction features.")
            
            extraction_text = st.text_area("Paste text for extraction:", height=300)
            
            if st.button("🚀 Start Extraction", disabled=not gemini_model or not extraction_text.strip()):
                if gemini_model and extraction_text.strip():
                    with st.spinner("Extracting data with AI... This may take a moment."):
                        st.session_state.background_processing['is_running'] = True
                        st.session_state.background_processing['status_message'] = "Starting AI extraction..."
                        st.session_state.background_processing['progress'] = 0
                        
                        # Use a thread for background processing
                        q = queue.Queue()
                        thread = threading.Thread(target=_run_extraction_in_background, args=(extraction_text, gemini_model, q))
                        thread.start()
                        
                        while thread.is_alive() or not q.empty():
                            try:
                                result = q.get(timeout=0.1)
                                if isinstance(result, dict) and 'progress' in result:
                                    st.session_state.background_processing.update(result)
                                else:
                                    st.session_state.background_processing['results'] = result
                                    break
                            except queue.Empty:
                                pass # No updates yet, keep spinning
                            time.sleep(0.1) # Update UI frequently

                        st.session_state.background_processing['is_running'] = False
                        st.session_state.background_processing['progress'] = 100
                        st.session_state.background_processing['status_message'] = "AI extraction complete!"
                        
                        extracted_people = st.session_state.background_processing['results'][0]
                        extracted_performance = st.session_state.background_processing['results'][1]
                        
                        st.success("Extraction Complete!")
                        st.markdown(f"**Extracted {len(extracted_people)} people and {len(extracted_performance)} performance metrics.**")
                        
                        if extracted_people:
                            st.subheader("Extracted People")
                            extracted_people_df = pd.DataFrame(extracted_people)
                            st.dataframe(extracted_people_df, use_container_width=True)
                            
                            # Option to add to database
                            if st.button("✅ Add Extracted People to Database"):
                                # Perform duplicate checking before adding
                                new_people_to_add = []
                                for person in extracted_people:
                                    existing = find_existing_person_strict(
                                        safe_get(person, 'name'), 
                                        safe_get(person, 'current_company')
                                    )
                                    if not existing:
                                        new_people_to_add.append(person)
                                    else:
                                        st.warning(f"Skipped potential duplicate: {safe_get(person, 'name')} at {safe_get(person, 'current_company')} (matches existing ID: {existing['id']})")
                                
                                if new_people_to_add:
                                    for person_data in new_people_to_add:
                                        add_person(person_data)
                                    st.success(f"Added {len(new_people_to_add)} unique people to the database!")
                                else:
                                    st.info("No unique people to add after duplicate check.")
                                st.rerun()

                        if extracted_performance:
                            st.subheader("Extracted Performance Metrics")
                            extracted_performance_df = pd.DataFrame(extracted_performance)
                            st.dataframe(extracted_performance_df, use_container_width=True)
                            
                            # Option to link to firms
                            st.markdown("##### Link Performance Metrics to Firms")
                            for metric in extracted_performance:
                                firm_name_for_metric = safe_get(metric, 'fund_name')
                                firm_options = [f.get('name') for f in st.session_state.firms]
                                
                                # Try to pre-select if there's a good match
                                initial_selection = None
                                if firm_name_for_metric:
                                    for opt in firm_options:
                                        if normalize_company(firm_name_for_metric) == normalize_company(opt):
                                            initial_selection = opt
                                            break
                                
                                selected_firm_name = st.selectbox(
                                    f"Link '{firm_name_for_metric}' performance to which firm?",
                                    options=['Select Firm'] + firm_options,
                                    index=firm_options.index(initial_selection) + 1 if initial_selection else 0,
                                    key=f"link_perf_{metric['id']}"
                                )
                                
                                if selected_firm_name and selected_firm_name != 'Select Firm':
                                    firm_to_link = get_firm_by_name(selected_firm_name)
                                    if firm_to_link:
                                        if st.button(f"Add Metric to {selected_firm_name}", key=f"add_metric_{metric['id']}"):
                                            if add_performance_metric_to_firm(firm_to_link['id'], 
                                                                               safe_get(metric, 'metric_type'), 
                                                                               safe_get(metric, 'value'), 
                                                                               safe_get(metric, 'period'), 
                                                                               safe_get(metric, 'date')):
                                                st.success(f"Added performance metric to {selected_firm_name}!")
                                                st.rerun()
                                            else:
                                                st.error(f"Failed to add metric to {selected_firm_name}.")
                                    else:
                                        st.warning(f"Could not find firm '{selected_firm_name}' for linking.")
                else:
                    st.error("AI extraction requires a valid Gemini API key and text input.")
            
            # Display background processing status
            if st.session_state.background_processing['is_running']:
                st.info(f"Background process: {st.session_state.background_processing['status_message']} "
                        f"({st.session_state.background_processing['progress']}%)")
            
        with tab6:
            st.header("🛠️ Debugging and Utilities")
            
            st.subheader("Duplicate Detection Test")
            if st.button("Run Duplicate Detection Test"):
                test_results = test_duplicate_detection()
                st.json(test_results)
            
            st.subheader("All Person Keys")
            if st.button("Show All Person Keys"):
                person_keys = debug_person_keys()
                st.json(person_keys)

            st.subheader("Internal Duplicate Check (Last Extraction)")
            # This would normally run on extracted data, but for debug, we'll use existing people
            if st.button("Check Existing People for Internal Duplicates"):
                unique, internal_duplicates = check_for_duplicates_in_extraction(st.session_state.people)
                if internal_duplicates:
                    st.warning(f"Found {len(internal_duplicates)} internal duplicates in existing people data:")
                    for dup in internal_duplicates:
                        st.write(f"- {safe_get(dup, 'name')} at {safe_get(dup, 'current_company_name')}")
                else:
                    st.info("No internal duplicates found in existing people data.")

            st.subheader("Force Asia Tagging Re-run")
            if st.button("Run Asia Tagging Now"):
                asia_people, asia_firms = tag_all_existing_profiles()
                st.success(f"Re-tagged profiles: {asia_people} Asia people, {asia_firms} Asia firms.")
                st.rerun() # Rerun to update display

            st.subheader("View Raw Data (Read-Only)")
            data_view_option = st.selectbox("Select Data to View:", ["People", "Firms", "Employments"])
            if data_view_option == "People":
                st.json(st.session_state.people)
            elif data_view_option == "Firms":
                st.json(st.session_state.firms)
            elif data_view_option == "Employments":
                st.json(st.session_state.employments)

    # Modals for adding/editing
    if st.session_state.show_add_person_modal:
        with st.expander("➕ Add New Person", expanded=True):
            st.markdown("Fill in the details for the new person.")
            with st.form("add_person_form"):
                new_person_name = st.text_input("Name*")
                new_person_title = st.text_input("Current Title")
                new_person_company = st.text_input("Current Company Name*")
                new_person_location = st.text_input("Location")
                new_person_email = st.text_input("Email")
                new_person_phone = st.text_input("Phone")
                new_person_education = st.text_input("Education")
                new_person_expertise = st.text_input("Expertise (comma-separated)")
                new_person_aum = st.text_input("AUM Managed (e.g., '1.5B USD')")
                new_person_strategy = st.text_input("Strategy")
                
                submitted = st.form_submit_button("Add Person")
                if submitted:
                    if not new_person_name or not new_person_company:
                        st.error("Name and Current Company Name are required fields.")
                    else:
                        existing = find_existing_person_strict(new_person_name, new_person_company)
                        if existing:
                            st.warning(f"Duplicate found: {safe_get(existing, 'name')} already exists at {safe_get(existing, 'current_company_name')}. ID: {existing['id']}")
                        else:
                            person_data = {
                                "name": new_person_name,
                                "current_title": new_person_title,
                                "current_company_name": new_person_company,
                                "location": new_person_location,
                                "email": new_person_email,
                                "phone": new_person_phone,
                                "education": new_person_education,
                                "expertise": new_person_expertise,
                                "aum_managed": new_person_aum,
                                "strategy": new_person_strategy
                            }
                            add_person(person_data)
                            st.success(f"Person '{new_person_name}' added successfully!")
                            st.session_state.show_add_person_modal = False
                            st.rerun()
            if st.button("Close Add Person", key="close_add_person_modal"):
                st.session_state.show_add_person_modal = False
                st.rerun()

    if st.session_state.show_add_firm_modal:
        with st.expander("➕ Add New Firm", expanded=True):
            st.markdown("Fill in the details for the new firm.")
            with st.form("add_firm_form"):
                new_firm_name = st.text_input("Firm Name*")
                new_firm_type = st.text_input("Firm Type (e.g., Hedge Fund, Asset Manager)")
                new_firm_location = st.text_input("Location")
                new_firm_headquarters = st.text_input("Headquarters")
                new_firm_aum = st.text_input("AUM (e.g., '50B USD')")
                new_firm_founded = st.number_input("Founded Year", min_value=1900, max_value=datetime.now().year, value=2000)
                new_firm_strategy = st.text_input("Strategy (e.g., 'Long/Short Equity', 'Multi-Strategy')")
                new_firm_website = st.text_input("Website")
                new_firm_description = st.text_area("Description")
                
                submitted = st.form_submit_button("Add Firm")
                if submitted:
                    if not new_firm_name:
                        st.error("Firm Name is required.")
                    else:
                        firm_data = {
                            "name": new_firm_name,
                            "firm_type": new_firm_type,
                            "location": new_firm_location,
                            "headquarters": new_firm_headquarters,
                            "aum": new_firm_aum,
                            "founded": new_firm_founded,
                            "strategy": new_firm_strategy,
                            "website": new_firm_website,
                            "description": new_firm_description
                        }
                        add_firm(firm_data)
                        st.success(f"Firm '{new_firm_name}' added successfully!")
                        st.session_state.show_add_firm_modal = False
                        st.rerun()
            if st.button("Close Add Firm", key="close_add_firm_modal"):
                st.session_state.show_add_firm_modal = False
                st.rerun()

    if st.session_state.current_view == 'person_detail' and st.session_state.selected_person_id:
        person = get_person_by_id(st.session_state.selected_person_id)
        if person:
            st.header(f"👤 Person Details: {safe_get(person, 'name')}")
            
            col_b1, col_b2, col_b3 = st.columns([0.15, 0.15, 0.7])
            with col_b1:
                if st.button("⬅️ Back to People"):
                    st.session_state.current_view = 'people'
                    st.session_state.selected_person_id = None
                    st.rerun()
            with col_b2:
                if st.button("✏️ Edit Person"):
                    st.session_state.edit_person_data = person
                    st.session_state.show_edit_person_modal = True
                    st.rerun()
            with col_b3:
                if st.button("🗑️ Delete Person", type="secondary"):
                    if st.warning("Are you sure you want to delete this person and all their employments?"):
                        if st.button("Confirm Delete", key="confirm_delete_person"):
                            delete_person(person['id'])
                            st.success("Person deleted.")
                            st.session_state.current_view = 'people'
                            st.session_state.selected_person_id = None
                            st.rerun()
            
            st.subheader("Basic Information")
            st.write(f"**Name:** {safe_get(person, 'name')}")
            st.write(f"**Current Title:** {safe_get(person, 'current_title')}")
            st.write(f"**Current Company:** {safe_get(person, 'current_company_name')}")
            st.write(f"**Location:** {safe_get(person, 'location')}")
            st.write(f"**Email:** {safe_get(person, 'email')}")
            st.write(f"**Phone:** {safe_get(person, 'phone')}")
            st.write(f"**Education:** {safe_get(person, 'education')}")
            st.write(f"**Expertise:** {safe_get(person, 'expertise')}")
            st.write(f"**AUM Managed:** {safe_get(person, 'aum_managed')}")
            st.write(f"**Strategy:** {safe_get(person, 'strategy')}")
            st.write(f"**Asia-based:** {'Yes' if person.get('is_asia_based', False) else 'No'}")
            st.write(f"**Created On:** {safe_get(person, 'created_date').split('T')[0]}")
            st.write(f"**Last Updated:** {safe_get(person, 'last_updated').split('T')[0]}")
            
            st.subheader("Employment History")
            person_employments = get_employments_by_person_id(person['id'])
            if person_employments:
                employments_df = pd.DataFrame(person_employments)
                employments_df['start_date'] = employments_df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
                employments_df['end_date'] = employments_df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else 'Present' if x is None else x)
                st.dataframe(employments_df[['company_name', 'title', 'start_date', 'end_date', 'location', 'strategy']], use_container_width=True)
                
                # Add employment editing/deleting
                st.markdown("##### Edit/Delete Employments")
                for emp in person_employments:
                    col_e1, col_e2, col_e3 = st.columns([0.6, 0.2, 0.2])
                    with col_e1:
                        st.write(f"{safe_get(emp, 'title')} at {safe_get(emp, 'company_name')} ({safe_get(emp, 'start_date')} - {safe_get(emp, 'end_date') if emp.get('end_date') else 'Present'})")
                    with col_e2:
                        # Simple edit form directly here
                        with st.popover(f"Edit {emp.get('company_name')}"):
                            edited_company = st.text_input("Company Name", value=safe_get(emp, 'company_name'), key=f"edit_emp_company_{emp['id']}")
                            edited_title = st.text_input("Title", value=safe_get(emp, 'title'), key=f"edit_emp_title_{emp['id']}")
                            edited_start = st.date_input("Start Date", value=emp['start_date'], key=f"edit_emp_start_{emp['id']}")
                            
                            # Handle "Present" for end_date
                            is_present = emp.get('end_date') is None
                            use_present = st.checkbox("Current (Present)", value=is_present, key=f"edit_emp_present_{emp['id']}")
                            edited_end = None
                            if not use_present:
                                edited_end = st.date_input("End Date", value=emp['end_date'] if emp.get('end_date') else date.today(), key=f"edit_emp_end_{emp['id']}")

                            edited_location = st.text_input("Location", value=safe_get(emp, 'location'), key=f"edit_emp_location_{emp['id']}")
                            edited_strategy = st.text_input("Strategy", value=safe_get(emp, 'strategy'), key=f"edit_emp_strategy_{emp['id']}")

                            if st.button("Save Changes", key=f"save_edit_emp_{emp['id']}"):
                                new_employment_data = {
                                    "company_name": edited_company,
                                    "title": edited_title,
                                    "start_date": edited_start,
                                    "end_date": None if use_present else edited_end,
                                    "location": edited_location,
                                    "strategy": edited_strategy
                                }
                                update_employment(emp['id'], new_employment_data)
                                st.success("Employment updated successfully!")
                                st.rerun()
                    with col_e3:
                        if st.button("Delete", key=f"delete_emp_{emp['id']}"):
                            delete_employment(emp['id'])
                            st.success("Employment deleted.")
                            st.rerun()

            if st.button("➕ Add New Employment for This Person"):
                st.session_state.show_add_employment_modal = True
                
            # Add employment modal
            if st.session_state.show_add_employment_modal:
                with st.expander(f"➕ Add New Employment for {safe_get(person, 'name')}", expanded=True):
                    with st.form("add_employment_form"):
                        new_emp_company = st.text_input("Company Name*")
                        new_emp_title = st.text_input("Title*")
                        new_emp_start_date = st.date_input("Start Date*", value=date.today())
                        
                        new_emp_is_present = st.checkbox("Current (Present)")
                        new_emp_end_date = None
                        if not new_emp_is_present:
                            new_emp_end_date = st.date_input("End Date")
                        
                        new_emp_location = st.text_input("Location")
                        new_emp_strategy = st.text_input("Strategy")
                        
                        submitted_emp = st.form_submit_button("Add Employment")
                        if submitted_emp:
                            if not new_emp_company or not new_emp_title or not new_emp_start_date:
                                st.error("Company Name, Title, and Start Date are required.")
                            else:
                                employment_data = {
                                    "person_id": person['id'],
                                    "company_name": new_emp_company,
                                    "title": new_emp_title,
                                    "start_date": new_emp_start_date,
                                    "end_date": new_emp_end_date if not new_emp_is_present else None,
                                    "location": new_emp_location,
                                    "strategy": new_emp_strategy
                                }
                                add_employment(employment_data)
                                st.success("Employment added successfully!")
                                st.session_state.show_add_employment_modal = False
                                st.rerun()
                    if st.button("Close Add Employment", key="close_add_employment_modal"):
                        st.session_state.show_add_employment_modal = False
                        st.rerun()

            st.subheader("Shared Work History")
            shared_history = get_shared_work_history(person['id'])
            if shared_history:
                shared_history_df = pd.DataFrame(shared_history)
                st.dataframe(shared_history_df[['colleague_name', 'shared_company', 'overlap_period', 'overlap_duration', 'connection_strength']], use_container_width=True)
            else:
                st.info(f"No shared work history found for {safe_get(person, 'name')}.")

        else:
            st.error("Person not found.")
            if st.button("Back to People"):
                st.session_state.current_view = 'people'
                st.session_state.selected_person_id = None
                st.rerun()

    if st.session_state.show_edit_person_modal and st.session_state.edit_person_data:
        person_to_edit = st.session_state.edit_person_data
        with st.expander(f"✏️ Edit Person: {safe_get(person_to_edit, 'name')}", expanded=True):
            st.markdown("Edit the details for this person.")
            with st.form("edit_person_form"):
                edited_name = st.text_input("Name*", value=safe_get(person_to_edit, 'name'))
                edited_title = st.text_input("Current Title", value=safe_get(person_to_edit, 'current_title'))
                edited_company = st.text_input("Current Company Name*", value=safe_get(person_to_edit, 'current_company_name'))
                edited_location = st.text_input("Location", value=safe_get(person_to_edit, 'location'))
                edited_email = st.text_input("Email", value=safe_get(person_to_edit, 'email'))
                edited_phone = st.text_input("Phone", value=safe_get(person_to_edit, 'phone'))
                edited_education = st.text_input("Education", value=safe_get(person_to_edit, 'education'))
                edited_expertise = st.text_input("Expertise (comma-separated)", value=safe_get(person_to_edit, 'expertise'))
                edited_aum = st.text_input("AUM Managed (e.g., '1.5B USD')", value=safe_get(person_to_edit, 'aum_managed'))
                edited_strategy = st.text_input("Strategy", value=safe_get(person_to_edit, 'strategy'))
                
                submitted_edit = st.form_submit_button("Save Changes")
                if submitted_edit:
                    if not edited_name or not edited_company:
                        st.error("Name and Current Company Name are required fields.")
                    else:
                        person_data_update = {
                            "name": edited_name,
                            "current_title": edited_title,
                            "current_company_name": edited_company,
                            "location": edited_location,
                            "email": edited_email,
                            "phone": edited_phone,
                            "education": edited_education,
                            "expertise": edited_expertise,
                            "aum_managed": edited_aum,
                            "strategy": edited_strategy
                        }
                        update_person(person_to_edit['id'], person_data_update)
                        st.success(f"Person '{edited_name}' updated successfully!")
                        st.session_state.show_edit_person_modal = False
                        st.session_state.edit_person_data = None
                        st.rerun()
            if st.button("Close Edit Person", key="close_edit_person_modal"):
                st.session_state.show_edit_person_modal = False
                st.session_state.edit_person_data = None
                st.rerun()

    if st.session_state.current_view == 'firm_detail' and st.session_state.selected_firm_id:
        firm = get_firm_by_id(st.session_state.selected_firm_id)
        if firm:
            st.header(f"🏢 Firm Details: {safe_get(firm, 'name')}")
            
            col_b1, col_b2, col_b3 = st.columns([0.15, 0.15, 0.7])
            with col_b1:
                if st.button("⬅️ Back to Firms"):
                    st.session_state.current_view = 'firms'
                    st.session_state.selected_firm_id = None
                    st.rerun()
            with col_b2:
                if st.button("✏️ Edit Firm"):
                    st.session_state.edit_firm_data = firm
                    st.session_state.show_edit_firm_modal = True
                    st.rerun()
            with col_b3:
                if st.button("🗑️ Delete Firm", type="secondary"):
                    if st.warning("Are you sure you want to delete this firm?"):
                        if st.button("Confirm Delete", key="confirm_delete_firm"):
                            delete_firm(firm['id'])
                            st.success("Firm deleted.")
                            st.session_state.current_view = 'firms'
                            st.session_state.selected_firm_id = None
                            st.rerun()
            
            st.subheader("Basic Information")
            st.write(f"**Firm Name:** {safe_get(firm, 'name')}")
            st.write(f"**Firm Type:** {safe_get(firm, 'firm_type')}")
            st.write(f"**Location:** {safe_get(firm, 'location')}")
            st.write(f"**Headquarters:** {safe_get(firm, 'headquarters')}")
            st.write(f"**AUM:** {safe_get(firm, 'aum')}")
            st.write(f"**Founded:** {safe_get(firm, 'founded')}")
            st.write(f"**Strategy:** {safe_get(firm, 'strategy')}")
            st.write(f"**Website:** {safe_get(firm, 'website')}")
            st.write(f"**Description:** {safe_get(firm, 'description')}")
            st.write(f"**Asia-based:** {'Yes' if firm.get('is_asia_based', False) else 'No'}")
            st.write(f"**Created On:** {safe_get(firm, 'created_date').split('T')[0]}")
            st.write(f"**Last Updated:** {safe_get(firm, 'last_updated').split('T')[0]}")
            
            st.subheader("Associated People")
            people_at_firm = get_people_by_firm(safe_get(firm, 'name'))
            if people_at_firm:
                people_at_firm_df = pd.DataFrame(people_at_firm)
                st.dataframe(people_at_firm_df[['name', 'current_title', 'location', 'expertise']], use_container_width=True)
            else:
                st.info("No people currently associated with this firm in the database.")
            
            st.subheader("Performance Metrics")
            firm_performance_metrics = get_performance_by_firm(firm['id'])
            if firm_performance_metrics:
                perf_df = pd.DataFrame(firm_performance_metrics)
                st.dataframe(perf_df[['type', 'value', 'period', 'date', 'timestamp']], use_container_width=True)
            else:
                st.info("No performance metrics recorded for this firm.")
        else:
            st.error("Firm not found.")
            if st.button("Back to Firms"):
                st.session_state.current_view = 'firms'
                st.session_state.selected_firm_id = None
                st.rerun()

    if st.session_state.show_edit_firm_modal and st.session_state.edit_firm_data:
        firm_to_edit = st.session_state.edit_firm_data
        with st.expander(f"✏️ Edit Firm: {safe_get(firm_to_edit, 'name')}", expanded=True):
            st.markdown("Edit the details for this firm.")
            with st.form("edit_firm_form"):
                edited_firm_name = st.text_input("Firm Name*", value=safe_get(firm_to_edit, 'name'))
                edited_firm_type = st.text_input("Firm Type (e.g., Hedge Fund, Asset Manager)", value=safe_get(firm_to_edit, 'firm_type'))
                edited_firm_location = st.text_input("Location", value=safe_get(firm_to_edit, 'location'))
                edited_firm_headquarters = st.text_input("Headquarters", value=safe_get(firm_to_edit, 'headquarters'))
                edited_firm_aum = st.text_input("AUM (e.g., '50B USD')", value=safe_get(firm_to_edit, 'aum'))
                edited_firm_founded = st.number_input("Founded Year", min_value=1900, max_value=datetime.now().year, value=safe_get(firm_to_edit, 'founded') if safe_get(firm_to_edit, 'founded') != 'Unknown' else 2000)
                edited_firm_strategy = st.text_input("Strategy (e.g., 'Long/Short Equity', 'Multi-Strategy')", value=safe_get(firm_to_edit, 'strategy'))
                edited_firm_website = st.text_input("Website", value=safe_get(firm_to_edit, 'website'))
                edited_firm_description = st.text_area("Description", value=safe_get(firm_to_edit, 'description'))
                
                submitted_edit_firm = st.form_submit_button("Save Changes")
                if submitted_edit_firm:
                    if not edited_firm_name:
                        st.error("Firm Name is required.")
                    else:
                        firm_data_update = {
                            "name": edited_firm_name,
                            "firm_type": edited_firm_type,
                            "location": edited_firm_location,
                            "headquarters": edited_firm_headquarters,
                            "aum": edited_firm_aum,
                            "founded": edited_firm_founded,
                            "strategy": edited_firm_strategy,
                            "website": edited_firm_website,
                            "description": edited_firm_description
                        }
                        update_firm(firm_to_edit['id'], firm_data_update)
                        st.success(f"Firm '{edited_firm_name}' updated successfully!")
                        st.session_state.show_edit_firm_modal = False
                        st.session_state.edit_firm_data = None
                        st.rerun()
            if st.button("Close Edit Firm", key="close_edit_firm_modal"):
                st.session_state.show_edit_firm_modal = False
                st.session_state.edit_firm_data = None
                st.rerun()

    st.markdown("---")
    st.subheader("💾 Database Operations (Direct Access)")
    
    # Text input for direct person/firm key checking
    st.markdown("#### Duplicate Check (Manual Input)")
    col_dup1, col_dup2 = st.columns(2)
    with col_dup1:
        check_name = st.text_input("Name to check:", key="check_name_input")
    with col_dup2:
        check_company = st.text_input("Company to check:", key="check_company_input")
    
    if st.button("Check for Duplicates"):
        if check_name and check_company:
            existing = find_existing_person_strict(check_name, check_company)
            if existing:
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
            height=300)
    else:
        st.info(f"No recent logs found for '{log_type}'.")

def get_recent_logs(log_type, num_lines):
    """Retrieve recent log entries from a specific log file."""
    log_file_map = {
        "user_actions": "extraction.log", # User actions are logged to extraction_logger
        "extraction": "extraction.log",
        "database": "hedge_fund_app.log", # Database errors are logged to main logger
        "api": "hedge_fund_app.log", # API errors are logged to main logger
        "main": "hedge_fund_app.log"
    }
    
    log_file_name = log_file_map.get(log_type, "hedge_fund_app.log")
    
    try:
        # Construct the full path to the log file (assuming current directory)
        log_path = Path(log_file_name)
        
        if not log_path.exists():
            return [f"Log file '{log_file_name}' does not exist."]
            
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            # Filter logs based on type for more specific display if needed, 
            # though current loggers largely separate them already.
            # For "user_actions" and "extraction", they both go to extraction.log
            # For "database" and "api", they both go to hedge_fund_app.log
            if log_type == "user_actions":
                filtered_lines = [line for line in lines if "USER:" in line]
            elif log_type == "extraction":
                filtered_lines = [line for line in lines if "EXTRACTION:" in line or "GEMINI_REQUEST" in line]
            elif log_type == "database":
                filtered_lines = [line for line in lines if "Save error:" in line or "Error loading data:" in line]
            elif log_type == "api":
                filtered_lines = [line for line in lines if "Gemini setup failed:" in line or "Extraction failed:" in line]
            else: # main
                filtered_lines = lines # Show all for main log
                
            return filtered_lines[-num_lines:]
            
    except Exception as e:
        logger.error(f"Error reading log file '{log_file_name}': {e}")
        return [f"Error reading log file: {e}"]

def _run_extraction_in_background(text, model, q):
    """Helper function to run extraction in a separate thread and put results in a queue."""
    try:
        people, performance = process_extraction_with_rate_limiting(text, model)
        q.put((people, performance))
    except Exception as e:
        q.put(e)

if __name__ == "__main__":
    main()
