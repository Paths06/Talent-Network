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

# --- FIXED: Enhanced JSON repair and cleanup function ---
def repair_json_response(json_text):
    """Repair common JSON formatting issues from AI responses"""
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
        json_text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_text)
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

# --- FIXED: Simple text input function for use in forms ---
def simple_text_input(field_name, current_value, context="", suggestions=None):
    """Simple text input that works inside forms - no buttons or complex logic"""
    placeholder = f"Enter {field_name.replace('_', ' ')}"
    if suggestions and len(suggestions) > 0:
        placeholder += f" (e.g., {suggestions[0]})"
    
    return st.text_input(
        f"{field_name.replace('_', ' ').title()}",
        value=current_value if current_value and current_value != 'Unknown' else "",
        placeholder=placeholder,
        key=f"{field_name}_{context}_{uuid.uuid4().hex[:8]}"
    )

# --- FIXED: Enhanced dynamic input that works outside forms ---
def handle_dynamic_input(field_name, current_value, table_name, context=""):
    """Enhanced dynamic input that prioritizes typing with suggestions"""
    import streamlit as st
    
    # Get existing options from database based on field and table
    existing_options = get_unique_values_from_session_state(table_name, field_name)
    
    # Remove None/empty values and sort
    existing_options = sorted([opt for opt in existing_options if opt and opt.strip() and opt != 'Unknown'])
    
    # Create unique key for input
    unique_key = f"{field_name}_input_{table_name}_{context}_{uuid.uuid4().hex[:8]}"
    
    # Primary text input with current value
    user_input = st.text_input(
        f"{field_name.replace('_', ' ').title()}",
        value=current_value if current_value and current_value != 'Unknown' else "",
        placeholder=f"Enter {field_name.replace('_', ' ')} or select from suggestions below",
        key=unique_key,
        help=f"Type directly or choose from {len(existing_options)} existing options below"
    )
    
    # Show existing options as clickable suggestions if there are any
    if existing_options and len(existing_options) > 0:
        st.caption(f"💡 **Suggestions** (click to use):")
        
        # Display suggestions in columns for better layout
        cols_per_row = 3
        suggestion_cols = st.columns(cols_per_row)
        
        for i, option in enumerate(existing_options[:9]):  # Show max 9 suggestions
            col_idx = i % cols_per_row
            with suggestion_cols[col_idx]:
                # Use a button that updates the input when clicked
                if st.button(f"📍 {option}", key=f"{unique_key}_suggestion_{i}", help=f"Use: {option}"):
                    # Return the selected suggestion
                    st.session_state[unique_key] = option
                    st.rerun()
        
        if len(existing_options) > 9:
            st.caption(f"... and {len(existing_options) - 9} more options available")
    
    # Return the user input (either typed or from session state if suggestion was clicked)
    return user_input.strip() if user_input else ""

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
                'utf-8',           # Standard UTF-8
                'utf-8-sig',       # UTF-8 with BOM
                'cp1252',          # Windows-1252 (common for Windows files)
                'latin1',          # ISO-8859-1
                'iso-8859-1',      # Alternative name for latin1
                'cp1251',          # Windows-1251 (Cyrillic)
                'ascii',           # Plain ASCII
                'utf-16',          # UTF-16 (less common)
                'utf-16le',        # UTF-16 Little Endian
                'utf-16be'         # UTF-16 Big Endian
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
            
        elif uploaded_file.type in ["application/pdf"] or uploaded_file.name.endswith('.pdf'):
            return False, "", "PDF files not supported. Please convert to .txt format.", None
            
        elif uploaded_file.type in ["application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"] or uploaded_file.name.endswith(('.doc', '.docx')):
            return False, "", "Word documents not supported. Please save as .txt format.", None
            
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

# Fallback function for compatibility
def load_file_content(uploaded_file):
    """Legacy function for compatibility - calls enhanced version"""
    try:
        success, content, error_msg, encoding_used = load_file_content_enhanced(uploaded_file)
        return success, content, error_msg
    except Exception as e:
        return False, "", f"Error loading file: {str(e)}"

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

def check_and_recover_stuck_processing():
    """Check for and recover from stuck background processing"""
    try:
        bg_proc = st.session_state.background_processing
        
        if not bg_proc['is_running']:
            return False
        
        # Check for timeout - 2000 RPM OPTIMIZED
        if 'last_activity' in bg_proc and bg_proc['last_activity']:
            time_since_activity = (datetime.now() - bg_proc['last_activity']).total_seconds()
            
            # 2000 RPM: Reduced timeout from 2 minutes to 90 seconds
            if time_since_activity > 90:  # 90 seconds timeout for 2000 RPM
                logger.warning(f"2000 RPM processing timeout detected after {time_since_activity}s inactivity")
                
                # Force stop and save any results
                total_people = bg_proc.get('saved_people', 0) + len(bg_proc['results']['people'])
                total_metrics = bg_proc.get('saved_performance', 0) + len(bg_proc['results']['performance'])
                
                bg_proc.update({
                    'is_running': False,
                    'status_message': f'2000 RPM auto-stopped due to timeout. Recovered {total_people} people, {total_metrics} metrics',
                    'errors': bg_proc['errors'] + ['2000 RPM processing timeout - automatically recovered']
                })
                
                return True  # Indicate recovery occurred
        
        return False
    except Exception as e:
        logger.error(f"Error in recovery check: {e}")
        return False

def emergency_stop_processing():
    """Emergency stop function for stuck processing"""
    try:
        bg_proc = st.session_state.background_processing
        
        if bg_proc['is_running']:
            logger.warning("Emergency stop triggered")
            
            # Force stop
            total_people = bg_proc.get('saved_people', 0) + len(bg_proc['results']['people'])
            total_metrics = bg_proc.get('saved_performance', 0) + len(bg_proc['results']['performance'])
            
            bg_proc.update({
                'is_running': False,
                'status_message': f'Emergency stop. Found {total_people} people, {total_metrics} metrics',
                'errors': bg_proc['errors'] + ['Emergency stop triggered']
            })
            
            return True
        
        return False
    except Exception as e:
        logger.error(f"Error in emergency stop: {e}")
        return False

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
                    safe_get(firm, 'firm_type', '')  # Added firm_type to search
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
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=15)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample data for demo purposes"
            }]
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Sarah Kim",
            "current_title": "Head of Research",
            "current_company_name": "Citadel Asia",
            "location": "Seoul",
            "email": "s.kim@citadel.com",
            "linkedin_profile_url": "https://linkedin.com/in/sarahkim",
            "phone": "+82-10-1234-5678",
            "education": "Seoul National University, MIT Sloan",
            "expertise": "Equity Research, ESG",
            "aum_managed": "800M USD",
            "strategy": "Equity Long/Short",
            "created_date": (datetime.now() - timedelta(days=7)).isoformat(),
            "last_updated": datetime.now().isoformat(),
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=7)).isoformat(),
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
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=20)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample firm data for demo purposes"
            }],
            "performance_metrics": [
                {
                    "id": str(uuid.uuid4()),
                    "metric_type": "sharpe",
                    "value": "1.8",
                    "period": "Current",
                    "date": "2025",
                    "additional_info": "Improved from 1.2",
                    "recorded_date": (datetime.now() - timedelta(days=3)).isoformat(),
                    "source_reliability": "sample_data"
                }
            ]
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Citadel Asia",
            "firm_type": "Hedge Fund",
            "location": "Hong Kong",
            "headquarters": "Chicago, USA",
            "aum": "45B USD",
            "founded": 1990,
            "strategy": "Multi-strategy, Market Making",
            "website": "https://citadel.com",
            "description": "Leading global hedge fund with growing Asian presence",
            "created_date": (datetime.now() - timedelta(days=5)).isoformat(),
            "last_updated": datetime.now().isoformat(),
            "extraction_history": [{
                "extraction_date": (datetime.now() - timedelta(days=5)).isoformat(),
                "source_type": "sample_data",
                "context_preview": "Sample firm data for demo purposes"
            }],
            "performance_metrics": []
        }
    ]
    
    # Create employment history with overlaps using FIXED date ranges
    sample_employments = []
    
    # Li Wei Chen's history (Hillhouse Capital) - WITH DATES
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
    
    # Akira Tanaka's history - WITH OVERLAPPING DATES at Goldman Sachs
    akira_id = sample_people[1]['id']
    sample_employments.extend([
        {
            "id": str(uuid.uuid4()),
            "person_id": akira_id,
            "company_name": "Goldman Sachs Asia",
            "title": "Associate",
            "start_date": date(2017, 6, 1),
            "end_date": date(2020, 12, 31),  # OVERLAPS with Li Wei Chen!
            "location": "Singapore",
            "strategy": "Fixed Income Trading",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat(),
            "extraction_context": {
                "extraction_date": (datetime.now() - timedelta(days=15)).isoformat(),
                "source_type": "sample_data"
            }
        },
        {
            "id": str(uuid.uuid4()),
            "person_id": akira_id,
            "company_name": "Millennium Partners Asia",
            "title": "Chief Investment Officer",
            "start_date": date(2021, 1, 15),
            "end_date": None,  # Current position
            "location": "Singapore",
            "strategy": "Multi-Strategy Quantitative",
            "created_date": (datetime.now() - timedelta(days=15)).isoformat(),
            "extraction_context": {
                "extraction_date": (datetime.now() - timedelta(days=15)).isoformat(),
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
    if 'edit_person_data' not in st.session_state:
        st.session_state.edit_person_data = None
    if 'edit_firm_data' not in st.session_state:
        st.session_state.edit_firm_data = None
    if 'pending_updates' not in st.session_state:
        st.session_state.pending_updates = []
    if 'show_update_review' not in st.session_state:
        st.session_state.show_update_review = False
    if 'global_search' not in st.session_state:
        st.session_state.global_search = ""
    
    # Pagination state
    if 'people_page' not in st.session_state:
        st.session_state.people_page = 0
    if 'firms_page' not in st.session_state:
        st.session_state.firms_page = 0
    if 'search_page' not in st.session_state:
        st.session_state.search_page = 0
    
    # File processing preferences
    if 'preprocessing_mode' not in st.session_state:
        st.session_state.preprocessing_mode = "balanced"
    if 'chunk_size_preference' not in st.session_state:
        st.session_state.chunk_size_preference = "auto"
    
    # Review system
    if 'enable_review_mode' not in st.session_state:
        st.session_state.enable_review_mode = True
    if 'pending_review_data' not in st.session_state:
        st.session_state.pending_review_data = []
    if 'review_start_time' not in st.session_state:
        st.session_state.review_start_time = None
    if 'show_review_interface' not in st.session_state:
        st.session_state.show_review_interface = False
    if 'auto_save_timeout' not in st.session_state:
        st.session_state.auto_save_timeout = 180
    
    # BACKGROUND PROCESSING STATE with timeout tracking
    if 'background_processing' not in st.session_state:
        st.session_state.background_processing = {
            'is_running': False,
            'progress': 0,
            'total_chunks': 0,
            'current_chunk': 0,
            'status_message': '',
            'results': {'people': [], 'performance': []},
            'errors': [],
            'start_time': None,
            'last_activity': None,
            'saved_people': 0,
            'saved_performance': 0,
            'processing_id': None,
            'failed_chunks': []
        }

# --- AI Setup with Enhanced Model Support ---
@st.cache_resource
def setup_gemini(api_key, model_id="gemini-1.5-flash"):
    """Setup Gemini AI model safely with model selection"""
    if not GENAI_AVAILABLE:
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_id)
        model.model_id = model_id
        return model
    except Exception as e:
        logger.error(f"AI setup failed: {e}")
        return None

def get_model_rate_limits(model_id):
    """Get rate limits for different Gemini models - UPDATED FOR ACTUAL TIER 1 LIMITS"""
    rate_limits = {
        # ACTUAL TIER 1 LIMITS - Much higher than before
        "gemini-1.5-flash": {"requests_per_minute": 2000, "delay": 0.03},  # 2000 RPM = 33/sec
        "gemini-1.5-flash-latest": {"requests_per_minute": 2000, "delay": 0.03},
        "gemini-1.5-flash-8b": {"requests_per_minute": 4000, "delay": 0.015},  # 4000 RPM = 67/sec
        "gemini-1.5-pro": {"requests_per_minute": 1000, "delay": 0.06},  # 1000 RPM = 17/sec
        "gemini-1.5-pro-latest": {"requests_per_minute": 1000, "delay": 0.06},
        # 2.0 models with conservative paid limits
        "gemini-2.0-flash-thinking-exp": {"requests_per_minute": 300, "delay": 0.2},
        "gemini-2.0-flash-exp": {"requests_per_minute": 300, "delay": 0.2},
        "gemini-2.5-flash-exp": {"requests_per_minute": 200, "delay": 0.3},
        "gemini-exp-1114": {"requests_per_minute": 100, "delay": 0.6},
        "gemini-exp-1121": {"requests_per_minute": 100, "delay": 0.6}
    }
    
    # Get base limits
    base_limits = rate_limits.get(model_id, {"requests_per_minute": 100, "delay": 0.6})
    
    # Special handling for 2.0+ models which can be unstable
    if any(version in model_id for version in ["2.0", "2.5"]):
        base_limits["delay"] = max(base_limits["delay"], 0.2)  # Minimum 0.2s delay
        base_limits["max_retries"] = 3
        base_limits["timeout_seconds"] = 90  # Longer timeout for newer models
        base_limits["experimental"] = True
    else:
        base_limits["timeout_seconds"] = 60
        base_limits["experimental"] = False
    
    # Add batching info for ultra-high rate models
    if base_limits["requests_per_minute"] >= 2000:
        base_limits["supports_batching"] = True
        base_limits["batch_size"] = 3  # Process 3 chunks with minimal delay
    elif base_limits["requests_per_minute"] >= 1000:
        base_limits["supports_batching"] = True
        base_limits["batch_size"] = 2  # Process 2 chunks with minimal delay
    else:
        base_limits["supports_batching"] = False
        base_limits["batch_size"] = 1
    
    return base_limits

@st.cache_data(ttl=3600)
def create_cached_context():
    """Create cached context for hedge fund extraction with optimized prompts"""
    return {
        "system_instructions": """You are an expert financial analyst specializing in the global financial industry. Your task is to meticulously analyze the following text and extract key intelligence about financial institutions, investment firms, and related organizations.

CORE EXTRACTION TARGETS:
1. PEOPLE: All individuals in professional contexts (current employees, new hires, departures, promotions, launches, appointments)
2. FIRMS: All types of financial institutions including hedge funds, investment banks, asset managers, family offices, private equity, sovereign wealth funds, pension funds, central banks, fintech companies, financial news organizations, rating agencies
3. PERFORMANCE DATA: Returns, risk metrics, AUM figures, fund performance, benchmarks
4. MOVEMENTS: Job changes, fund launches, firm transitions, strategic shifts

IMPORTANT: Accurately classify the type of financial institution based on context clues. Do not default to "Hedge Fund" for all firms.""",
        
        "output_format": """{
  "geographic_focus": "Primary geographic region or 'Global' if multiple regions",
  "people": [
    {
      "name": "Full Legal Name",
      "current_company": "Current Firm Name",
      "current_title": "Exact Job Title",
      "previous_company": "Former Firm (if mentioned)",
      "movement_type": "hire|promotion|launch|departure|appointment",
      "location": "City, Country or Region",
      "experience_years": "Number of years experience (if mentioned)",
      "expertise": "Area of specialization",
      "seniority_level": "junior|mid|senior|c_suite"
    }
  ],
  "firms": [
    {
      "name": "Exact Firm Name",
      "firm_type": "Hedge Fund|Investment Bank|Asset Manager|Private Equity|Family Office|Pension Fund|Central Bank|Sovereign Wealth Fund|Fintech Company|News Organization|Rating Agency|Financial Services|Other",
      "strategy": "Investment strategy or business line",
      "geographic_focus": "Geographic focus if mentioned",
      "aum": "Assets under management (numeric only)",
      "status": "launching|expanding|closing|operating|acquired"
    }
  ],
  "performance": [
    {
      "fund_name": "Exact Fund/Firm Name",
      "metric_type": "return|irr|sharpe|information_ratio|drawdown|alpha|beta|volatility|aum|tracking_error|correlation",
      "value": "numeric_value_only_no_units",
      "period": "YTD|Q1|Q2|Q3|Q4|1Y|3Y|5Y|ITD|Monthly|Current",
      "date": "YYYY or YYYY-MM",
      "benchmark": "Benchmark name if comparison mentioned",
      "additional_info": "Units, context, fund type, net/gross specification"
    }
  ]
}"""
    }

def build_extraction_prompt_with_cache(newsletter_text, cached_context):
    """Build enhanced extraction prompt using cached context"""
    prompt = f"""
{cached_context['system_instructions']}

TARGET NEWSLETTER FOR ANALYSIS:
{newsletter_text}

REQUIRED OUTPUT FORMAT:
{cached_context['output_format']}

Return ONLY the JSON output with geographic_focus, people, firms, and performance arrays populated with verified data."""
    
    return prompt

def preprocess_newsletter_text(text, mode="balanced"):
    """Enhanced preprocessing with configurable modes"""
    import re
    
    if mode == "none":
        return text
    
    original_size = len(text)
    
    # Basic cleaning
    if mode in ["balanced", "aggressive"]:
        # Remove email headers
        email_header_patterns = [
            r'From:\s*.*?\n', r'To:\s*.*?\n', r'Sent:\s*.*?\n',
            r'Subject:\s*.*?\n', r'Date:\s*.*?\n'
        ]
        for pattern in email_header_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove URLs
        url_patterns = [
            r'https?://[^\s<>"{}|\\^`\[\]]+',
            r'<https?://[^>]+>'
        ]
        for pattern in url_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # Aggressive cleaning
    if mode == "aggressive":
        # Remove HTML and disclaimers
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
        
        # Remove legal disclaimers
        disclaimer_patterns = [
            r'This message is confidential.*?(?=\n\n|\Z)',
            r'©.*?All rights reserved.*?(?=\n\n|\Z)',
            r'Unsubscribe.*?(?=\n\n|\Z)'
        ]
        for pattern in disclaimer_patterns:
            text = re.sub(pattern, '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Clean up whitespace
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    text = text.strip()
    
    final_size = len(text)
    reduction_pct = ((original_size - final_size) / original_size) * 100 if original_size > 0 else 0
    
    logger.info(f"Text preprocessing complete. Mode: {mode}, Reduction: {reduction_pct:.1f}%")
    
    return text

# --- FIXED: Enhanced single chunk extraction with better JSON handling ---
def extract_single_chunk_safe(text, model):
    """FIXED: Enhanced single chunk extraction with improved JSON parsing and error handling"""
    try:
        cached_context = create_cached_context()
        prompt = build_extraction_prompt_with_cache(text, cached_context)
        
        # Get model-specific timeout
        rate_limits = get_model_rate_limits(model.model_id)
        timeout = rate_limits.get('timeout_seconds', 30)
        
        # Set generation config for more reliable responses
        generation_config = {
            'temperature': 0.1,  # Lower temperature for more consistent results
            'top_p': 0.8,
            'top_k': 40,
            'max_output_tokens': 8192,
        }
        
        # Add timeout for Flash 2.0
        if "2.0-flash" in model.model_id:
            generation_config['candidate_count'] = 1  # Single candidate for stability
        
        logger.info(f"Extracting with model {model.model_id}, timeout: {timeout}s, chunk size: {len(text)}")
        
        # Generate content with timeout handling
        response = None
        try:
            response = model.generate_content(prompt, generation_config=generation_config)
            
            if not response or not response.text:
                logger.warning("Empty response from AI model")
                return [], []
            
            # Check for content filtering
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                if hasattr(response.prompt_feedback, 'block_reason'):
                    logger.warning(f"Content blocked: {response.prompt_feedback.block_reason}")
                    return [], []
            
        except Exception as generation_error:
            logger.error(f"Generation failed: {generation_error}")
            if "timeout" in str(generation_error).lower():
                raise TimeoutError(f"Model request timed out after {timeout}s")
            else:
                raise generation_error
        
        # FIXED: Enhanced JSON parsing with repair logic
        response_text = response.text.strip()
        logger.debug(f"Raw response length: {len(response_text)} chars")
        
        # Try to find JSON boundaries
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            logger.warning("No valid JSON boundaries found in AI response")
            # Try regex approach for partial JSON
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_text = json_match.group()
                logger.info("Found JSON using regex fallback")
            else:
                logger.error("No JSON found even with regex")
                return [], []
        else:
            json_text = response_text[json_start:json_end]
        
        # First attempt: Parse as-is
        result = None
        try:
            result = json.loads(json_text)
            logger.info("JSON parsed successfully on first attempt")
        except json.JSONDecodeError as first_error:
            logger.warning(f"Initial JSON parsing failed: {first_error}")
            logger.warning(f"Error position: line {getattr(first_error, 'lineno', 'unknown')}, column {getattr(first_error, 'colno', 'unknown')}")
            
            # Second attempt: Try repair and parse
            try:
                repaired_json = repair_json_response(json_text)
                result = json.loads(repaired_json)
                logger.info("JSON parsed successfully after repair")
            except json.JSONDecodeError as repair_error:
                logger.error(f"JSON repair also failed: {repair_error}")
                logger.error(f"Original JSON snippet: {json_text[:500]}...")
                logger.error(f"Repaired JSON snippet: {repaired_json[:500] if 'repaired_json' in locals() else 'N/A'}...")
                
                # Third attempt: Try to extract partial data
                try:
                    # Try to find at least the people/firms/performance arrays
                    partial_result = {"people": [], "firms": [], "performance": []}
                    
                    # Look for people array
                    people_match = re.search(r'"people"\s*:\s*\[(.*?)\]', json_text, re.DOTALL)
                    if people_match:
                        try:
                            people_json = '[' + people_match.group(1) + ']'
                            people_data = json.loads(people_json)
                            partial_result["people"] = people_data
                            logger.info(f"Extracted {len(people_data)} people from partial JSON")
                        except:
                            pass
                    
                    # Look for performance array
                    perf_match = re.search(r'"performance"\s*:\s*\[(.*?)\]', json_text, re.DOTALL)
                    if perf_match:
                        try:
                            perf_json = '[' + perf_match.group(1) + ']'
                            perf_data = json.loads(perf_json)
                            partial_result["performance"] = perf_data
                            logger.info(f"Extracted {len(perf_data)} performance metrics from partial JSON")
                        except:
                            pass
                    
                    if partial_result["people"] or partial_result["performance"]:
                        result = partial_result
                        logger.info("Successfully extracted partial data despite JSON errors")
                    else:
                        logger.error("No usable data could be extracted")
                        return [], []
                        
                except Exception as partial_error:
                    logger.error(f"Partial extraction also failed: {partial_error}")
                    return [], []
        
        if not result:
            logger.error("All JSON parsing attempts failed")
            return [], []
        
        people = result.get('people', [])
        performance = result.get('performance', [])
        firms = result.get('firms', [])  # Extract firms data for validation
        
        # Enhanced validation with better logging
        valid_people = []
        valid_performance = []
        
        for i, p in enumerate(people):
            try:
                name = safe_get(p, 'name', '').strip()
                current_company = safe_get(p, 'current_company', '').strip()
                
                if (name and current_company and 
                    name.lower() not in ['full name', 'full legal name', 'name', 'unknown'] and
                    current_company.lower() not in ['company', 'current firm name', 'firm name', 'unknown'] and
                    len(name) > 2 and len(current_company) > 2):
                    
                    # Map to legacy structure for compatibility
                    legacy_person = {
                        'name': name,
                        'company': current_company,
                        'title': safe_get(p, 'current_title', 'Unknown'),
                        'movement_type': safe_get(p, 'movement_type', 'Unknown'),
                        'location': safe_get(p, 'location', 'Unknown'),
                        'current_company': current_company,
                        'current_title': safe_get(p, 'current_title', 'Unknown'),
                        'previous_company': safe_get(p, 'previous_company', 'Unknown'),
                        'experience_years': safe_get(p, 'experience_years', 'Unknown'),
                        'expertise': safe_get(p, 'expertise', 'Unknown'),
                        'seniority_level': safe_get(p, 'seniority_level', 'Unknown')
                    }
                    valid_people.append(legacy_person)
                else:
                    logger.debug(f"Person {i} failed validation: name='{name}', company='{current_company}'")
            except Exception as person_error:
                logger.warning(f"Error processing person {i}: {person_error}")
                continue
        
        for i, p in enumerate(performance):
            try:
                fund_name = safe_get(p, 'fund_name', '').strip()
                metric_type = safe_get(p, 'metric_type', '').strip()
                value = safe_get(p, 'value', '').strip()
                
                if (fund_name and metric_type and value and
                    fund_name.lower() not in ['fund name', 'exact fund name', 'unknown'] and
                    metric_type.lower() not in ['metric', 'metric type', 'unknown'] and
                    value.lower() not in ['value', 'numeric value only', 'unknown']):
                    valid_performance.append(p)
                else:
                    logger.debug(f"Performance {i} failed validation: fund='{fund_name}', metric='{metric_type}', value='{value}'")
            except Exception as perf_error:
                logger.warning(f"Error processing performance {i}: {perf_error}")
                continue
        
        logger.info(f"Extraction complete: {len(valid_people)} people, {len(valid_performance)} performance metrics")
        return valid_people, valid_performance
        
    except TimeoutError as timeout_error:
        logger.error(f"Extraction timed out: {timeout_error}")
        raise timeout_error
    except json.JSONDecodeError as json_error:
        logger.error(f"All JSON parsing methods failed: {json_error}")
        return [], []
    except Exception as e:
        logger.error(f"Enhanced extraction failed: {e}")
        return [], []

# --- BACKGROUND PROCESSING FUNCTIONS ---
def start_background_extraction(text, model, preprocessing_mode, chunk_size_mode):
    """OPTIMIZED: Start background extraction with 2000 RPM Flash 1.5 optimizations"""
    st.session_state.background_processing = {
        'is_running': True,
        'progress': 0,
        'total_chunks': 1,
        'current_chunk': 0,
        'status_message': 'Starting extraction...',
        'results': {'people': [], 'performance': []},
        'errors': [],
        'start_time': datetime.now(),
        'last_activity': datetime.now(),
        'saved_people': 0,
        'saved_performance': 0,
        'processing_id': str(uuid.uuid4())
    }
    
    try:
        # Preprocess text
        cleaned_text = preprocess_newsletter_text(text, preprocessing_mode)
        
        # 2000 RPM OPTIMIZED: Even larger chunk sizes for ultra-fast processing
        flash_2000_chunk_sizes = {
            "small": 40000,      # Was 25K, now 40K for 2000 RPM
            "medium": 80000,     # Was 50K, now 80K
            "large": 120000,     # Was 75K, now 120K
            "xlarge": 150000,    # Was 100K, now 150K
            "auto": min(max(len(cleaned_text) // 15, 60000), 120000),  # Much more aggressive auto-sizing
            "single": len(cleaned_text)  # Process entire text as single chunk if possible
        }
        
        # Use 2000 RPM optimized sizes
        chunk_size = flash_2000_chunk_sizes.get(chunk_size_mode, 80000)
        
        # For single chunk mode with 2000 RPM, try to process everything at once
        if chunk_size_mode == "single" and len(cleaned_text) <= 200000:  # Up to 200K chars in single chunk
            chunks = [cleaned_text]
        else:
            # Create optimized chunks for batching
            chunks = []
            current_pos = 0
            while current_pos < len(cleaned_text):
                end_pos = min(current_pos + chunk_size, len(cleaned_text))
                if end_pos < len(cleaned_text):
                    # Look for natural break points in larger window
                    search_start = max(end_pos - 2000, current_pos)  # Much larger search window
                    para_break = cleaned_text.rfind('\n\n', search_start, end_pos)
                    if para_break > current_pos:
                        end_pos = para_break + 2
                
                chunk = cleaned_text[current_pos:end_pos].strip()
                if len(chunk) > 1000:  # Minimum chunk size for 2000 RPM
                    chunks.append(chunk)
                current_pos = end_pos
        
        st.session_state.background_processing['total_chunks'] = len(chunks)
        logger.info(f"2000 RPM OPTIMIZED: {len(chunks)} chunks, avg size: {len(cleaned_text)//len(chunks) if chunks else 0}, model: {model.model_id}")
        
        # 2000 RPM PROCESSING: Ultra-fast with batching
        rate_limits = get_model_rate_limits(model.model_id)
        delay = rate_limits['delay']
        supports_batching = rate_limits.get('supports_batching', False)
        batch_size = rate_limits.get('batch_size', 1)
        
        all_people = []
        all_performance = []
        failed_chunks = []
        consecutive_failures = 0
        
        # Process chunks with batching for high-rate models
        i = 0
        while i < len(chunks):
            # Check if processing should stop
            if not st.session_state.background_processing['is_running']:
                logger.info(f"Processing stopped by user at chunk {i+1}")
                break
            
            # Update activity timestamp
            st.session_state.background_processing['last_activity'] = datetime.now()
            
            # Determine batch size for this iteration
            if supports_batching and consecutive_failures == 0:
                current_batch_size = min(batch_size, len(chunks) - i)
                chunk_batch = chunks[i:i + current_batch_size]
            else:
                current_batch_size = 1
                chunk_batch = [chunks[i]]
            
            # Update progress
            st.session_state.background_processing.update({
                'current_chunk': i + current_batch_size,
                'progress': int(((i + current_batch_size) / len(chunks)) * 100),
                'status_message': f'Processing batch {i//batch_size + 1} (chunks {i + 1}-{i + current_batch_size}/{len(chunks)})... (2000 RPM)'
            })
            
            # Process batch
            batch_people = []
            batch_performance = []
            batch_errors = 0
            
            for j, chunk in enumerate(chunk_batch):
                try:
                    logger.info(f"Processing chunk {i+j+1}/{len(chunks)} (size: {len(chunk)} chars, delay: {delay}s)")
                    people, performance = extract_single_chunk_safe(chunk, model)
                    
                    if people or performance:
                        batch_people.extend(people)
                        batch_performance.extend(performance)
                        logger.info(f"Chunk {i+j+1} success: {len(people)} people, {len(performance)} metrics")
                    else:
                        batch_errors += 1
                        logger.warning(f"Chunk {i+j+1}: No results found")
                    
                    # 2000 RPM: Very minimal delay between chunks in batch
                    if j < len(chunk_batch) - 1:  # Don't delay after last chunk in batch
                        time.sleep(delay)
                        
                except Exception as e:
                    batch_errors += 1
                    error_msg = f"Chunk {i+j+1} failed: {str(e)}"
                    st.session_state.background_processing['errors'].append(error_msg)
                    failed_chunks.append(i+j+1)
                    logger.error(error_msg)
            
            # Process batch results
            if batch_people or batch_performance:
                all_people.extend(batch_people)
                all_performance.extend(batch_performance)
                consecutive_failures = 0  # Reset failure counter on success
                
                # 2000 RPM OPTIMIZED: Very frequent auto-saves (every 2 batches or 10+ items)
                if (i // batch_size + 1) % 2 == 0 or len(all_people) >= 10:
                    if not st.session_state.enable_review_mode:
                        # Direct save
                        saved_p, saved_perf = save_approved_extractions(all_people, all_performance)
                        st.session_state.background_processing['saved_people'] += saved_p
                        st.session_state.background_processing['saved_performance'] += saved_perf
                        
                        # Clear saved items from memory
                        all_people = []
                        all_performance = []
                        
                        logger.info(f"2000 RPM auto-saved {saved_p} people, {saved_perf} metrics at batch {i//batch_size + 1}")
            else:
                consecutive_failures += batch_errors
            
            # Store results
            st.session_state.background_processing['results'] = {
                'people': all_people,
                'performance': all_performance
            }
            
            # 2000 RPM: Minimal inter-batch delays, light backoff on failures
            if consecutive_failures >= 3:
                actual_delay = min(delay * (1.1 ** consecutive_failures), delay * 2)  # Very light backoff
                logger.warning(f"Multiple failures detected, increasing delay to {actual_delay}s")
            else:
                actual_delay = delay * 2  # Small inter-batch delay even on success
            
            # Inter-batch delay
            if i + current_batch_size < len(chunks):
                time.sleep(actual_delay)
            
            # Move to next batch
            i += current_batch_size
            
            # Emergency stop if too many consecutive failures (very high tolerance for 2000 RPM)
            if consecutive_failures >= 20:  # Was 15, now 20 for ultra-fast processing
                logger.error(f"Too many consecutive failures ({consecutive_failures}), stopping processing")
                st.session_state.background_processing['errors'].append(f"Stopped due to {consecutive_failures} consecutive failures")
                break
        
        # Final save of any remaining items
        if all_people or all_performance:
            if st.session_state.enable_review_mode:
                # Add to review queue
                add_to_review_queue(all_people, all_performance, f"2000 RPM Flash Extraction ({len(chunks)} chunks)")
                logger.info(f"Added {len(all_people)} people, {len(all_performance)} metrics to review queue")
            else:
                # Direct save
                saved_p, saved_perf = save_approved_extractions(all_people, all_performance)
                st.session_state.background_processing['saved_people'] += saved_p
                st.session_state.background_processing['saved_performance'] += saved_perf
                logger.info(f"Final 2000 RPM save: {saved_p} people, {saved_perf} metrics")
        
        # Complete processing
        total_found = st.session_state.background_processing['saved_people'] + len(st.session_state.background_processing['results']['people'])
        total_metrics = st.session_state.background_processing['saved_performance'] + len(st.session_state.background_processing['results']['performance'])
        
        processing_time = (datetime.now() - st.session_state.background_processing['start_time']).total_seconds()
        chunks_per_minute = (len(chunks) / processing_time) * 60 if processing_time > 0 else 0
        
        st.session_state.background_processing.update({
            'is_running': False,
            'status_message': f'2000 RPM Complete! Found {total_found} people, {total_metrics} metrics in {processing_time:.1f}s ({chunks_per_minute:.1f} chunks/min)',
            'progress': 100,
            'failed_chunks': failed_chunks
        })
        
        logger.info(f"2000 RPM background extraction completed: {total_found} people, {total_metrics} metrics, {len(failed_chunks)} failed chunks, {processing_time:.1f}s total, {chunks_per_minute:.1f} chunks/min")
        
    except Exception as e:
        st.session_state.background_processing.update({
            'is_running': False,
            'status_message': f'Failed: {str(e)}',
            'errors': [str(e)]
        })
        logger.error(f"2000 RPM background extraction failed: {e}")

def display_background_processing_widget():
    """SIMPLIFIED: Display minimal background processing widget in sidebar only"""
    bg_proc = st.session_state.background_processing
    
    if not bg_proc['is_running'] and bg_proc['progress'] == 0:
        return
    
    # Check for timeout/stuck processing - 2000 RPM OPTIMIZED
    if bg_proc['is_running'] and 'last_activity' in bg_proc:
        time_since_activity = (datetime.now() - bg_proc['last_activity']).total_seconds()
        # 2000 RPM: Reduced timeout from 2 minutes to 90 seconds (ultra-fast processing expected)
        if time_since_activity > 90:  # 90 seconds timeout for 2000 RPM
            st.session_state.background_processing['is_running'] = False
            st.session_state.background_processing['status_message'] = '2000 RPM timeout - Processing stopped automatically'
            st.session_state.background_processing['errors'].append('2000 RPM processing timeout after 90 seconds of inactivity')
    
    # SIMPLIFIED WIDGET - Only show in sidebar
    with st.container():
        if bg_proc['is_running']:
            progress = bg_proc['progress']
            current = bg_proc['current_chunk']
            total = bg_proc['total_chunks']
            
            # Minimal progress display
            st.progress(progress / 100, text=f"⚡ 2000 RPM Extracting: {progress}%")
            
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"Chunk {current}/{total}")
            with col2:
                people_count = bg_proc.get('saved_people', 0) + len(bg_proc['results']['people'])
                metrics_count = bg_proc.get('saved_performance', 0) + len(bg_proc['results']['performance'])
                st.caption(f"Found: {people_count}P/{metrics_count}M")
            
            # Simple stop button
            if st.button("⏹️ Stop", key="stop_background_simple", use_container_width=True):
                st.session_state.background_processing['is_running'] = False
                st.rerun()
        
        else:
            # Completion status - very minimal
            results = bg_proc['results']
            
            if results['people'] or results['performance'] or bg_proc.get('saved_people', 0) > 0:
                total_people = bg_proc.get('saved_people', 0) + len(results['people'])
                total_metrics = bg_proc.get('saved_performance', 0) + len(results['performance'])
                
                st.success(f"⚡ 2000 RPM Complete: {total_people}P/{total_metrics}M")
                
                col1, col2 = st.columns(2)
                with col1:
                    if results['people'] or results['performance']:  # Only show if there's unsaved data
                        if st.button("📋 Review", key="review_results_simple", use_container_width=True):
                            source_info = f"Background Extraction"
                            add_to_review_queue(results['people'], results['performance'], source_info)
                            # Clear results after adding to review
                            st.session_state.background_processing['results'] = {'people': [], 'performance': []}
                            st.rerun()
                with col2:
                    if st.button("❌ Clear", key="dismiss_results_simple", use_container_width=True):
                        st.session_state.background_processing = {
                            'is_running': False, 'progress': 0, 'total_chunks': 0, 'current_chunk': 0,
                            'status_message': '', 'results': {'people': [], 'performance': []}, 'errors': [], 'start_time': None
                        }
                        st.rerun()
            
            elif bg_proc.get('errors'):
                st.error("❌ 2000 RPM extraction failed")
                if st.button("❌ Clear", key="dismiss_error_simple", use_container_width=True):
                    st.session_state.background_processing = {
                        'is_running': False, 'progress': 0, 'total_chunks': 0, 'current_chunk': 0,
                        'status_message': '', 'results': {'people': [], 'performance': []}, 'errors': [], 'start_time': None
                    }
                    st.rerun()

# --- ASIA & HEDGE FUND PRIORITIZATION HELPERS ---
def is_asia_location(location):
    """Check if location is in Asia"""
    if not location or location == 'Unknown':
        return False
    
    asia_keywords = [
        'hong kong', 'singapore', 'tokyo', 'seoul', 'bangkok', 'mumbai', 'delhi', 
        'shanghai', 'beijing', 'shenzhen', 'guangzhou', 'taipei', 'kuala lumpur',
        'jakarta', 'manila', 'ho chi minh', 'hanoi', 'macau', 'osaka', 'kyoto',
        'busan', 'china', 'japan', 'korea', 'india', 'thailand', 'malaysia',
        'indonesia', 'philippines', 'vietnam', 'taiwan', 'asia', 'asian'
    ]
    
    location_lower = location.lower()
    return any(keyword in location_lower for keyword in asia_keywords)

def is_hedge_fund_firm(firm):
    """Check if firm is a hedge fund"""
    firm_type = safe_get(firm, 'firm_type', '').lower()
    firm_name = safe_get(firm, 'name', '').lower()
    strategy = safe_get(firm, 'strategy', '').lower()
    
    # Direct type check
    if 'hedge fund' in firm_type:
        return True
    
    # Name-based check
    hedge_fund_indicators = ['hedge', 'fund', 'capital', 'investment', 'management', 'partners']
    name_matches = sum(1 for indicator in hedge_fund_indicators if indicator in firm_name)
    
    # Strategy-based check
    strategy_indicators = ['long/short', 'equity', 'fixed income', 'macro', 'distressed', 'event driven']
    strategy_matches = any(indicator in strategy for indicator in strategy_indicators)
    
    return name_matches >= 2 or strategy_matches

def get_priority_score(item, item_type):
    """Calculate priority score for sorting (higher = more priority)"""
    score = 0
    
    if item_type == 'person':
        # Asia location bonus
        if is_asia_location(safe_get(item, 'location')):
            score += 100
        
        # Check if person works at hedge fund
        company_name = safe_get(item, 'current_company_name', '').lower()
        if any(keyword in company_name for keyword in ['hedge', 'fund', 'capital', 'investment']):
            score += 50
    
    elif item_type == 'firm':
        # Asia location bonus
        if is_asia_location(safe_get(item, 'location')):
            score += 100
        
        # Hedge fund bonus
        if is_hedge_fund_firm(item):
            score += 50
    
    return score

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

def get_person_performance_metrics(person_id):
    """Get performance metrics related to a specific person"""
    person = get_person_by_id(person_id)
    if not person:
        return []
    
    person_company = safe_get(person, 'current_company_name').lower()
    related_metrics = []
    
    for firm in st.session_state.firms:
        firm_name = safe_get(firm, 'name').lower()
        if person_company in firm_name or firm_name in person_company:
            if firm.get('performance_metrics'):
                related_metrics.extend(firm['performance_metrics'])
    
    return related_metrics

def get_firm_performance_metrics(firm_id):
    """Get performance metrics related to a specific firm"""
    firm = get_firm_by_id(firm_id)
    if not firm:
        return []
    return firm.get('performance_metrics', [])

# --- FIXED: Enhanced shared work history with date overlap calculations ---
def get_shared_work_history(person_id):
    """FIXED: Get people who have overlapping work periods at the same companies"""
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
                            "person_title": person_period['title'],
                            "colleague_title": other_period['title'],
                            "colleague_email": safe_get(other_person, 'email'),
                            "colleague_linkedin": safe_get(other_person, 'linkedin_profile_url'),
                            "overlap_start": overlap_start,
                            "overlap_end": overlap_end,
                            "overlap_days": overlap_days,
                            "overlap_duration": duration_str,
                            "overlap_period": overlap_period,
                            "connection_type": "Overlapping Tenure",
                            "connection_strength": "Strong" if overlap_days >= 365 else "Medium" if overlap_days >= 90 else "Brief"
                        })
    
    # Remove duplicates (same person, different overlapping periods) and consolidate
    unique_shared = {}
    for item in shared_history:
        key = f"{item['colleague_id']}_{item['shared_company']}"
        if key not in unique_shared:
            unique_shared[key] = item
        else:
            # If same person and company but different overlap, keep the longer one
            existing = unique_shared[key]
            if item['overlap_days'] > existing['overlap_days']:
                unique_shared[key] = item
    
    # Also find people who worked at same companies but with no overlap (legacy connections)
    for other_person in st.session_state.people:
        if other_person['id'] == person_id:
            continue
            
        other_employments = get_employments_by_person_id(other_person['id'])
        other_companies = set(emp['company_name'] for emp in other_employments)
        person_companies = set(emp['company_name'] for emp in person_employments)
        
        shared_companies = person_companies.intersection(other_companies)
        
        for company in shared_companies:
            # Check if we already have an overlapping connection for this person/company
            key = f"{other_person['id']}_{company}"
            if key not in unique_shared:
                # Add as non-overlapping connection
                person_emp = next((emp for emp in person_employments if emp['company_name'] == company), None)
                other_emp = next((emp for emp in other_employments if emp['company_name'] == company), None)
                
                if person_emp and other_emp:
                    shared_history.append({
                        "colleague_name": safe_get(other_person, 'name'),
                        "colleague_id": other_person['id'],
                        "shared_company": company,
                        "colleague_current_company": safe_get(other_person, 'current_company_name'),
                        "colleague_current_title": safe_get(other_person, 'current_title'),
                        "person_title": person_emp['title'],
                        "colleague_title": other_emp['title'],
                        "colleague_email": safe_get(other_person, 'email'),
                        "colleague_linkedin": safe_get(other_person, 'linkedin_profile_url'),
                        "overlap_days": 0,
                        "overlap_duration": "No overlap",
                        "overlap_period": "Different periods",
                        "connection_type": "Same Company (No Overlap)",
                        "connection_strength": "Weak"
                    })
    
    # Convert back to list and sort by connection strength and overlap duration
    all_connections = list(unique_shared.values()) + [item for item in shared_history if item.get('overlap_days', 0) == 0]
    
    # Sort: Strong connections first, then by overlap days (longest first), then by name
    def sort_key(conn):
        strength_order = {"Strong": 0, "Medium": 1, "Brief": 2, "Weak": 3}
        return (
            strength_order.get(conn.get('connection_strength', 'Weak'), 3),
            -conn.get('overlap_days', 0),  # Negative for descending order
            conn['colleague_name']
        )
    
    return sorted(all_connections, key=sort_key)

# --- FIXED: Enhanced employment addition with date validation ---
def add_employment_with_dates(person_id, company_name, title, start_date, end_date=None, location="Unknown", strategy="Unknown"):
    """Add employment record with proper date validation"""
    try:
        # Validate dates
        if end_date and start_date and end_date <= start_date:
            raise ValueError("End date must be after start date")
        
        # Check for overlapping employments for the same person
        existing_employments = get_employments_by_person_id(person_id)
        for emp in existing_employments:
            if emp.get('start_date') and start_date:
                existing_start = emp['start_date']
                existing_end = emp.get('end_date')
                
                # Check for overlap
                overlap = calculate_date_overlap(existing_start, existing_end, start_date, end_date)
                if overlap and emp['company_name'] != company_name:
                    logger.warning(f"Potential employment overlap detected: {company_name} vs {emp['company_name']}")
        
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

# --- PAGINATION HELPERS ---
def paginate_data(data, page, items_per_page=10):
    """Paginate data and return current page items and pagination info"""
    total_items = len(data)
    total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
    
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    
    current_items = data[start_idx:end_idx]
    
    return current_items, {
        'current_page': page,
        'total_pages': total_pages,
        'total_items': total_items,
        'items_per_page': items_per_page,
        'start_idx': start_idx,
        'end_idx': end_idx
    }

def display_pagination_controls(page_info, page_key):
    """Display pagination controls"""
    if page_info['total_pages'] <= 1:
        return
    
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col1:
        if st.button("⏮️ First", disabled=page_info['current_page'] == 0, key=f"{page_key}_first"):
            st.session_state[f"{page_key}_page"] = 0
            st.rerun()
    
    with col2:
        if st.button("◀️ Prev", disabled=page_info['current_page'] == 0, key=f"{page_key}_prev"):
            st.session_state[f"{page_key}_page"] = max(0, page_info['current_page'] - 1)
            st.rerun()
    
    with col3:
        st.write(f"Page {page_info['current_page'] + 1} of {page_info['total_pages']} " +
                f"(showing {page_info['start_idx'] + 1}-{page_info['end_idx']} of {page_info['total_items']})")
    
    with col4:
        if st.button("▶️ Next", disabled=page_info['current_page'] >= page_info['total_pages'] - 1, key=f"{page_key}_next"):
            st.session_state[f"{page_key}_page"] = min(page_info['total_pages'] - 1, page_info['current_page'] + 1)
            st.rerun()
    
    with col5:
        if st.button("⏭️ Last", disabled=page_info['current_page'] >= page_info['total_pages'] - 1, key=f"{page_key}_last"):
            st.session_state[f"{page_key}_page"] = page_info['total_pages'] - 1
            st.rerun()

# --- REVIEW SYSTEM FUNCTIONS ---
def add_to_review_queue(people_extractions, performance_extractions, source_info="Manual extraction"):
    """Add extracted data to review queue"""
    review_item = {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now(),
        'source': source_info,
        'people': people_extractions,
        'performance': performance_extractions,
        'status': 'pending',
        'reviewed_people': [],
        'reviewed_performance': []
    }
    
    st.session_state.pending_review_data.append(review_item)
    st.session_state.review_start_time = datetime.now()
    st.session_state.show_review_interface = True
    
    return review_item['id']

def get_review_time_remaining():
    """Get remaining time before auto-save (in seconds)"""
    if not st.session_state.review_start_time:
        return st.session_state.auto_save_timeout
    
    elapsed = (datetime.now() - st.session_state.review_start_time).total_seconds()
    remaining = max(0, st.session_state.auto_save_timeout - elapsed)
    return remaining

def display_review_interface():
    """Display the review interface for pending extractions"""
    if not st.session_state.pending_review_data:
        return
    
    st.markdown("---")
    st.header("📋 Review Extracted Data")
    
    # Timer display and controls
    remaining_time = get_review_time_remaining()
    if remaining_time > 0:
        minutes_left = int(remaining_time // 60)
        seconds_left = int(remaining_time % 60)
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.info(f"⏱️ **Auto-save in**: {minutes_left}m {seconds_left}s")
        with col2:
            if st.button("💾 Save All Now", use_container_width=True, key="save_all_now_button"):
                saved_count = 0
                for review_item in st.session_state.pending_review_data:
                    approved_people, approved_performance = approve_all_in_review(review_item['id'])
                    source_context = f"Review batch approved: {review_item['source']}. Timestamp: {review_item['timestamp']}"
                    people_saved, perf_saved = save_approved_extractions(approved_people, approved_performance, source_context)
                    saved_count += people_saved
                
                st.session_state.pending_review_data = []
                st.session_state.show_review_interface = False
                st.success(f"✅ Saved {saved_count} items to database!")
                st.rerun()
        with col3:
            if st.button("❌ Cancel Review", use_container_width=True, key="cancel_review_button"):
                st.session_state.pending_review_data = []
                st.session_state.show_review_interface = False
                st.rerun()
    
    # Display review items (simplified for space)
    for i, review_item in enumerate(st.session_state.pending_review_data):
        st.markdown(f"### 📦 Batch {i+1}: {review_item['source']}")
        
        # People review
        if review_item['people']:
            st.markdown("#### 👥 People Found")
            for j, person in enumerate(review_item['people'][:5]):  # Show first 5
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.write(f"**{safe_get(person, 'name')}**")
                    st.caption(f"{safe_get(person, 'title')} at {safe_get(person, 'company')}")
                with col2:
                    st.caption(f"📍 {safe_get(person, 'location')}")
                with col3:
                    person_id = f"person_{review_item['id']}_{j}"
                    is_approved = person in review_item.get('reviewed_people', [])
                    if st.button("✅" if not is_approved else "❌", key=person_id):
                        if is_approved:
                            review_item['reviewed_people'] = [p for p in review_item['reviewed_people'] if p != person]
                        else:
                            review_item['reviewed_people'].append(person)
                        st.rerun()
        
        # Performance review
        if review_item['performance']:
            st.markdown("#### 📊 Performance Metrics")
            for j, metric in enumerate(review_item['performance'][:3]):  # Show first 3
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.write(f"**{safe_get(metric, 'fund_name')}**")
                    st.caption(f"{safe_get(metric, 'metric_type')}: {safe_get(metric, 'value')}")
                with col2:
                    st.caption(f"📅 {safe_get(metric, 'period')}")
                with col3:
                    metric_id = f"metric_{review_item['id']}_{j}"
                    is_approved = metric in review_item.get('reviewed_performance', [])
                    if st.button("✅" if not is_approved else "❌", key=metric_id):
                        if is_approved:
                            review_item['reviewed_performance'] = [m for m in review_item['reviewed_performance'] if m != metric]
                        else:
                            review_item['reviewed_performance'].append(metric)
                        st.rerun()

def approve_all_in_review(review_id):
    """Approve all items in a specific review"""
    review_item = next((r for r in st.session_state.pending_review_data if r['id'] == review_id), None)
    if not review_item:
        return [], []
    
    review_item['reviewed_people'] = review_item['people'].copy()
    review_item['reviewed_performance'] = review_item['performance'].copy()
    review_item['status'] = 'approved'
    
    return review_item['reviewed_people'], review_item['reviewed_performance']

def save_approved_extractions(approved_people, approved_performance, source_context=""):
    """FIXED: Save approved extractions to main database with historical context and firm type inference"""
    saved_people = 0
    saved_performance = 0
    
    # Create source metadata
    source_metadata = {
        "extraction_date": datetime.now().isoformat(),
        "source_type": "newsletter_extraction", 
        "context_preview": source_context[:500] + "..." if len(source_context) > 500 else source_context,
        "total_people_in_batch": len(approved_people),
        "total_metrics_in_batch": len(approved_performance)
    }
    
    # Process people with historical context
    for person_data in approved_people:
        new_person_id = str(uuid.uuid4())
        company_name = person_data.get('current_company') or person_data.get('company', 'Unknown')
        title = person_data.get('current_title') or person_data.get('title', 'Unknown')
        
        # Create person with historical notes
        new_person = {
            "id": new_person_id,
            "name": safe_get(person_data, 'name'),
            "current_title": title,
            "current_company_name": company_name,
            "location": safe_get(person_data, 'location'),
            "email": "",
            "linkedin_profile_url": "",
            "phone": "",
            "education": "",
            "expertise": safe_get(person_data, 'expertise'),
            "aum_managed": "",
            "strategy": safe_get(person_data, 'expertise', 'Unknown'),
            "created_date": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            # Historical context
            "extraction_history": [{
                **source_metadata,
                "movement_type": safe_get(person_data, 'movement_type'),
                "previous_company": safe_get(person_data, 'previous_company'),
                "seniority_level": safe_get(person_data, 'seniority_level'),
                "original_extraction": person_data
            }]
        }
        
        st.session_state.people.append(new_person)
        
        # FIXED: Add firm if doesn't exist with proper firm type inference
        if not get_firm_by_name(company_name):
            # FIXED: Infer firm type based on company name and person context
            firm_type = infer_firm_type(company_name, person_data)
            
            new_firm = {
                "id": str(uuid.uuid4()),
                "name": company_name,
                "firm_type": firm_type,  # FIXED: Added proper firm type
                "location": safe_get(person_data, 'location'),
                "headquarters": "Unknown",
                "aum": "Unknown",
                "founded": None,
                "strategy": safe_get(person_data, 'expertise', 'Various'),
                "website": "",
                "description": f"{firm_type} - extracted from newsletter intelligence",
                "performance_metrics": [],
                "created_date": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                # Historical context for firm
                "extraction_history": [{
                    **source_metadata,
                    "discovered_through": f"Employee extraction: {safe_get(person_data, 'name')}",
                    "original_context": person_data
                }]
            }
            st.session_state.firms.append(new_firm)
        
        # Add employment record with context - FIXED: Include date handling
        employment_start_date = date.today()  # Default to today for extracted people
        add_employment_with_dates(
            new_person_id, 
            company_name, 
            title, 
            employment_start_date, 
            None,  # No end date (current position)
            safe_get(person_data, 'location'), 
            safe_get(person_data, 'expertise', 'Unknown')
        )
        
        saved_people += 1
    
    # Process performance metrics with historical context
    for metric in approved_performance:
        fund_name = safe_get(metric, 'fund_name')
        matching_firm = None
        
        for firm in st.session_state.firms:
            firm_name = safe_get(firm, 'name').lower()
            if fund_name.lower() in firm_name or firm_name in fund_name.lower():
                matching_firm = firm
                break
        
        if matching_firm:
            if 'performance_metrics' not in matching_firm:
                matching_firm['performance_metrics'] = []
            
            # Add metric with historical context
            enhanced_metric = {
                **metric,
                "id": str(uuid.uuid4()),
                "recorded_date": datetime.now().isoformat(),
                "extraction_context": source_metadata,
                "source_reliability": "newsletter_extraction"
            }
            
            # Check for duplicates
            existing = any(
                m.get('metric_type') == metric.get('metric_type') and 
                m.get('period') == metric.get('period') and
                m.get('date') == metric.get('date')
                for m in matching_firm['performance_metrics']
            )
            
            if not existing:
                matching_firm['performance_metrics'].append(enhanced_metric)
                
                # Update firm's last_updated
                matching_firm['last_updated'] = datetime.now().isoformat()
                
                # Add to firm's extraction history if it exists
                if 'extraction_history' not in matching_firm:
                    matching_firm['extraction_history'] = []
                
                matching_firm['extraction_history'].append({
                    **source_metadata,
                    "content_type": "performance_metric",
                    "metric_added": {
                        "type": safe_get(metric, 'metric_type'),
                        "value": safe_get(metric, 'value'),
                        "period": safe_get(metric, 'period')
                    }
                })
                
                saved_performance += 1
    
    save_data()
    return saved_people, saved_performance

def infer_firm_type(company_name, person_data=None):
    """FIXED: Infer firm type based on company name and context"""
    company_lower = company_name.lower()
    
    # Banking keywords
    if any(word in company_lower for word in ['bank', 'banking', 'goldman sachs', 'morgan stanley', 'jp morgan', 'barclays', 'credit suisse', 'ubs', 'deutsche bank', 'citi']):
        return "Investment Bank"
    
    # Central banks
    if any(word in company_lower for word in ['central bank', 'federal reserve', 'bank of england', 'ecb', 'boe', 'fed', 'people\'s bank']):
        return "Central Bank"
    
    # Pension funds
    if any(word in company_lower for word in ['pension', 'retirement', 'calpers', 'canada pension', 'superannuation']):
        return "Pension Fund"
    
    # Sovereign wealth funds
    if any(word in company_lower for word in ['sovereign wealth', 'temasek', 'gic', 'cpf', 'adia', 'norges']):
        return "Sovereign Wealth Fund"
    
    # Asset managers
    if any(word in company_lower for word in ['asset management', 'blackrock', 'vanguard', 'fidelity', 'state street', 'invesco', 'franklin templeton']):
        return "Asset Manager"
    
    # Private equity
    if any(word in company_lower for word in ['private equity', 'kkr', 'blackstone', 'carlyle', 'apollo', 'warburg pincus']):
        return "Private Equity"
    
    # Family offices
    if any(word in company_lower for word in ['family office', 'family investment']):
        return "Family Office"
    
    # Fintech
    if any(word in company_lower for word in ['fintech', 'financial technology', 'payments', 'blockchain', 'crypto']):
        return "Fintech Company"
    
    # News/Media
    if any(word in company_lower for word in ['news', 'media', 'financial times', 'bloomberg', 'reuters', 'wsj', 'wall street journal', 'cnbc']):
        return "News Organization"
    
    # Rating agencies
    if any(word in company_lower for word in ['rating', 'moody\'s', 'standard & poor', 's&p', 'fitch']):
        return "Rating Agency"
    
    # Hedge fund indicators (check last to avoid false positives)
    if any(word in company_lower for word in ['hedge', 'capital', 'partners', 'management', 'fund', 'investment']):
        return "Hedge Fund"
    
    # Default to Financial Services if unclear
    return "Financial Services"

# --- DATA EXPORT FUNCTIONS ---
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
                'Expertise': safe_get(firm, 'firm_type'),  # FIXED: Export firm type
                'AUM': safe_get(firm, 'aum')
            })
        
        df = pd.DataFrame(all_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        return df.to_csv(index=False), f"hedge_fund_data_{timestamp}.csv"
    
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        return None, None

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

# Initialize session state
try:
    initialize_session_state()
except Exception as init_error:
    st.error(f"Initialization error: {init_error}")
    st.stop()

# Check for stuck processing and auto-recover
try:
    recovery_occurred = check_and_recover_stuck_processing()
    if recovery_occurred:
        st.rerun()  # Refresh UI after recovery
except Exception as recovery_error:
    logger.warning(f"Recovery check failed: {recovery_error}")
    # Continue without recovery

# --- HEADER WITH QUICK DOWNLOAD ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("👥 Asian Hedge Fund Talent Network")
    st.markdown("### Professional intelligence platform for Asia's financial industry")

with col2:
    # Quick CSV download and emergency controls
    col2a, col2b = st.columns(2)
    with col2a:
        csv_data, filename = export_to_csv()
        if csv_data:
            st.download_button(
                "📊 Quick CSV Export",
                csv_data,
                filename,
                "text/csv",
                use_container_width=True,
                help="Download all data as CSV"
            )
    with col2b:
        # Emergency stop button (only show if processing is running)
        if st.session_state.background_processing['is_running']:
            if st.button("🚨 Emergency Stop", use_container_width=True, help="Force stop stuck processing", type="secondary"):
                if emergency_stop_processing():
                    st.warning("🚨 Emergency stop executed!")
                    st.rerun()

# --- SIDEBAR: AI Talent Extractor ---
with st.sidebar:
    st.title("🤖 AI Talent Extractor")
    
    # API Key Setup
    api_key = None
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if api_key:
            st.success("✅ API key loaded from secrets")
    except:
        pass
    
    if not api_key:
        api_key = st.text_input("Gemini API Key", type="password", 
                              help="Get from: https://makersuite.google.com/app/apikey")
    
    # Enhanced Model Selection with actual 2.0/2.5 models
    st.markdown("---")
    st.subheader("🤖 Model Selection")
    
    model_options = {
        "Gemini 1.5 Flash (Recommended)": "gemini-1.5-flash",
        "Gemini 1.5 Flash Latest": "gemini-1.5-flash-latest",
        "Gemini 1.5 Flash 8B": "gemini-1.5-flash-8b",
        "Gemini 1.5 Pro": "gemini-1.5-pro",
        "Gemini 1.5 Pro Latest": "gemini-1.5-pro-latest",
        "Gemini 2.0 Flash": "gemini-2.0-flash-thinking-exp",
        "Gemini 2.0 Flash Experimental": "gemini-2.0-flash-exp",
        "Gemini 2.5 Flash": "gemini-2.5-flash-exp",
        "Gemini Experimental 1114": "gemini-exp-1114",
        "Gemini Experimental 1121": "gemini-exp-1121"
    }
    
    selected_model_name = st.selectbox(
        "Choose AI model:",
        options=list(model_options.keys()),
        index=0,
        help="Different models have different capabilities and rate limits"
    )
    
    selected_model_id = model_options[selected_model_name]
    
    # Show rate limits and processing status
    rate_limits = get_model_rate_limits(selected_model_id)
    if rate_limits['delay'] < 1:
        delay_str = f"{int(rate_limits['delay'] * 1000)}ms delay"
    else:
        delay_str = f"{rate_limits['delay']}s delay"
    
    # Show batching support
    batching_info = ""
    if rate_limits.get('supports_batching', False):
        batch_size = rate_limits.get('batch_size', 1)
        batching_info = f" • Batching: {batch_size} chunks"
    
    st.caption(f"⚡ Flash 1.5: {rate_limits['requests_per_minute']} req/min, {delay_str}{batching_info}")
    
    # Show optimization note
    if rate_limits['requests_per_minute'] >= 2000:
        st.success("🚀 **2000 RPM OPTIMIZED**: Ultra-fast batched processing with massive chunks!")
    elif rate_limits['requests_per_minute'] >= 1000:
        st.info("⚡ **High-Speed Processing**: Fast batched processing enabled")
    elif rate_limits['delay'] < 0.5:
        st.info("⚡ **Fast Processing**: Optimized for speed")
    else:
        st.caption("🔬 Experimental model - slower processing")
    
    # Show current processing status if running - SIMPLIFIED
    if st.session_state.background_processing['is_running']:
        st.markdown("---")
        st.subheader("🔄 Processing Status")
        display_background_processing_widget()
    elif st.session_state.background_processing['progress'] > 0:
        st.markdown("---")
        st.subheader("✅ Last Extraction")
        display_background_processing_widget()
    
    # Processing Configuration
    st.markdown("---")
    st.subheader("⚙️ Processing Configuration")
    
    preprocessing_options = {
        "🚀 Minimal": "minimal",
        "⚖️ Balanced (Recommended)": "balanced", 
        "🔍 Aggressive": "aggressive",
        "📄 None (Raw Content)": "none"
    }
    
    selected_preprocessing = st.selectbox(
        "Text Preprocessing:",
        options=list(preprocessing_options.keys()),
        index=1,
        help="How much filtering to apply to input text"
    )
    preprocessing_mode = preprocessing_options[selected_preprocessing]
    
    chunking_options = {
        "🤖 Auto (Recommended)": "auto",
        "📄 Single Chunk": "single",
        "🔹 Small Chunks (40K)": "small",
        "⚖️ Medium Chunks (80K)": "medium",
        "🔷 Large Chunks (120K)": "large",
        "🔶 XLarge Chunks (150K)": "xlarge"
    }
    
    selected_chunking = st.selectbox(
        "Chunking Strategy:",
        options=list(chunking_options.keys()),
        index=0,
        help="2000 RPM OPTIMIZED: Ultra-large chunks with batching for maximum speed. Auto mode uses intelligent sizing with batching support."
    )
    chunk_size_mode = chunking_options[selected_chunking]
    
    # Review Mode Toggle
    st.markdown("---")
    st.subheader("👀 Review Settings")
    
    enable_review = st.checkbox(
        "📋 Enable Review Mode", 
        value=st.session_state.enable_review_mode,
        help="Review extracted data before saving to database"
    )
    st.session_state.enable_review_mode = enable_review
    
    if enable_review:
        timeout_options = {"2 minutes": 120, "3 minutes": 180, "5 minutes": 300, "10 minutes": 600}
        selected_timeout = st.selectbox("⏱️ Auto-save timeout:", options=list(timeout_options.keys()), index=1)
        st.session_state.auto_save_timeout = timeout_options[selected_timeout]
        
        if st.session_state.pending_review_data:
            remaining = get_review_time_remaining()
            if remaining > 0:
                minutes_left = int(remaining // 60)
                seconds_left = int(remaining % 60)
                st.warning(f"⏱️ **{len(st.session_state.pending_review_data)} items pending review!**")
                st.info(f"Auto-save in: {minutes_left}m {seconds_left}s")
                
                if st.button("🔍 Go to Review", use_container_width=True, key="sidebar_goto_review"):
                    st.session_state.show_review_interface = True
                    st.rerun()
    
    # Setup model
    model = None
    if api_key and GENAI_AVAILABLE:
        model = setup_gemini(api_key, selected_model_id)
        
        st.markdown("---")
        st.subheader("📄 Extract from Newsletter")
        
        input_method = st.radio("Input method:", ["📝 Text", "📁 File"])
        
        newsletter_text = ""
        if input_method == "📝 Text":
            newsletter_text = st.text_area("Newsletter content:", height=200, 
                                         placeholder="Paste hedge fund newsletter content here...")
        else:
            uploaded_file = st.file_uploader("Upload newsletter:", type=['txt'])
            if uploaded_file:
                try:
                    # Try enhanced file loading first
                    success, content, error_msg, encoding_used = load_file_content_enhanced(uploaded_file)
                    
                    if success:
                        newsletter_text = content
                        char_count = len(newsletter_text)
                        
                        # Show file info with encoding
                        st.success(f"✅ **File loaded successfully!**")
                        col_info1, col_info2 = st.columns(2)
                        with col_info1:
                            st.info(f"📁 **Size**: {char_count:,} characters")
                            st.info(f"🔤 **Encoding**: {encoding_used}")
                        with col_info2:
                            # 2000 RPM OPTIMIZED: Calculate estimates based on ultra-fast processing
                            if chunk_size_mode == "auto":
                                estimated_chunk_size = min(max(char_count // 15, 60000), 120000)
                            else:
                                flash_2000_chunk_sizes = {"single": char_count, "small": 40000, "medium": 80000, "large": 120000, "xlarge": 150000}
                                estimated_chunk_size = flash_2000_chunk_sizes.get(chunk_size_mode, 80000)
                            
                            if chunk_size_mode == "single" and char_count <= 200000:
                                estimated_chunks = 1
                            else:
                                estimated_chunks = max(1, char_count // estimated_chunk_size)
                            
                            # 2000 RPM SPEED: Ultra-fast processing (0.1-0.2 minutes per chunk with batching)
                            estimated_time = estimated_chunks * 0.15  # 9 seconds per chunk average for 2000 RPM
                            
                            st.info(f"📊 **Est. chunks**: {estimated_chunks} ({chunk_size_mode} mode)")
                            if estimated_time < 1:
                                st.info(f"⚡ **Est. time**: ~{int(estimated_time * 60)}s (2000 RPM)")
                            else:
                                st.info(f"⚡ **Est. time**: ~{estimated_time:.1f}min (2000 RPM)")
                        
                        # Show warning message if there was one
                        if error_msg:
                            st.warning(f"⚠️ {error_msg}")
                        
                        # Show preview of content
                        with st.expander("👀 Content Preview", expanded=False):
                            preview_text = newsletter_text[:1000] + "..." if len(newsletter_text) > 1000 else newsletter_text
                            st.text_area("Preview:", value=preview_text, height=150, disabled=True)
                    else:
                        st.error(f"❌ {error_msg}")
                        st.info("💡 **Tips**: Try saving the file as UTF-8 text, or check if it contains special characters")
                        
                except Exception as file_error:
                    st.error(f"❌ Error loading file: {str(file_error)}")
                    st.info("💡 **Try**: Different file encoding or copy/paste the content instead")

        # Extract button
        if st.button("🚀 Start Background Extraction", use_container_width=True):
            if not newsletter_text.strip():
                st.error("Please provide newsletter content")
            elif not model:
                st.error("Please provide API key")
            elif st.session_state.background_processing['is_running']:
                st.warning("Extraction already in progress!")
            else:
                # Start background processing
                start_background_extraction(newsletter_text, model, preprocessing_mode, chunk_size_mode)
                st.success("🚀 2000 RPM Flash extraction started! Ultra-fast batched processing enabled.")
                st.rerun()

    elif not GENAI_AVAILABLE:
        st.error("Please install: pip install google-generativeai")

# --- MAIN CONTENT AREA ---

# Review Interface (if active)
if st.session_state.show_review_interface and st.session_state.pending_review_data:
    display_review_interface()

# Global Search Bar (SINGLE)
st.markdown("---")
col1, col2 = st.columns([4, 1])

with col1:
    search_query = st.text_input(
        "🔍 Search people, firms, or performance...", 
        value=st.session_state.global_search,
        placeholder="Try: 'Goldman Sachs', 'Portfolio Manager', 'Citadel', 'Sharpe ratio'...",
        key="main_search_input"
    )

with col2:
    if st.button("🔍 Search", use_container_width=True, key="main_search_button") or search_query != st.session_state.global_search:
        st.session_state.global_search = search_query
        if search_query and len(search_query.strip()) >= 2:
            st.rerun()

# Handle global search results
if st.session_state.global_search and len(st.session_state.global_search.strip()) >= 2:
    search_query = st.session_state.global_search
    matching_people, matching_firms, matching_metrics = enhanced_global_search(search_query)
    
    if matching_people or matching_firms or matching_metrics:
        st.markdown("### 🔍 Search Results")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("👥 People", len(matching_people))
        with col2:
            st.metric("🏢 Firms", len(matching_firms))
        with col3:
            st.metric("📊 Metrics", len(matching_metrics))
        with col4:
            total_results = len(matching_people) + len(matching_firms) + len(matching_metrics)
            st.metric("🎯 Total", total_results)
        
        # Show search results (limited to save space)
        if matching_people:
            st.markdown(f"**👥 People ({len(matching_people)} found)**")
            for person in matching_people[:3]:  # Show first 3
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.markdown(f"**{safe_get(person, 'name')}**")
                    st.caption(f"{safe_get(person, 'current_title')} at {safe_get(person, 'current_company_name')}")
                with col2:
                    st.caption(f"📍 {safe_get(person, 'location')}")
                with col3:
                    if st.button("👁️ View", key=f"search_person_{person['id']}", use_container_width=True):
                        go_to_person_details(person['id'])
                        st.rerun()
        
        if matching_firms:
            st.markdown(f"**🏢 Firms ({len(matching_firms)} found)**")
            for firm in matching_firms[:3]:  # Show first 3
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.markdown(f"**{safe_get(firm, 'name')}**")
                    firm_type = safe_get(firm, 'firm_type', 'Unknown')
                    strategy = safe_get(firm, 'strategy', '')
                    display_text = f"{firm_type}"
                    if strategy and strategy != 'Unknown':
                        display_text += f" • {strategy}"
                    st.caption(f"{display_text} • {safe_get(firm, 'location')}")
                with col2:
                    st.caption(f"💰 {safe_get(firm, 'aum')}")
                with col3:
                    if st.button("👁️ View", key=f"search_firm_{firm['id']}", use_container_width=True):
                        go_to_firm_details(firm['id'])
                        st.rerun()
        
        # Clear search button
        if st.button("❌ Clear Search", key="main_clear_search"):
            st.session_state.global_search = ""
            st.rerun()
        
        st.markdown("---")
    
    else:
        st.info(f"🔍 No results found for '{search_query}'. Try different keywords.")
        if st.button("❌ Clear Search", key="no_results_clear_search"):
            st.session_state.global_search = ""
            st.rerun()
        st.markdown("---")

# Top Navigation
col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 2])

with col1:
    if st.button("👥 People", use_container_width=True, 
                 type="primary" if st.session_state.current_view == 'people' else "secondary"):
        go_to_people()
        st.rerun()

with col2:
    if st.button("🏢 Firms", use_container_width=True, 
                 type="primary" if st.session_state.current_view == 'firms' else "secondary"):
        go_to_firms()
        st.rerun()

with col3:
    if st.button("➕ Person", use_container_width=True):
        st.session_state.show_add_person_modal = True
        st.rerun()

with col4:
    if st.button("🏢➕ Firm", use_container_width=True):
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

# --- MAIN VIEWS AND MODALS ---

# Current view handling
if st.session_state.current_view == 'people':
    st.markdown("---")
    st.header("👥 Financial Professionals")
    
    if not st.session_state.people:
        st.info("No people added yet. Use 'Add Person' button above or extract from newsletters using AI.")
        st.info("💡 The sample data includes people from Goldman Sachs Asia and other firms with **overlapping work periods** to demonstrate the new network features!")
    else:
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            locations = ["All"] + sorted(list(set(safe_get(p, 'location') for p in st.session_state.people if safe_get(p, 'location') not in ['Unknown', 'N/A', ''])))
            location_filter = st.selectbox("Filter by Location", locations)
        with col2:
            companies = ["All"] + sorted(list(set(safe_get(p, 'current_company_name') for p in st.session_state.people if safe_get(p, 'current_company_name') not in ['Unknown', 'N/A', ''])))
            company_filter = st.selectbox("Filter by Company", companies)
        with col3:
            search_term = st.text_input("Search by Name", placeholder="Enter name...")
        
        # Apply filters
        filtered_people = st.session_state.people
        if location_filter != "All":
            filtered_people = [p for p in filtered_people if safe_get(p, 'location') == location_filter]
        if company_filter != "All":
            filtered_people = [p for p in filtered_people if safe_get(p, 'current_company_name') == company_filter]
        if search_term:
            filtered_people = [p for p in filtered_people if search_term.lower() in safe_get(p, 'name').lower()]
        
        # PRIORITIZED SORTING: Asia + Hedge Funds first, then by date
        def get_person_sort_key(person):
            priority_score = get_priority_score(person, 'person')
            
            for date_field in ['last_updated', 'created_date']:
                if date_field in person and person[date_field]:
                    try:
                        date_obj = datetime.fromisoformat(person[date_field].replace('Z', '+00:00'))
                        return (-priority_score, -date_obj.timestamp())
                    except:
                        continue
            return (-priority_score, 0)
        
        filtered_people.sort(key=get_person_sort_key)
        
        # Paginate results
        people_to_show, people_page_info = paginate_data(filtered_people, st.session_state.people_page, 12)
        
        st.write(f"**Showing {people_page_info['start_idx'] + 1}-{people_page_info['end_idx']} of {people_page_info['total_items']} people** (Asia + Hedge Funds prioritized)")
        
        # Display people in card grid (3 columns)
        if people_to_show:
            for i in range(0, len(people_to_show), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(people_to_show):
                        person = people_to_show[i + j]
                        
                        with col:
                            # Create card container
                            with st.container(border=True):
                                # Person header with priority indicator
                                person_priority = get_priority_score(person, 'person')
                                priority_emoji = "🌟" if person_priority >= 100 else "⭐" if person_priority >= 50 else ""
                                
                                st.markdown(f"**👤 {safe_get(person, 'name')} {priority_emoji}**")
                                st.caption(f"{safe_get(person, 'current_title')}")
                                st.caption(f"🏢 {safe_get(person, 'current_company_name')}")
                                
                                # Person details
                                location = safe_get(person, 'location')
                                if location != 'Unknown':
                                    st.text(f"📍 {location}")
                                
                                expertise = safe_get(person, 'expertise')
                                if expertise and expertise != 'Unknown':
                                    st.text(f"🏆 {expertise}")
                                
                                aum = safe_get(person, 'aum_managed')
                                if aum and aum != 'Unknown':
                                    st.text(f"💰 {aum}")
                                
                                # FIXED: Show connection info with overlap details
                                shared_history = get_shared_work_history(person['id'])
                                if shared_history:
                                    strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
                                    total_connections = len(shared_history)
                                    if strong_connections > 0:
                                        st.text(f"🔗 {strong_connections} strong, {total_connections} total")
                                    else:
                                        st.text(f"🔗 {total_connections} connections")
                                
                                # Show when added/updated
                                if 'created_date' in person or 'last_updated' in person:
                                    date_to_show = person.get('last_updated') or person.get('created_date')
                                    if date_to_show:
                                        try:
                                            date_obj = datetime.fromisoformat(date_to_show.replace('Z', '+00:00'))
                                            days_ago = (datetime.now(date_obj.tzinfo) - date_obj).days
                                            if days_ago == 0:
                                                st.caption("🕒 Added today")
                                            elif days_ago == 1:
                                                st.caption("🕒 Added yesterday")
                                            elif days_ago < 7:
                                                st.caption(f"🕒 Added {days_ago} days ago")
                                            else:
                                                st.caption(f"🕒 Added {date_obj.strftime('%b %d, %Y')}")
                                        except:
                                            pass
                                
                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("👁️ View", key=f"view_person_card_{person['id']}", use_container_width=True):
                                        go_to_person_details(person['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("✏️ Edit", key=f"edit_person_card_{person['id']}", use_container_width=True):
                                        st.session_state.edit_person_data = person
                                        st.session_state.show_edit_person_modal = True
                                        st.rerun()
        
        # Pagination controls
        display_pagination_controls(people_page_info, "people")

elif st.session_state.current_view == 'firms':
    st.markdown("---")
    st.header("🏢 Financial Institutions")
    
    if not st.session_state.firms:
        st.info("No firms added yet. Use 'Add Firm' button above.")
    else:
        # PRIORITIZED SORTING: Asia + Hedge Funds first, then by date
        def get_firm_sort_key(firm):
            priority_score = get_priority_score(firm, 'firm')
            
            for date_field in ['last_updated', 'created_date']:
                if date_field in firm and firm[date_field]:
                    try:
                        date_obj = datetime.fromisoformat(firm[date_field].replace('Z', '+00:00'))
                        return (-priority_score, -date_obj.timestamp())
                    except:
                        continue
            return (-priority_score, 0)
        
        sorted_firms = sorted(st.session_state.firms, key=get_firm_sort_key)
        
        # Paginate results
        firms_to_show, firms_page_info = paginate_data(sorted_firms, st.session_state.firms_page, 12)
        
        st.write(f"**Showing {firms_page_info['start_idx'] + 1}-{firms_page_info['end_idx']} of {firms_page_info['total_items']} firms** (Asia + Hedge Funds prioritized)")
        
        # Display firms in card grid (3 columns)
        if firms_to_show:
            for i in range(0, len(firms_to_show), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(firms_to_show):
                        firm = firms_to_show[i + j]
                        people_count = len(get_people_by_firm(safe_get(firm, 'name')))
                        metrics_count = len(firm.get('performance_metrics', []))
                        
                        with col:
                            # Create card container
                            with st.container(border=True):
                                # Firm header with priority indicator
                                firm_priority = get_priority_score(firm, 'firm')
                                priority_emoji = "🌟" if firm_priority >= 100 else "⭐" if firm_priority >= 50 else ""
                                
                                st.markdown(f"**🏢 {safe_get(firm, 'name')} {priority_emoji}**")
                                # Show firm type prominently
                                firm_type = safe_get(firm, 'firm_type', 'Unknown')
                                strategy = safe_get(firm, 'strategy', '')
                                if strategy and strategy != 'Unknown':
                                    st.caption(f"{firm_type} • {strategy}")
                                else:
                                    st.caption(f"{firm_type}")
                                
                                # Firm details
                                location = safe_get(firm, 'location')
                                if location != 'Unknown':
                                    st.text(f"📍 {location}")
                                
                                aum = safe_get(firm, 'aum')
                                if aum and aum != 'Unknown':
                                    st.text(f"💰 {aum}")
                                
                                founded = safe_get(firm, 'founded')
                                if founded and founded != 'Unknown':
                                    st.text(f"📅 Founded {founded}")
                                
                                # Metrics
                                col_people, col_metrics = st.columns(2)
                                with col_people:
                                    st.metric("👥", people_count, label_visibility="collapsed", help="People")
                                with col_metrics:
                                    st.metric("📊", metrics_count, label_visibility="collapsed", help="Performance Metrics")
                                
                                # Website
                                website = safe_get(firm, 'website')
                                if website and website != 'Unknown':
                                    st.text(f"🌐 Website")
                                
                                # Show when added/updated
                                if 'created_date' in firm or 'last_updated' in firm:
                                    date_to_show = firm.get('last_updated') or firm.get('created_date')
                                    if date_to_show:
                                        try:
                                            date_obj = datetime.fromisoformat(date_to_show.replace('Z', '+00:00'))
                                            days_ago = (datetime.now(date_obj.tzinfo) - date_obj).days
                                            if days_ago == 0:
                                                st.caption("🕒 Added today")
                                            elif days_ago == 1:
                                                st.caption("🕒 Added yesterday")
                                            elif days_ago < 7:
                                                st.caption(f"🕒 Added {days_ago} days ago")
                                            else:
                                                st.caption(f"🕒 Added {date_obj.strftime('%b %d, %Y')}")
                                        except:
                                            pass
                                
                                # Action buttons
                                col_view, col_edit = st.columns(2)
                                with col_view:
                                    if st.button("👁️ View", key=f"view_firm_card_{firm['id']}", use_container_width=True):
                                        go_to_firm_details(firm['id'])
                                        st.rerun()
                                with col_edit:
                                    if st.button("✏️ Edit", key=f"edit_firm_card_{firm['id']}", use_container_width=True):
                                        st.session_state.edit_firm_data = firm
                                        st.session_state.show_edit_firm_modal = True
                                        st.rerun()
        
        # Pagination controls
        display_pagination_controls(firms_page_info, "firms")

elif st.session_state.current_view == 'person_details' and st.session_state.selected_person_id:
    person = get_person_by_id(st.session_state.selected_person_id)
    if not person:
        st.error("Person not found")
        go_to_people()
        st.rerun()
    
    # Person header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.header(f"👤 {safe_get(person, 'name')}")
        st.subheader(f"{safe_get(person, 'current_title')} at {safe_get(person, 'current_company_name')}")
    with col2:
        col2a, col2b = st.columns(2)
        with col2a:
            if st.button("← Back"):
                go_to_people()
                st.rerun()
        with col2b:
            if st.button("✏️ Edit"):
                st.session_state.edit_person_data = person
                st.session_state.show_edit_person_modal = True
                st.rerun()
    
    # Basic info
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**📍 Location:** {safe_get(person, 'location')}")
        email = safe_get(person, 'email')
        if email:
            st.markdown(f"**📧 Email:** [{email}](mailto:{email})")
        phone = safe_get(person, 'phone')
        if phone:
            st.markdown(f"**📱 Phone:** {phone}")
        linkedin = safe_get(person, 'linkedin_profile_url')
        if linkedin:
            st.markdown(f"**🔗 LinkedIn:** [Profile]({linkedin})")
    
    with col2:
        education = safe_get(person, 'education')
        if education:
            st.markdown(f"**🎓 Education:** {education}")
        expertise = safe_get(person, 'expertise')
        if expertise:
            st.markdown(f"**🏆 Expertise:** {expertise}")
        aum = safe_get(person, 'aum_managed')
        if aum:
            st.markdown(f"**💰 AUM Managed:** {aum}")
        strategy = safe_get(person, 'strategy')
        if strategy:
            st.markdown(f"**📈 Strategy:** {strategy}")
    
    # Employment History
    st.markdown("---")
    st.subheader("💼 Employment History")
    
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
            
            st.markdown(f"""
            **{safe_get(emp, 'title')}** at **{safe_get(emp, 'company_name')}**  
            📅 {start_date_str} → {end_date_str} ({duration_str})  
            📍 {safe_get(emp, 'location')} • 📈 {safe_get(emp, 'strategy')}
            """)
    else:
        st.info("No employment history available.")
    
    # FIXED: Enhanced Shared Work History with Date Overlaps
    st.markdown("---")
    st.subheader("🤝 Shared Work History")
    
    shared_history = get_shared_work_history(person['id'])
    
    if shared_history:
        st.write(f"**Found {len(shared_history)} colleagues who worked at the same companies:**")
        
        # Show summary statistics
        strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
        medium_connections = len([c for c in shared_history if c.get('connection_strength') == 'Medium'])
        brief_connections = len([c for c in shared_history if c.get('connection_strength') == 'Brief'])
        weak_connections = len([c for c in shared_history if c.get('connection_strength') == 'Weak'])
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🔥 Strong (1+ years)", strong_connections)
        with col2:
            st.metric("⚡ Medium (3+ months)", medium_connections)
        with col3:
            st.metric("💨 Brief (<3 months)", brief_connections)
        with col4:
            st.metric("🔗 Same Company", weak_connections)
        
        # Show individual connection cards
        st.markdown("#### 🔗 Connections Details")
        for connection in shared_history[:10]:  # Show first 10
            with st.container(border=True):
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.markdown(f"**{connection['colleague_name']}**")
                    st.caption(f"**Shared:** {connection['shared_company']}")
                    
                    # FIXED: Show overlap period information
                    if connection.get('overlap_days', 0) > 0:
                        st.caption(f"⏰ **Overlap:** {connection['overlap_period']}")
                        st.caption(f"📅 **Duration:** {connection['overlap_duration']}")
                    else:
                        st.caption(f"📅 **No overlap** (different periods)")
                
                with col2:
                    st.caption(f"**Current:** {connection['colleague_current_title']}")
                    st.caption(f"at {connection['colleague_current_company']}")
                    
                    # Contact info if available
                    if connection.get('colleague_email'):
                        st.caption(f"📧 {connection['colleague_email']}")
                    if connection.get('colleague_linkedin'):
                        st.caption(f"🔗 LinkedIn")
                
                with col3:
                    # FIXED: Show connection strength with overlap details
                    strength = connection.get('connection_strength', 'Weak')
                    if strength == "Strong":
                        st.success("🔥 Strong")
                        st.caption("1+ year overlap")
                    elif strength == "Medium":
                        st.info("⚡ Medium")
                        st.caption("3+ months overlap")
                    elif strength == "Brief":
                        st.warning("💨 Brief")
                        st.caption("<3 months overlap")
                    else:
                        st.info("🔗 Same")
                        st.caption("Company only")
                    
                    if st.button("👁️ View", key=f"view_colleague_{connection['colleague_id']}", use_container_width=True):
                        go_to_person_details(connection['colleague_id'])
                        st.rerun()
        
        if len(shared_history) > 10:
            st.info(f"Showing top 10 of {len(shared_history)} total connections")
        
    else:
        st.info("No shared work history found with other people in the database.")
        st.write("💡 This person will show connections when:")
        st.write("• Other people in the database have **overlapping work periods** at the same companies")
        st.write("• More employment history with **start and end dates** is added to the system")
        st.write("• People's work history includes past employers (not just current jobs)")

elif st.session_state.current_view == 'firm_details' and st.session_state.selected_firm_id:
    firm = get_firm_by_id(st.session_state.selected_firm_id)
    if not firm:
        st.error("Firm not found")
        go_to_firms()
        st.rerun()
    
    # Firm header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.header(f"🏢 {safe_get(firm, 'name')}")
        firm_type = safe_get(firm, 'firm_type', 'Unknown')
        strategy = safe_get(firm, 'strategy', '')
        location = safe_get(firm, 'location')
        if strategy and strategy != 'Unknown':
            st.markdown(f"**{firm_type} • {strategy}** • {location}")
        else:
            st.markdown(f"**{firm_type}** • {location}")
    with col2:
        col2a, col2b = st.columns(2)
        with col2a:
            if st.button("← Back"):
                go_to_firms()
                st.rerun()
        with col2b:
            if st.button("✏️ Edit"):
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
        st.markdown(f"**🏢 Type:** {safe_get(firm, 'firm_type')}")
        st.markdown(f"**📍 Location:** {safe_get(firm, 'location')}")
        st.markdown(f"**🏛️ Headquarters:** {safe_get(firm, 'headquarters')}")
    with col2:
        st.markdown(f"**📈 Strategy:** {safe_get(firm, 'strategy')}")
        website = safe_get(firm, 'website')
        if website:
            st.markdown(f"**🌐 Website:** [{website}]({website})")
    
    description = safe_get(firm, 'description')
    if description:
        st.markdown(f"**📄 About:** {description}")
    
    # INTEGRATED PERFORMANCE METRICS
    st.markdown("---")
    st.subheader("📊 Performance Metrics")
    
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
        st.write("💡 Performance metrics will appear here when extracted from newsletters.")
    
    # People at this firm
    st.markdown("---")
    st.subheader(f"👥 People at {safe_get(firm, 'name')}")
    
    firm_people = get_people_by_firm(safe_get(firm, 'name'))
    if firm_people:
        # Show people with basic info
        for person in firm_people[:10]:  # Show first 10
            shared_history = get_shared_work_history(person['id'])
            
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
                
                with col1:
                    st.markdown(f"**👤 {safe_get(person, 'name')}**")
                    st.caption(safe_get(person, 'current_title'))
                
                with col2:
                    email = safe_get(person, 'email')
                    expertise = safe_get(person, 'expertise')
                    if email:
                        st.caption(f"📧 {email}")
                    if expertise:
                        st.caption(f"🏆 {expertise}")
                
                with col3:
                    # FIXED: Show connection info with overlap details
                    if shared_history:
                        strong_connections = len([c for c in shared_history if c.get('connection_strength') == 'Strong'])
                        total_connections = len(shared_history)
                        st.metric("🔗", total_connections, label_visibility="collapsed")
                        if strong_connections > 0:
                            st.caption(f"{strong_connections} strong connections")
                        else:
                            st.caption(f"{total_connections} connections")
                    else:
                        st.metric("🔗", 0, label_visibility="collapsed")
                        st.caption("No connections")
                
                with col4:
                    if st.button("👁️ Profile", key=f"view_full_{person['id']}", use_container_width=True):
                        go_to_person_details(person['id'])
                        st.rerun()
        
        if len(firm_people) > 10:
            st.info(f"Showing first 10 of {len(firm_people)} people")
    else:
        st.info("No people added for this firm yet.")

# Handle modals...
if st.session_state.show_add_person_modal:
    st.markdown("---")
    st.subheader("➕ Add New Person")
    
    with st.form("add_person_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("Full Name*", placeholder="John Smith")
            title = st.text_input("Current Title*", placeholder="Portfolio Manager")
            
            # Company selection with existing options
            company_options = [f['name'] for f in st.session_state.firms if f.get('name')]
            if company_options:
                company = st.text_input(
                    "Current Company*",
                    placeholder="Type company name or select from suggestions",
                    help=f"Available: {', '.join(company_options[:3])}{'...' if len(company_options) > 3 else ''}"
                )
            else:
                company = st.text_input("Current Company*", placeholder="Company Name")
            
            location = simple_text_input("location", "", "add_person", ["Hong Kong", "Singapore", "Tokyo"])
        
        with col2:
            email = st.text_input("Email", placeholder="john.smith@company.com")
            phone = st.text_input("Phone", placeholder="+852-1234-5678")
            education = st.text_input("Education", placeholder="Harvard, MIT")
            expertise = simple_text_input("expertise", "", "add_person", ["Equity Research", "Fixed Income", "Quantitative"])
        
        # FIXED: Add employment date fields
        st.markdown("**📅 Employment Dates**")
        col3, col4 = st.columns(2)
        with col3:
            aum_managed = st.text_input("AUM Managed", placeholder="500M USD")
            start_date = st.date_input("Start Date", value=date.today())
        with col4:
            strategy = simple_text_input("strategy", "", "add_person", ["Long/Short Equity", "Multi-Strategy"])
            is_current = st.checkbox("Current Position", value=True)
            end_date = None if is_current else st.date_input("End Date", value=date.today())
        
        submitted = st.form_submit_button("Add Person", use_container_width=True)
        
        if submitted:
            if name and title and company and location:
                new_person_id = str(uuid.uuid4())
                st.session_state.people.append({
                    "id": new_person_id,
                    "name": name,
                    "current_title": title,
                    "current_company_name": company,
                    "location": location,
                    "email": email,
                    "linkedin_profile_url": "",
                    "phone": phone,
                    "education": education,
                    "expertise": expertise,
                    "aum_managed": aum_managed,
                    "strategy": strategy,
                    "created_date": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "extraction_history": [{
                        "extraction_date": datetime.now().isoformat(),
                        "source_type": "manual_entry",
                        "context_preview": f"Manually added person: {name} at {company}",
                        "entry_method": "add_person_form"
                    }]
                })
                
                # FIXED: Add employment record with dates
                success = add_employment_with_dates(
                    new_person_id, company, title, start_date, end_date, location, strategy or "Unknown"
                )
                
                if success:
                    save_data()
                    st.success(f"✅ Added {name}!")
                    st.session_state.show_add_person_modal = False
                    st.rerun()
                else:
                    st.error("Failed to add employment record")
            else:
                st.error("Please fill required fields (*)")
    
    if st.button("❌ Cancel", key="cancel_add_person_outside"):
        st.session_state.show_add_person_modal = False
        st.rerun()

# Auto-save functionality
current_time = datetime.now()
if 'last_auto_save' not in st.session_state:
    st.session_state.last_auto_save = current_time

time_since_save = (current_time - st.session_state.last_auto_save).total_seconds()
if time_since_save > 30 and (st.session_state.people or st.session_state.firms):
    save_data()
    st.session_state.last_auto_save = current_time

# Handle review timeout
if st.session_state.pending_review_data and st.session_state.review_start_time:
    if get_review_time_remaining() <= 0:
        # Auto-save logic for reviews
        for review_item in st.session_state.pending_review_data:
            approved_people, approved_performance = approve_all_in_review(review_item['id'])
            save_approved_extractions(approved_people, approved_performance)
        
        st.session_state.pending_review_data = []
        st.session_state.show_review_interface = False

# --- FOOTER ---
st.markdown("---")
st.markdown("### 👥 Asian Financial Industry Intelligence Platform")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("**🔍 Global Search**")
with col2:
    st.markdown("**📊 Performance Tracking**") 
with col3:
    st.markdown("**🤝 Enhanced Work History**")
with col4:
    st.markdown("**📋 Smart Review System**")
