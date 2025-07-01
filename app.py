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
import threading
import logging # ADDED: Import logging

# Additional imports for enhanced export functionality
import zipfile
from io import BytesIO, StringIO

# Try to import openpyxl for Excel exports
try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    st.sidebar.warning("📊 Excel export unavailable. Install openpyxl: pip install openpyxl")

# Try to import google.generativeai, handle if not available
try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False

# --- Configure Logging ---
# Create a logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Define log file path
LOG_FILE = LOGS_DIR / "app_activity.log"

# Basic logging configuration
# This will write logs to the file and also output to the console
logging.basicConfig(
    level=logging.INFO, # Set to logging.DEBUG for more verbose output
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler() # Also log to console
    ]
)
# END ADDED LOGGING CONFIGURATION

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
    value = data.get(key, default)
    return value if value is not None else default

# --- MISSING FUNCTIONS - MOVED TO TOP ---

def handle_dynamic_input(field_name, current_value, table_name, context=""):
    """
    Enhanced dynamic input that prioritizes typing with suggestions
    
    Args:
        field_name: Name of the field (e.g., 'location', 'company')
        current_value: Current value to pre-select
        table_name: Database table name ('people', 'firms', etc.)
        context: Additional context for unique keys
    
    Returns:
        Selected or newly entered value
    """
    import streamlit as st
    
    # Get existing options from database based on field and table
    existing_options = get_unique_values_from_session_state(table_name, field_name)
    
    # Remove None/empty values and sort
    existing_options = sorted([opt for opt in existing_options if opt and opt.strip() and opt != 'Unknown'])
    
    # Create unique key for input
    unique_key = f"{field_name}_input_{table_name}_{context}"
    
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

def enhanced_global_search(query):
    """
    Enhanced global search function with better matching and debugging
    """
    query_lower = query.lower().strip()
    
    if len(query_lower) < 2:
        return [], [], []
    
    matching_people = []
    matching_firms = []
    matching_metrics = []
    
    # Search people with enhanced matching
    for person in st.session_state.people:
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
    
    # Search firms with enhanced matching  
    for firm in st.session_state.firms:
        searchable_fields = [
            safe_get(firm, 'name', ''),
            safe_get(firm, 'location', ''),
            safe_get(firm, 'strategy', ''),
            safe_get(firm, 'description', ''),
            safe_get(firm, 'headquarters', ''),
            safe_get(firm, 'aum', ''),
            safe_get(firm, 'website', '')
        ]
        
        searchable_text = " ".join([field for field in searchable_fields if field and field != 'Unknown']).lower()
        
        if (query_lower in searchable_text or 
            any(query_lower in field.lower() for field in searchable_fields if field and field != 'Unknown')):
            matching_firms.append(firm)
    
    # Search performance metrics in firms
    for firm in st.session_state.firms:
        if firm.get('performance_metrics'):
            for metric in firm['performance_metrics']:
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
    
    return matching_people, matching_firms, matching_metrics

# --- Database Persistence Setup ---
DATA_DIR = Path("hedge_fund_data")
DATA_DIR.mkdir(exist_ok=True)

PEOPLE_FILE = DATA_DIR / "people.json"
FIRMS_FILE = DATA_DIR / "firms.json"
EMPLOYMENTS_FILE = DATA_DIR / "employments.json"
EXTRACTIONS_FILE = DATA_DIR / "extractions.json"

# RENAMED: This function now performs the actual file saving in a thread-safe manner
def _perform_data_save_to_files(people_data, firms_data, employments_data, all_extractions_data):
    """
    Internal function to save data to JSON files.
    Designed to be called from a separate thread.
    """
    try:
        DATA_DIR.mkdir(exist_ok=True)
        
        with open(PEOPLE_FILE, 'w', encoding='utf-8') as f:
            json.dump(people_data, f, indent=2, default=str)
        logging.info(f"💾 Saved {len(people_data)} people to {PEOPLE_FILE}") # ADDED: Logging

        with open(FIRMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(firms_data, f, indent=2, default=str)
        logging.info(f"💾 Saved {len(firms_data)} firms to {FIRMS_FILE}") # ADDED: Logging
        
        with open(EMPLOYMENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(employments_data, f, indent=2, default=str)
        logging.info(f"💾 Saved {len(employments_data)} employments to {EMPLOYMENTS_FILE}") # ADDED: Logging
        
        if all_extractions_data is not None:
            with open(EXTRACTIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_extractions_data, f, indent=2, default=str)
            logging.info(f"💾 Saved {len(all_extractions_data)} extractions to {EXTRACTIONS_FILE}") # ADDED: Logging
        
        logging.info(f"💾 Background save complete for all data.") # CHANGED: Logging
        # In a real app, you might use a queue or another mechanism to update UI with success
        if 'save_status' in st.session_state: # Example of updating status in main thread
            st.session_state.save_status = "Data saved successfully!"
        
    except Exception as e:
        logging.error(f"❌ Background save error: {e}", exc_info=True) # CHANGED: Logging with exception info
        if 'save_status' in st.session_state:
            st.session_state.save_status = f"Error during save: {e}"


def save_data_async():
    """
    Triggers an asynchronous save of all extracted data to files.
    This function immediately returns without waiting for the save to complete.
    """
    logging.info("Initiating asynchronous save...") # ADDED: Logging
    # Create a copy of the data from session state before passing to thread
    # This is crucial as st.session_state is not thread-safe.
    people_copy = list(st.session_state.people)
    firms_copy = list(st.session_state.firms)
    employments_copy = list(st.session_state.employments)
    all_extractions_copy = list(st.session_state.all_extractions) if 'all_extractions' in st.session_state else None

    # Set a status message in the main thread (optional)
    st.session_state.save_status = "Saving data in background..."

    # Start the save operation in a new thread
    save_thread = threading.Thread(
        target=_perform_data_save_to_files,
        args=(people_copy, firms_copy, employments_copy, all_extractions_copy),
        name="DataSaveThread" # ADDED: Name the thread for clearer logs
    )
    save_thread.daemon = True # Allow the program to exit even if thread is running
    save_thread.start()
    
    st.sidebar.info("💾 Auto-save triggered in background (non-blocking).") # Inform user
    logging.info("Asynchronous save thread started.") # ADDED: Logging

def load_data():
    """Load data from JSON files with detailed logging"""
    logging.info("Attempting to load data from files...") # ADDED: Logging
    try:
        people = []
        firms = []
        employments = []
        extractions = []
        
        # Load people
        if PEOPLE_FILE.exists():
            with open(PEOPLE_FILE, 'r', encoding='utf-8') as f:
                people = json.load(f)
            logging.info(f"✅ Loaded {len(people)} people from {PEOPLE_FILE}") # CHANGED: Logging
        else:
            logging.warning(f"⚠️ No people file found at {PEOPLE_FILE}") # CHANGED: Logging
        
        # Load firms
        if FIRMS_FILE.exists():
            with open(FIRMS_FILE, 'r', encoding='utf-8') as f:
                firms = json.load(f)
            logging.info(f"✅ Loaded {len(firms)} firms from {FIRMS_FILE}") # CHANGED: Logging
        else:
            logging.warning(f"⚠️ No firms file found at {FIRMS_FILE}") # CHANGED: Logging
        
        # Load employments
        if EMPLOYMENTS_FILE.exists():
            with open(EMPLOYMENTS_FILE, 'r', encoding='utf-8') as f:
                employments = json.load(f)
                # Convert date strings back to date objects
                for emp in employments:
                    if emp.get('start_date'):
                        try: # ADDED: Try-except for date parsing
                            emp['start_date'] = datetime.strptime(emp['start_date'], '%Y-%m-%d').date()
                        except ValueError:
                            logging.warning(f"Invalid start_date format for employment {emp.get('id')}: {emp.get('start_date')}")
                    if emp.get('end_date') and emp['end_date'] != 'Present': # Handle 'Present' string
                        try: # ADDED: Try-except for date parsing
                            emp['end_date'] = datetime.strptime(emp['end_date'], '%Y-%m-%d').date()
                        except ValueError:
                            logging.warning(f"Invalid end_date format for employment {emp.get('id')}: {emp.get('end_date')}")
            logging.info(f"✅ Loaded {len(employments)} employments from {EMPLOYMENTS_FILE}") # CHANGED: Logging
        else:
            logging.warning(f"⚠️ No employments file found at {EMPLOYMENTS_FILE}") # CHANGED: Logging
        
        # Load extractions
        if EXTRACTIONS_FILE.exists():
            with open(EXTRACTIONS_FILE, 'r', encoding='utf-8') as f:
                extractions = json.load(f)
            logging.info(f"✅ Loaded {len(extractions)} extractions from {EXTRACTIONS_FILE}") # CHANGED: Logging
        else:
            logging.warning(f"⚠️ No extractions file found at {EXTRACTIONS_FILE}") # CHANGED: Logging
        
        logging.info(f"📁 Data directory: {DATA_DIR.absolute()}") # CHANGED: Logging
        
        return people, firms, employments, extractions
        
    except Exception as e:
        logging.error(f"❌ Error loading data: {e}", exc_info=True) # CHANGED: Logging with exception info
        return [], [], [], []

# --- Initialize Session State with Rich Dummy Data ---
def init_dummy_data():
    """Initialize with comprehensive dummy data if no saved data exists"""
    logging.info("Initializing session state with dummy data.") # ADDED: Logging
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
            "strategy": "Long-only Growth Equity"
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
            "strategy": "Multi-Strategy Quantitative"
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
            "strategy": "Equity Long/Short"
        }
    ]
    
    # Sample firms with detailed information and performance metrics
    sample_firms = [
        {
            "id": str(uuid.uuid4()),
            "name": "Hillhouse Capital",
            "location": "Hong Kong",
            "headquarters": "Beijing, China",
            "aum": "60B USD",
            "founded": 2005,
            "strategy": "Long-only, Growth Equity",
            "website": "https://hillhousecap.com",
            "description": "Asia's largest hedge fund focusing on technology and healthcare investments",
            "performance_metrics": [
                {
                    "id": str(uuid.uuid4()),
                    "metric_type": "return",
                    "value": "12.5",
                    "period": "YTD",
                    "date": "2025",
                    "additional_info": "Net return"
                }
            ]
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Millennium Partners Asia",
            "location": "Singapore",
            "headquarters": "New York, USA",
            "aum": "35B USD",
            "founded": 1989,
            "strategy": "Multi-strategy, Quantitative",
            "website": "https://millennium.com",
            "description": "Global hedge fund with significant Asian operations",
            "performance_metrics": [
                {
                    "id": str(uuid.uuid4()),
                    "metric_type": "sharpe",
                    "value": "1.8",
                    "period": "Current",
                    "date": "2025",
                    "additional_info": "Improved from 1.2"
                }
            ]
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Citadel Asia",
            "location": "Hong Kong",
            "headquarters": "Chicago, USA",
            "aum": "45B USD",
            "founded": 1990,
            "strategy": "Multi-strategy, Market Making",
            "website": "https://citadel.com",
            "description": "Leading global hedge fund with growing Asian presence",
            "performance_metrics": []
        }
    ]
    
    # Create employment history with overlaps
    sample_employments = []
    
    # Li Wei Chen's history (Hillhouse Capital)
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
            "strategy": "Investment Banking"
        },
        {
            "id": str(uuid.uuid4()),
            "person_id": li_id,
            "company_name": "Hillhouse Capital",
            "title": "Portfolio Manager",
            "start_date": date(2021, 9, 1),
            "end_date": None,
            "location": "Hong Kong",
            "strategy": "Growth Equity"
        }
    ])
    
    return sample_people, sample_firms, sample_employments

def initialize_session_state():
    """Initialize session state with saved or dummy data"""
    logging.info("Initializing Streamlit session state...") # ADDED: Logging
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
    
    # NEW: File processing preferences
    if 'preprocessing_mode' not in st.session_state:
        st.session_state.preprocessing_mode = "balanced"
    if 'chunk_size_preference' not in st.session_state:
        st.session_state.chunk_size_preference = "auto"
    
    # NEW: Review system (SIMPLIFIED/DISABLED FOR NOW)
    st.session_state.enable_review_mode = False # Set to False to disable review system initially
    if 'pending_review_data' not in st.session_state:
        st.session_state.pending_review_data = []
    if 'review_start_time' not in st.session_state:
        st.session_state.review_start_time = None
    if 'show_review_interface' not in st.session_state:
        st.session_state.show_review_interface = False
    if 'auto_save_timeout' not in st.session_state:
        st.session_state.auto_save_timeout = 180  # 3 minutes in seconds
    # ADDED: State for asynchronous save status
    if 'save_status' not in st.session_state:
        st.session_state.save_status = ""
    logging.info("Streamlit session state initialized.") # ADDED: Logging

# --- AI Setup ---
@st.cache_resource
def setup_gemini(api_key, model_id="gemini-1.5-flash"):
    """Setup Gemini AI model safely with model selection"""
    logging.info(f"Setting up Gemini AI model: {model_id}") # ADDED: Logging
    if not GENAI_AVAILABLE:
        logging.warning("Google Generative AI library not available.") # ADDED: Logging
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_id)
        # Store model_id as an attribute for rate limiting
        model.model_id = model_id
        logging.info(f"Gemini AI model '{model_id}' setup successfully.") # ADDED: Logging
        return model
    except Exception as e:
        st.error(f"AI setup failed: {e}")
        logging.error(f"AI setup failed for model '{model_id}': {e}", exc_info=True) # CHANGED: Logging with exception info
        return None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def create_cached_context():
    """Create cached context for hedge fund extraction with optimized prompts"""
    logging.info("Creating cached AI context.") # ADDED: Logging
    return {
        "system_instructions": """You are an expert financial analyst specializing in the hedge fund industry. Your task is to meticulously analyze the following text and extract key intelligence about hedge funds, investment banks, asset managers, private equity firms, and related financial institutions.

CORE EXTRACTION TARGETS:
1. PEOPLE: All individuals in professional contexts (current employees, new hires, departures, promotions, launches, appointments)
2. FIRMS: Hedge funds, investment banks, asset managers, family offices, private equity, sovereign wealth funds
3. PERFORMANCE DATA: Returns, risk metrics, AUM figures, fund performance, benchmarks
4. MOVEMENTS: Job changes, fund launches, firm transitions, strategic shifts

SPECIFIC FOCUS AREAS:
- Hedge fund managers and portfolio managers
- Investment bank professionals (VP, MD, Managing Director levels)
- Asset management executives (CIO, CEO, Head of Trading, etc.)
- Quantitative analysts and researchers
- Fund launches, closures, and strategic changes
- Performance attribution and risk metrics
- Assets under management (AUM) changes
- Geographic expansion and office openings

GEOGRAPHIC INTELLIGENCE:
- Identify primary geographic focus (Asia-Pacific, North America, Europe, etc.)
- Extract specific office locations and expansion plans
- Note regulatory environments and market access

PERFORMANCE METRICS PRIORITY:
- Net returns (YTD, annual, multi-year)
- Risk-adjusted returns (Sharpe ratio, information ratio)
- Maximum drawdown and volatility measures
- Alpha generation and beta coefficients
- Assets under management (AUM) and flows
- Benchmark comparisons and relative performance

FIRM CATEGORIZATION:
- Hedge funds (long/short equity, macro, credit, quantitative, etc.)
- Investment banks (bulge bracket, boutique, regional)
- Asset managers (traditional, alternative, specialized)
- Family offices (single-family, multi-family)
- Private equity and venture capital
- Sovereign wealth funds and pension funds""",
        
        "example_input": """Goldman Sachs veteran John Smith joins Citadel Asia as Managing Director in Hong Kong, bringing 15 years of equity trading experience. Former JPMorgan portfolio manager Lisa Chen launches Dragon Capital Management, a $200M long/short equity fund focused on Asian markets. 

Engineers Gate's systematic trading fund topped $4.2 billion in assets and delivered 12.3% net returns year-to-date, with a Sharpe ratio of 1.8 compared to 1.2 last year. The fund's maximum drawdown remained below 2.5% during Q3 volatility.

Millennium Management's flagship fund returned 15.2% net in Q2 with maximum drawdown of 2.1%, outperforming the MSCI World Index by 340 basis points. The firm is expanding its London office and hired three senior portfolio managers from Renaissance Technologies.""",
        
        "example_output": """{
  "geographic_focus": "Global with Asia-Pacific and European expansion",
  "people": [
    {
      "name": "John Smith",
      "current_company": "Citadel Asia",
      "current_title": "Managing Director",
      "previous_company": "Goldman Sachs",
      "movement_type": "hire",
      "location": "Hong Kong",
      "experience_years": "15",
      "expertise": "Equity Trading",
      "seniority_level": "senior"
    },
    {
      "name": "Lisa Chen",
      "current_company": "Dragon Capital Management",
      "current_title": "Founder/Portfolio Manager", 
      "previous_company": "JPMorgan",
      "movement_type": "launch",
      "location": "Unknown",
      "expertise": "Long/Short Equity",
      "seniority_level": "senior"
    }
  ],
  "firms": [
    {
      "name": "Dragon Capital Management",
      "firm_type": "Hedge Fund",
      "strategy": "Long/Short Equity",
      "geographic_focus": "Asian Markets",
      "aum": "200000000",
      "status": "newly_launched"
    },
    {
      "name": "Citadel Asia",
      "firm_type": "Hedge Fund",
      "location": "Hong Kong",
      "status": "expanding"
    },
    {
      "name": "Engineers Gate",
      "firm_type": "Hedge Fund", 
      "strategy": "Systematic Trading",
      "status": "operating"
    },
    {
      "name": "Millennium Management",
      "firm_type": "Hedge Fund",
      "status": "expanding",
      "expansion_location": "London"
    }
  ],
  "performance": [
    {
      "fund_name": "Engineers Gate",
      "metric_type": "aum",
      "value": "4200000000",
      "period": "Current",
      "date": "2025",
      "additional_info": "USD, systematic trading fund"
    },
    {
      "fund_name": "Engineers Gate",
      "metric_type": "return",
      "value": "12.3",
      "period": "YTD", 
      "date": "2025",
      "additional_info": "net return, percent"
    },
    {
      "fund_name": "Engineers Gate",
      "metric_type": "sharpe",
      "value": "1.8",
      "period": "Current",
      "date": "2025", 
      "additional_info": "improved from 1.2 previous year"
    },
    {
      "fund_name": "Engineers Gate",
      "metric_type": "drawdown",
      "value": "2.5",
      "period": "Q3",
      "date": "2025",
      "additional_info": "maximum drawdown below, percent"
    },
    {
      "fund_name": "Millennium Management",
      "metric_type": "return",
      "value": "15.2", 
      "period": "Q2",
      "date": "2025",
      "additional_info": "net return, flagship fund, percent"
    },
    {
      "fund_name": "Millennium Management",
      "metric_type": "drawdown",
      "value": "2.1",
      "period": "Q2", 
      "date": "2025",
      "additional_info": "maximum drawdown, percent"
    },
    {
      "fund_name": "Millennium Management",
      "metric_type": "alpha",
      "value": "340",
      "period": "Q2",
      "date": "2025",
      "benchmark": "MSCI World Index",
      "additional_info": "outperformance in basis points"
    }
  ]
}""",
        
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
      "firm_type": "Hedge Fund|Investment Bank|Asset Manager|Private Equity|Family Office",
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
      "date": "YYYY or MM-DD",
      "benchmark": "Benchmark name if comparison mentioned",
      "additional_info": "Units, context, fund type, net/gross specification"
    }
  ]
}"""
    }

def build_extraction_prompt_with_cache(newsletter_text, cached_context):
    """Build enhanced extraction prompt using cached context for superior hedge fund intelligence"""
    
    prompt = f"""
{cached_context['system_instructions']}

CRITICAL EXTRACTION PROTOCOLS:
1. ZERO TOLERANCE for placeholder text - NEVER use "Full Name", "Full Legal Name", "Name", "Person Name", "Exact Firm Name"
2. EXTRACT ONLY verified, specific names and firms explicitly mentioned in the text
3. PRIORITIZE senior-level movements (MD, VP, CIO, CEO, Portfolio Manager, Head of Trading)
4. CAPTURE numerical precision - exact percentages, dollar amounts, basis points
5. IDENTIFY industry context - hedge fund vs investment bank vs asset manager
6. DETERMINE seniority level from titles and context clues
7. EXTRACT geographic intelligence and market focus areas

ENHANCED TARGETING:
- Look for fund launches with specific AUM figures
- Identify performance attribution with benchmarks  
- Capture risk metrics in institutional context
- Track senior talent movements between major institutions
- Note expansion strategies and office openings
- Extract regulatory and compliance appointments

PROFESSIONAL TITLE MAPPING:
- Managing Director (MD) = senior level
- Vice President (VP) = senior level  
- Portfolio Manager (PM) = senior level
- Chief Investment Officer (CIO) = c_suite level
- Head of [Department] = senior level
- Analyst = junior/mid level
- Associate = mid level

EXAMPLE INPUT:
{cached_context['example_input']}

EXAMPLE OUTPUT:
{cached_context['example_output']}

REQUIRED OUTPUT FORMAT:
{cached_context['output_format']}

TARGET NEWSLETTER FOR ANALYSIS:
{newsletter_text}

EXTRACTION MANDATE: Extract ONLY concrete, verifiable information with complete names and specific institutions. If any field cannot be determined with certainty, omit that entry entirely. Focus on actionable intelligence for hedge fund industry tracking.

Return ONLY the JSON output with geographic_focus, people, firms, and performance arrays populated with verified data."""
    
    return prompt

# ENHANCED: Flexible file preprocessing with configurable options
def preprocess_newsletter_text(text, mode="balanced"):
    """
    Enhanced preprocessing with configurable modes for different file sizes and types
    
    Args:
        text: Input text to preprocess
        mode: Preprocessing intensity level
            - "minimal": Only basic cleaning, preserve most content
            - "balanced": Moderate filtering (default)
            - "aggressive": Heavy filtering for very noisy content
            - "none": Skip preprocessing entirely
    """
    import re
    
    logging.info(f"Starting text preprocessing in '{mode}' mode.") # ADDED: Logging
    
    if mode == "none":
        st.info("📄 **No preprocessing applied** - Processing raw content")
        logging.info("Preprocessing mode set to 'none', skipping all preprocessing.") # ADDED: Logging
        return text
    
    # Show original size
    original_size = len(text)
    
    # Step 1: Extract and preserve subject lines that contain relevant info
    subject_line = ""
    subject_match = re.search(r'Subject:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if subject_match:
        subject_line = subject_match.group(1).strip()
        hf_keywords_in_subject = ['appoints', 'joins', 'launches', 'hires', 'promotes', 'moves', 'cio', 'ceo', 'pm', 'portfolio manager', 'hedge fund', 'capital', 'management']
        if any(keyword in subject_line.lower() for keyword in hf_keywords_in_subject):
            text = f"NEWSLETTER SUBJECT: {subject_line}\n\n{text}"
            logging.debug(f"Preserving subject line: {subject_line}") # ADDED: Logging
    
    # Step 2: Remove email headers (but preserve subject if already extracted above)
    if mode in ["balanced", "aggressive"]:
        email_header_patterns = [
            r'From:\s*.*?\n',
            r'To:\s*.*?\n', 
            r'Sent:\s*.*?\n',
            r'Subject:\s*.*?\n',  # Remove original subject since we preserved it above
            r'Date:\s*.*?\n',
            r'Reply-To:\s*.*?\n',
            r'Return-Path:\s*.*?\n'
        ]
        logging.debug("Removing email headers.") # ADDED: Logging
        for pattern in email_header_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
    
    # Step 3: Remove URLs and tracking links
    if mode in ["balanced", "aggressive"]:
        url_patterns = [
            r'https?://[^\s<>"{}|\\^`\[\]]+',  # Standard URLs
            r'<https?://[^>]+>',  # URLs in angle brackets
            r'urldefense\.proofpoint\.com[^\s]*',  # Proofpoint URLs
            r'pardot\.withintelligence\.com[^\s]*',  # Tracking URLs
            r'jpmorgan\.email\.streetcontxt\.net[^\s]*'  # Email tracking
        ]
        logging.debug("Removing URLs and tracking links.") # ADDED: Logging
        for pattern in url_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # Step 4: Remove email disclaimers and legal text (only in aggressive mode)
    if mode == "aggressive":
        logging.debug("Removing disclaimers and legal text in aggressive mode.") # ADDED: Logging
        disclaimer_patterns = [
            r'This section contains materials produced by third parties.*?(?=\n\n|\Z)',
            r'This message is confidential and subject to terms.*?(?=\n\n|\Z)',
            r'Important Reminder: JPMorgan Chase will never send emails.*?(?=\n\n|\Z)',
            r'Although this transmission and any links.*?(?=\n\n|\Z)',
            r'©.*?All rights reserved.*?(?=\n\n|\Z)',
            r'Unsubscribe.*?(?=\n\n|\Z)',
            r'Privacy Policy.*?(?=\n\n|\Z)',
            r'Update email preferences.*?(?=\n\n|\Z)',
            r'Not seeing what you expected\?.*?(?=\n\n|\Z)',
            r'Log in to my account.*?(?=\n\n|\Z)'
        ]
        
        for pattern in disclaimer_patterns:
            text = re.sub(pattern, '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Step 5: Remove HTML artifacts and email formatting
    if mode in ["balanced", "aggressive"]:
        html_patterns = [
            r'<[^>]+>',  # HTML tags
            r'&[a-zA-Z0-9#]+;',  # HTML entities
            r'\[cid:[^\]]+\]',  # Email embedded images
        ]
        logging.debug("Removing HTML artifacts and email formatting.") # ADDED: Logging
        for pattern in html_patterns:
            text = re.sub(pattern, '', text)
        
        # Only remove excessive formatting in aggressive mode
        if mode == "aggressive":
            text = re.sub(r'________________________________+', '', text)  # Email separators
            text = re.sub(r'\*\s*\|.*?\|\s*\*', '', text)  # Email table formatting
    
    # Step 6: Clean up excessive whitespace
    if mode in ["minimal", "balanced", "aggressive"]:
        logging.debug("Cleaning up excessive whitespace.") # ADDED: Logging
        # Remove multiple consecutive newlines
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        
        # Only filter lines in balanced/aggressive mode
        if mode in ["balanced", "aggressive"]:
            lines = text.split('\n')
            cleaned_lines = []
            for line in lines:
                # Keep line if it has meaningful content
                if re.search(r'[a-zA-Z].*[a-zA-Z]', line) and len(line.strip()) > 5:
                    # Clean up the line
                    line = re.sub(r'\s+', ' ', line.strip())  # Normalize whitespace
                    if line:
                        cleaned_lines.append(line)
            text = '\n'.join(cleaned_lines)
    
    # Step 7: Focus on hedge fund relevant content (only in aggressive mode)
    if mode == "aggressive":
        logging.debug("Focusing on hedge fund relevant content in aggressive mode.") # ADDED: Logging
        # Look for common hedge fund keywords and keep paragraphs containing them
        hf_keywords = [
            'hedge fund', 'portfolio manager', 'pm', 'cio', 'chief investment officer',
            'managing director', 'md', 'vice president', 'vp', 'analyst', 'trader',
            'fund launch', 'fund debut', 'joins', 'moves', 'promotes', 'appoints',
            'former', 'ex-', 'launches', 'capital management', 'partners', 'advisors',
            'assets under management', 'aum', 'long/short', 'equity', 'credit',
            'quantitative', 'macro', 'multi-strategy', 'arbitrage'
        ]
        
        # Performance-related keywords
        performance_keywords = [
            'irr', 'internal rate of return', 'sharpe', 'sharpe ratio', 'drawdown', 
            'maximum drawdown', 'max drawdown', 'alpha', 'beta', 'volatility', 'vol',
            'return', 'returns', 'performance', 'ytd', 'year to date', 'annualized',
            'net return', 'gross return', 'benchmark', 'outperformed', 'underperformed',
            'basis points', 'bps', '%', 'percent', 'up ', 'down ', 'gained', 'lost',
            'aum', 'assets', 'billion', 'million', 'fund size', 'nav', 'net asset value'
        ]
        
        # Additional keywords for better extraction
        movement_keywords = [
            'appoints', 'appointed', 'hiring', 'hired', 'departure', 'departing', 'leaving', 'joining', 'joined', 'moved', 'moving', 'promoted', 'promotion', 'named', 'named as', 'becomes', 'became', 'takes over', 'steps down'
        ]
        
        # Combine all keywords
        all_keywords = hf_keywords + performance_keywords + movement_keywords
        
        # Split into paragraphs and keep relevant ones
        paragraphs = text.split('\n\n')
        relevant_paragraphs = []
        for para in paragraphs:
            para_lower = para.lower()
            if any(keyword in para_lower for keyword in all_keywords):
                relevant_paragraphs.append(para)
            elif len(para) > 100 and ('capital' in para_lower or 'management' in para_lower): # Keep longer paragraphs that might be relevant
                relevant_paragraphs.append(para)
        
        # If we didn't find enough relevant content, keep more of the original
        if len(relevant_paragraphs) < 3:
            relevant_paragraphs = paragraphs[:20] # Keep first 20 paragraphs as fallback
            logging.debug("Not enough relevant paragraphs found, keeping first 20 as fallback.") # ADDED: Logging
        
        text = '\n\n'.join(relevant_paragraphs)
    
    # Final cleanup
    text = text.strip()
    
    # Show cleaning results
    original_size_before_filter = original_size # Use the size before the filtering in step 7 for more accurate reduction %
    final_size = len(text)
    reduction_pct = ((original_size_before_filter - final_size) / original_size_before_filter) * 100 if original_size_before_filter > 0 else 0
    st.info(f"📝 **Text Preprocessing Complete** (Mode: {mode.title()})")
    st.write(f"• **Original size**: {original_size_before_filter:,} characters")
    st.write(f"• **Processed size**: {final_size:,} characters")
    st.write(f"• **Reduction**: {reduction_pct:.1f}% content filtered")
    if mode == "aggressive":
        paragraphs_found = len(text.split('\n\n'))
        st.write(f"• **Relevant sections**: {paragraphs_found} found")
    
    logging.info(f"Text preprocessing finished. Original: {original_size_before_filter} chars, Processed: {final_size} chars, Reduction: {reduction_pct:.1f}%") # ADDED: Logging
    return text

# ENHANCED: Better file type support with encoding detection
def load_file_content(uploaded_file):
    """
    Enhanced file loading with better encoding detection and file type support
    Args:
        uploaded_file: Streamlit uploaded file object
    Returns:
        tuple: (success: bool, content: str, error_message: str)
    """
    logging.info(f"Attempting to load file content for: {uploaded_file.name}") # ADDED: Logging
    try:
        file_size = len(uploaded_file.getvalue())
        file_size_mb = file_size / (1024 * 1024)
        st.info(f"📁 **File Details**: {uploaded_file.name} ({file_size_mb:.1f} MB)")
        
        # Handle different file types
        if uploaded_file.type == "text/plain" or uploaded_file.name.endswith('.txt'):
            # Text file - try multiple encodings
            raw_data = uploaded_file.getvalue()
            # Try common encodings in order of preference
            encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1']
            content = None
            encoding_used = None
            for encoding in encodings:
                try:
                    content = raw_data.decode(encoding)
                    encoding_used = encoding
                    break
                except UnicodeDecodeError:
                    logging.debug(f"Failed to decode with {encoding}.") # ADDED: Logging
                    continue
            
            if content is None:
                logging.error(f"Could not decode file '{uploaded_file.name}' with any common encoding.") # ADDED: Logging
                return False, "", "Could not decode file. Please ensure it's a valid text file."
            st.success(f"✅ **Text file loaded** (encoding: {encoding_used})")
            logging.info(f"Successfully loaded text file '{uploaded_file.name}' with encoding: {encoding_used}") # ADDED: Logging
            return True, content, ""
        
        elif uploaded_file.type in ["application/pdf"] or uploaded_file.name.endswith('.pdf'):
            logging.warning(f"Unsupported file type for loading: {uploaded_file.name} ({uploaded_file.type}).") # ADDED: Logging
            return False, "", "PDF files not yet supported. Please convert to .txt format."
        
        elif uploaded_file.type in ["application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"] or uploaded_file.name.endswith(('.doc', '.docx')):
            logging.warning(f"Unsupported file type for loading: {uploaded_file.name} ({uploaded_file.type}).") # ADDED: Logging
            return False, "", "Word documents not yet supported. Please save as .txt format."
        
        else:
            # Try to treat as text anyway
            try:
                raw_data = uploaded_file.getvalue()
                content = raw_data.decode('utf-8', errors='ignore')
                st.warning(f"⚠️ Unknown file type '{uploaded_file.type}'. Attempting to read as text...")
                logging.warning(f"Unknown file type '{uploaded_file.type}' for '{uploaded_file.name}'. Attempting to read as UTF-8 with errors ignored.") # ADDED: Logging
                return True, content, ""
            except Exception as e:
                logging.error(f"Error reading unknown file type '{uploaded_file.name}' as text: {e}", exc_info=True) # CHANGED: Logging with exception info
                return False, "", f"Unsupported file type: {uploaded_file.type}. Please use .txt files."
    
    except Exception as e:
        logging.error(f"Error during file loading for '{uploaded_file.name}': {e}", exc_info=True) # CHANGED: Logging with exception info
        return False, "", f"Error reading file: {str(e)}"

def extract_single_chunk_safe(text, model):
    """Enhanced single chunk extraction with improved validation for hedge fund intelligence"""
    logging.info("Starting AI extraction for a single text chunk.") # ADDED: Logging
    try:
        # Use cached context to build efficient prompt
        cached_context = create_cached_context()
        prompt = build_extraction_prompt_with_cache(text, cached_context)
        
        response = model.generate_content(prompt)
        
        if not response or not response.text:
            logging.warning("AI response was empty or invalid.") # ADDED: Logging
            return [], []

        # Show debug info if enabled
        if hasattr(st.session_state, 'debug_mode') and st.session_state.debug_mode:
            with st.expander("🐛 Debug: Raw AI Response", expanded=False):
                st.code(response.text[:1000] + "..." if len(response.text) > 1000 else response.text)
                logging.debug(f"Raw AI Response: {response.text[:500]}...") # ADDED: Logging

        # Parse JSON
        json_start = response.text.find('{')
        json_end = response.text.rfind('}') + 1
        
        if json_start == -1:
            logging.warning("No JSON found in AI response.") # ADDED: Logging
            if hasattr(st.session_state, 'debug_mode') and st.session_state.debug_mode:
                st.error("🐛 Debug: No JSON found in AI response")
            return [], []
        
        try: # ADDED: Try-except for JSON parsing
            result = json.loads(response.text[json_start:json_end])
            logging.info("Successfully parsed JSON from AI response.") # ADDED: Logging
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error from AI response: {e}. Raw response: {response.text[json_start:json_end]}", exc_info=True) # CHANGED: Logging
            raise # Re-raise to be caught by outer handler
        
        people = result.get('people', [])
        performance = result.get('performance', [])
        firms = result.get('firms', [])
        geographic_focus = result.get('geographic_focus', '')

        # Enhanced validation for hedge fund intelligence
        valid_people = []
        valid_performance = []
        valid_firms = [] # CHANGED: Moved valid_firms initialization here

        # Validate people with enhanced structure
        for p in people:
            name = p.get('name', '').strip()
            current_company = p.get('current_company', '').strip()
            
            if (name and current_company and 
                name.lower() not in ['full name', 'full legal name', 'name', 'person name', 'unknown'] and
                current_company.lower() not in ['company', 'current firm name', 'company name', 'firm name', 'unknown'] and
                len(name) > 2 and len(current_company) > 2 and
                not any(placeholder in name.lower() for placeholder in ['exact', 'sample', 'example']) and
                not any(placeholder in current_company.lower() for placeholder in ['exact', 'sample', 'example'])):
                
                legacy_person = {
                    'id': str(uuid.uuid4()), # Assign ID immediately upon validation
                    'name': name,
                    'company': current_company, # Redundant, kept for backward compatibility with some views
                    'title': p.get('current_title', 'Unknown'), # Redundant, kept for backward compatibility with some views
                    'movement_type': p.get('movement_type', 'Unknown'),
                    'location': p.get('location', 'Unknown'),
                    'current_company_name': current_company, # Standardized field name
                    'current_title': p.get('current_title', 'Unknown'), # Standardized field name
                    'previous_company': p.get('previous_company', 'Unknown'),
                    'experience_years': p.get('experience_years', 'Unknown'),
                    'expertise': p.get('expertise', 'Unknown'),
                    'seniority_level': p.get('seniority_level', 'Unknown')
                }
                valid_people.append(legacy_person)
            else:
                logging.debug(f"Skipping invalid person entry: {p}") # ADDED: Logging

        # Validate firms with enhanced structure
        for f in firms:
            name = f.get('name', '').strip()
            firm_type = f.get('firm_type', '').strip()
            if (name and firm_type and
                name.lower() not in ['exact firm name', 'firm name', 'unknown'] and
                len(name) > 2 and
                not any(placeholder in name.lower() for placeholder in ['exact', 'sample', 'example'])):
                
                legacy_firm = {
                    'id': str(uuid.uuid4()), # Assign ID immediately upon validation
                    'name': name,
                    'type': firm_type, # Redundant, kept for backward compatibility
                    'location': f.get('location', 'Unknown'),
                    'strategy': f.get('strategy', 'Unknown'),
                    'aum': safe_get(f, 'aum', 'Unknown'),
                    'firm_type': firm_type,
                    'geographic_focus': f.get('geographic_focus', 'Unknown'),
                    'headquarters': f.get('headquarters', 'Unknown'),
                    'founded': safe_get(f, 'founded', 'Unknown'),
                    'website': f.get('website', 'Unknown'),
                    'description': f.get('description', 'Unknown'),
                    'status': f.get('status', 'operating')
                }
                
                # Handle performance metrics within firms
                if f.get('performance_metrics'):
                    for metric in f['performance_metrics']: # ADDED: Iteration
                        valid_performance.append({**metric, 'fund_name': name, 'id': str(uuid.uuid4())}) # Add firm name for context and ID
                
                valid_firms.append(legacy_firm)
            else:
                logging.debug(f"Skipping invalid firm entry: {f}") # ADDED: Logging

        # Validate performance metrics and integrate with firms (if they were in the top-level 'performance' array)
        for p in performance:
            metric_type = p.get('metric_type', '').strip()
            value = safe_get(p, 'value', '').strip()
            fund_name = p.get('fund_name', '').strip()
            
            if (metric_type and value and fund_name and
                metric_type.lower() != 'metric_type' and value.lower() != 'numeric_value_only_no_units' and
                len(value) > 0 and len(fund_name) > 2):
                
                valid_performance.append({
                    'id': str(uuid.uuid4()), # Assign ID
                    'fund_name': fund_name,
                    'metric_type': metric_type,
                    'value': value,
                    'period': p.get('period', 'Unknown'),
                    'date': p.get('date', 'Unknown'),
                    'benchmark': p.get('benchmark', 'Unknown'),
                    'additional_info': p.get('additional_info', 'Unknown')
                })
            else:
                logging.debug(f"Skipping invalid performance entry: {p}") # ADDED: Logging

        logging.info(f"AI extraction completed. Extracted: {len(valid_people)} people, {len(valid_firms)} firms, {len(valid_performance)} performance metrics.") # ADDED: Logging
        return valid_people, valid_firms, valid_performance

    except json.JSONDecodeError as e:
        st.error(f"JSON parsing error: {e}")
        st.warning("AI response might not be valid JSON. Please adjust prompt or input.")
        if hasattr(st.session_state, 'debug_mode') and st.session_state.debug_mode:
            st.code(response.text)
        logging.error(f"JSON parsing error in extract_single_chunk_safe: {e}. Raw response: {response.text}", exc_info=True) # CHANGED: Logging
        return [], [], []
    except Exception as e:
        st.error(f"Error during AI extraction: {e}")
        if hasattr(st.session_state, 'debug_mode') and st.session_state.debug_mode:
            st.exception(e)
        logging.error(f"General error during AI extraction in extract_single_chunk_safe: {e}", exc_info=True) # CHANGED: Logging
        return [], [], []


def aggregate_and_deduplicate(new_people, new_firms, new_performance_metrics, source_extraction_id):
    """
    Aggregates new extractions with existing session state data and deduplicates.
    Adds a new 'extraction' record linking to the source content.
    Prioritizes retaining data already in session state if duplicates are found.
    
    Args:
        new_people: List of newly extracted people data.
        new_firms: List of newly extracted firms data.
        new_performance_metrics: List of newly extracted performance metrics.
        source_extraction_id: The ID of the document/chunk this data was extracted from.
    """
    logging.info(f"Aggregating and deduplicating new data from source: {source_extraction_id}.")

    # People Deduplication
    existing_people_ids = {p['id'] for p in st.session_state.people}
    people_added = 0
    for person in new_people:
        if 'id' not in person:
            person['id'] = str(uuid.uuid4()) # Ensure an ID exists
        if person['id'] not in existing_people_ids:
            # Check for semantic duplicates (name + current company) before adding
            is_semantic_duplicate = False
            for existing_p in st.session_state.people:
                if (existing_p.get('name', '').lower() == person.get('name', '').lower() and
                    existing_p.get('current_company_name', '').lower() == person.get('current_company_name', '').lower() and
                    existing_p.get('current_title', '').lower() == person.get('current_title', '').lower()):
                    is_semantic_duplicate = True
                    logging.debug(f"Skipping semantic duplicate person: {person.get('name')} at {person.get('current_company_name')}")
                    break
            
            if not is_semantic_duplicate:
                st.session_state.people.append(person)
                existing_people_ids.add(person['id'])
                people_added += 1
                logging.debug(f"Added new person: {person.get('name')}")
        else:
            logging.debug(f"Skipping duplicate person (ID exists): {person.get('name')}")
    logging.info(f"Deduplication complete for people. Added {people_added} new records.")

    # Firm Deduplication
    existing_firms_ids = {f['id'] for f in st.session_state.firms}
    firms_added = 0
    for firm in new_firms:
        if 'id' not in firm:
            firm['id'] = str(uuid.uuid4()) # Ensure an ID exists
        if firm['id'] not in existing_firms_ids:
            # Check for semantic duplicates (firm name + type + location)
            is_semantic_duplicate = False
            for existing_f in st.session_state.firms:
                if (existing_f.get('name', '').lower() == firm.get('name', '').lower() and
                    existing_f.get('firm_type', '').lower() == firm.get('firm_type', '').lower() and
                    existing_f.get('location', '').lower() == firm.get('location', '').lower()):
                    is_semantic_duplicate = True
                    logging.debug(f"Skipping semantic duplicate firm: {firm.get('name')}")
                    break
            if not is_semantic_duplicate:
                st.session_state.firms.append(firm)
                existing_firms_ids.add(firm['id'])
                firms_added += 1
                logging.debug(f"Added new firm: {firm.get('name')}")
        else:
            logging.debug(f"Skipping duplicate firm (ID exists): {firm.get('name')}")
    logging.info(f"Deduplication complete for firms. Added {firms_added} new records.")

    # Performance Metrics Deduplication
    # Performance metrics are complex to deduplicate purely by content.
    # We'll link them to the firm and assume if they come from a new extraction
    # they are new or provide additional context. For simplicity, we'll avoid
    # deep semantic deduplication for now and add them if they have a new ID
    # or if the combination of firm+metric+period+value is unique.
    
    # Store performance metrics directly within the firm object if possible, or keep separate
    # For now, keeping them separate as it was in the original structure from AI.
    existing_performance_keys = set()
    for firm in st.session_state.firms:
        if firm.get('performance_metrics'):
            for metric in firm['performance_metrics']:
                key = (metric.get('fund_name', ''), metric.get('metric_type', ''),
                       metric.get('value', ''), metric.get('period', ''), metric.get('date', ''))
                existing_performance_keys.add(tuple(str(k).lower() for k in key))

    performance_added = 0
    # Map firm names to their IDs for linking performance metrics
    firm_name_to_id = {firm['name'].lower(): firm['id'] for firm in st.session_state.firms}

    for perf in new_performance_metrics:
        # Ensure ID for performance metric
        if 'id' not in perf:
            perf['id'] = str(uuid.uuid4())
            
        key = (perf.get('fund_name', ''), perf.get('metric_type', ''),
               perf.get('value', ''), perf.get('period', ''), perf.get('date', ''))
        
        if tuple(str(k).lower() for k in key) not in existing_performance_keys:
            # Find the firm to attach this performance metric to, or create a new one if not found
            firm_id = firm_name_to_id.get(perf['fund_name'].lower())
            
            if firm_id:
                for firm in st.session_state.firms:
                    if firm['id'] == firm_id:
                        if 'performance_metrics' not in firm:
                            firm['performance_metrics'] = []
                        # Add only if not already present in the specific firm's metrics
                        if perf not in firm['performance_metrics']:
                            firm['performance_metrics'].append(perf)
                            performance_added += 1
                            logging.debug(f"Added new performance metric for {perf.get('fund_name')}: {perf.get('metric_type')}")
                            existing_performance_keys.add(tuple(str(k).lower() for k in key))
                        break
            else:
                # If firm not found, maybe add a placeholder firm or log a warning
                logging.warning(f"Performance metric for unknown firm '{perf.get('fund_name')}' was not added to a firm. Creating a new firm entry for it.")
                # Automatically create a new firm if the fund_name doesn't match an existing firm
                new_placeholder_firm = {
                    "id": str(uuid.uuid4()),
                    "name": perf['fund_name'],
                    "firm_type": "Hedge Fund", # Default type
                    "location": "Unknown",
                    "strategy": "Unknown",
                    "aum": "Unknown",
                    "founded": "Unknown",
                    "website": "Unknown",
                    "description": "Auto-created from performance metric.",
                    "status": "operating",
                    "performance_metrics": [perf]
                }
                st.session_state.firms.append(new_placeholder_firm)
                firm_name_to_id[new_placeholder_firm['name'].lower()] = new_placeholder_firm['id']
                firms_added += 1 # Count as a new firm added due to performance metric
                performance_added += 1 # Count the performance metric too
                logging.debug(f"Created new placeholder firm '{new_placeholder_firm['name']}' and added its performance metric.")
        else:
            logging.debug(f"Skipping duplicate performance metric: {perf.get('metric_type')} for {perf.get('fund_name')}")
    logging.info(f"Deduplication complete for performance metrics. Added {performance_added} new records.")

    # Employments are derived from person and firm data, and usually created via modals or specific logic.
    # For extraction, we won't directly extract 'employments' as a top-level item.
    # Instead, we'll infer new employments from 'people' data by checking if a person's current_company_name
    # and previous_company is already reflected in the employments list.
    employments_added = 0
    for person in new_people:
        person_id = person['id']
        current_company_name = person.get('current_company_name')
        current_title = person.get('current_title')
        previous_company = person.get('previous_company')
        
        # Check current employment
        if current_company_name and current_company_name != 'Unknown':
            is_current_employment_tracked = any(
                emp.get('person_id') == person_id and 
                emp.get('company_name', '').lower() == current_company_name.lower() and
                emp.get('end_date') is None # Assuming current employment has no end date
                for emp in st.session_state.employments
            )
            if not is_current_employment_tracked:
                # Try to find a start date from existing history if available, or set to current year
                # This is a simplification; a full solution would require more complex date inference
                start_date_for_new_employment = date.today().replace(month=1, day=1) # Default to start of current year
                
                st.session_state.employments.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_id,
                    "company_name": current_company_name,
                    "title": current_title if current_title != 'Unknown' else "Unknown",
                    "start_date": start_date_for_new_employment,
                    "end_date": None,
                    "location": person.get('location', 'Unknown'),
                    "strategy": person.get('strategy', 'Unknown')
                })
                employments_added += 1
                logging.debug(f"Added current employment for {person.get('name')} at {current_company_name}")

        # Check previous employment
        if previous_company and previous_company != 'Unknown':
            is_previous_employment_tracked = any(
                emp.get('person_id') == person_id and 
                emp.get('company_name', '').lower() == previous_company.lower() and
                emp.get('end_date') is not None # Assuming previous employment has an end date
                for emp in st.session_state.employments
            )
            if not is_previous_employment_tracked:
                # Add previous employment with dummy dates for now
                st.session_state.employments.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_id,
                    "company_name": previous_company,
                    "title": "Unknown (Previous)",
                    "start_date": date.today() - timedelta(days=730), # 2 years ago as dummy
                    "end_date": date.today() - timedelta(days=365), # 1 year ago as dummy
                    "location": person.get('location', 'Unknown'),
                    "strategy": "Unknown"
                })
                employments_added += 1
                logging.debug(f"Added previous employment for {person.get('name')} at {previous_company}")
    logging.info(f"Deduplication complete for inferred employments. Added {employments_added} new records.")

    # Extractions Deduplication
    # This stores the raw JSON output from the AI for review/debugging.
    # It's unique per extraction attempt from a specific source chunk.
    existing_extractions_ids = {e['id'] for e in st.session_state.all_extractions}
    extractions_added = 0
    
    new_extraction_record = {
        "id": source_extraction_id,
        "timestamp": datetime.now().isoformat(),
        "extracted_people_ids": [p['id'] for p in new_people],
        "extracted_firm_ids": [f['id'] for f in new_firms],
        "extracted_performance_metrics_ids": [p['id'] for p in new_performance_metrics],
        # You might also want to store the raw text chunk or a hash of it here for traceability
    }
    
    if new_extraction_record['id'] not in existing_extractions_ids:
        st.session_state.all_extractions.append(new_extraction_record)
        extractions_added += 1
        logging.debug(f"Added new extraction record: {new_extraction_record['id']}")
    else:
        logging.debug(f"Skipping duplicate extraction record (ID exists): {new_extraction_record['id']}")
    logging.info(f"Deduplication complete for extraction records. Added {extractions_added} new records.")

# Helper to get unique values for dynamic inputs
def get_unique_values_from_session_state(table_name, field_name):
    """Extracts unique values for a given field from session state data."""
    logging.debug(f"Getting unique values for {field_name} from {table_name}.") # ADDED: Logging
    if table_name == 'people':
        return sorted(list(set(safe_get(p, field_name) for p in st.session_state.people if safe_get(p, field_name) != 'Unknown')))
    elif table_name == 'firms':
        return sorted(list(set(safe_get(f, field_name) for f in st.session_state.firms if safe_get(f, field_name) != 'Unknown')))
    return []

# --- Review System Functions (Simplified/Removed from main flow) ---
# The review system is simplified by disabling it in initialize_session_state
# and directly aggregating extracted data. The functions are kept but might not
# be actively used in the main flow if enable_review_mode is False.

def get_review_time_remaining():
    """Calculates time remaining for review before auto-saving."""
    # This function is now mostly for illustrative purposes if review mode is disabled
    if st.session_state.review_start_time:
        elapsed_time = (datetime.now() - st.session_state.review_start_time).total_seconds()
        remaining = st.session_state.auto_save_timeout - elapsed_time
        return max(0, remaining)
    return 0

def auto_save_pending_reviews():
    """This function is no longer actively used if review mode is disabled."""
    logging.info("Auto-saving pending review items (this should not be called if review mode is disabled).")
    return 0 # No actual saving if review mode is off

# --- Streamlit UI ---
initialize_session_state()

# Auto-save with review handling
current_time = datetime.now()
if 'last_auto_save' not in st.session_state:
    st.session_state.last_auto_save = current_time
    logging.info("Initialized 'last_auto_save' in session state.") # ADDED: Logging

# Auto-save every 30 seconds if there's data
time_since_save = (current_time - st.session_state.last_auto_save).total_seconds()
if time_since_save > 30 and (st.session_state.people or st.session_state.firms or st.session_state.all_extractions):
    logging.info(f"Auto-save condition met. Time since last save: {time_since_save:.2f} seconds.") # ADDED: Logging
    save_data_async() # Ensure async save is called
    st.session_state.last_auto_save = current_time
else:
    logging.debug(f"Auto-save condition not met. Time since last save: {time_since_save:.2f} seconds. Data exists: {bool(st.session_state.people or st.session_state.firms or st.session_state.all_extractions)}") # ADDED: Logging

# Display save status in the sidebar (optional)
if st.session_state.save_status:
    st.sidebar.text(st.session_state.save_status)
    # Clear status after a short delay or next interaction if desired

# Handle review timeout (THIS BLOCK IS EFFECTIVELY DISABLED IF st.session_state.enable_review_mode IS FALSE)
if st.session_state.enable_review_mode and st.session_state.pending_review_data and st.session_state.review_start_time:
    remaining_review_time = get_review_time_remaining() # ADDED: Variable for clarity
    if remaining_review_time <= 0:
        logging.info("Review timeout reached. Initiating auto-save of pending reviews.") # ADDED: Logging
        saved_count = auto_save_pending_reviews()
        if saved_count > 0:
            st.sidebar.success(f"⏰ Auto-saved {saved_count} items from review queue!")
            st.rerun()
    else:
        logging.debug(f"Review interface active. Time remaining: {remaining_review_time:.1f} seconds.") # ADDED: Logging

# Auto-refresh for review interface using Streamlit's built-in mechanisms (EFFECTIVELY DISABLED)
if st.session_state.enable_review_mode and st.session_state.show_review_interface and st.session_state.pending_review_data:
    remaining = get_review_time_remaining()
    if remaining > 0:
        pass # In a real app, you might use st.rerun or JS for auto-refresh


# --- Sidebar Navigation ---
st.sidebar.header("Navigation")
if st.sidebar.button("🧑‍💼 People Dashboard"):
    st.session_state.current_view = 'people'
    st.session_state.selected_person_id = None
    st.session_state.selected_firm_id = None
    st.rerun()
if st.sidebar.button("🏢 Firms Dashboard"):
    st.session_state.current_view = 'firms'
    st.session_state.selected_person_id = None
    st.session_state.selected_firm_id = None
    st.rerun()
if st.sidebar.button("⚙️ Data Extraction"):
    st.session_state.current_view = 'extraction'
    st.session_state.selected_person_id = None
    st.session_state.selected_firm_id = None
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.header("Data Management")
if st.sidebar.button("🔄 Manual Save"):
    save_data_async()
    st.sidebar.success("Manual save triggered!")

# --- Global Search in Sidebar ---
st.sidebar.markdown("---")
st.sidebar.header("🔍 Global Search")
st.session_state.global_search = st.sidebar.text_input("Enter search query", st.session_state.global_search)
if st.sidebar.button("Search All Data"):
    st.session_state.current_view = 'search_results'
    st.rerun()

# --- Debug Mode Toggle in Sidebar ---
st.sidebar.markdown("---")
st.sidebar.header("🐞 Debugging")
st.session_state.debug_mode = st.sidebar.checkbox("Enable Debug Mode", value=st.session_state.get('debug_mode', False))
if st.session_state.debug_mode:
    st.sidebar.info("Debug mode is ON. More details may be logged to console/file.")
    with st.sidebar.expander("Session State (Debug)"):
        st.json({k: v for k, v in st.session_state.items() if not k.startswith('_')}) # Avoid internal streamlit keys

# --- Main Content Area ---
st.title("Asian Hedge Fund Talent Intelligence Platform")

if st.session_state.current_view == 'people':
    st.header("🧑‍💼 People Dashboard")
    st.write(f"Total People: {len(st.session_state.people)}")
    
    # Add Person Button
    if st.button("➕ Add New Person"):
        st.session_state.show_add_person_modal = True
        st.session_state.edit_person_data = None # Clear any previous edit data

    # People Table
    people_df = pd.DataFrame(st.session_state.people)
    if not people_df.empty:
        people_df = people_df.set_index('id')
        
        # Display pagination for people
        items_per_page = 10
        total_pages = (len(people_df) + items_per_page - 1) // items_per_page
        start_idx = st.session_state.people_page * items_per_page
        end_idx = min(start_idx + items_per_page, len(people_df))
        
        paginated_people_df = people_df.iloc[start_idx:end_idx]
        
        st.dataframe(paginated_people_df[[
            'name', 'current_title', 'current_company_name', 'location', 
            'expertise', 'strategy', 'email', 'linkedin_profile_url'
        ]])
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.button("Previous Page (People)", disabled=(st.session_state.people_page == 0)):
                st.session_state.people_page -= 1
                st.rerun()
        with col2:
            st.write(f"Page {st.session_state.people_page + 1} of {total_pages}")
        with col3:
            if st.button("Next Page (People)", disabled=(st.session_state.people_page >= total_pages - 1)):
                st.session_state.people_page += 1
                st.rerun()

        # Selection for details/edit
        selected_person_name = st.selectbox(
            "Select a person to view/edit details:",
            options=[''] + sorted(people_df['name'].tolist()),
            key='select_person_to_edit'
        )
        if selected_person_name:
            st.session_state.selected_person_id = people_df[people_df['name'] == selected_person_name].index[0]
            st.session_state.show_edit_person_modal = True
            st.session_state.edit_person_data = people_df.loc[st.session_state.selected_person_id].to_dict()
            st.rerun() # Rerun to show modal
    else:
        st.info("No people records yet. Use 'Add New Person' or 'Data Extraction' to add some.")

elif st.session_state.current_view == 'firms':
    st.header("🏢 Firms Dashboard")
    st.write(f"Total Firms: {len(st.session_state.firms)}")

    # Add Firm Button
    if st.button("➕ Add New Firm"):
        st.session_state.show_add_firm_modal = True
        st.session_state.edit_firm_data = None # Clear any previous edit data

    # Firms Table
    firms_df = pd.DataFrame(st.session_state.firms)
    if not firms_df.empty:
        firms_df = firms_df.set_index('id')
        
        # Display pagination for firms
        items_per_page = 10
        total_pages = (len(firms_df) + items_per_page - 1) // items_per_page
        start_idx = st.session_state.firms_page * items_per_page
        end_idx = min(start_idx + items_per_page, len(firms_df))
        
        paginated_firms_df = firms_df.iloc[start_idx:end_idx]
        
        st.dataframe(paginated_firms_df[[
            'name', 'firm_type', 'strategy', 'location', 'aum', 'founded', 'website'
        ]])
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.button("Previous Page (Firms)", disabled=(st.session_state.firms_page == 0)):
                st.session_state.firms_page -= 1
                st.rerun()
        with col2:
            st.write(f"Page {st.session_state.firms_page + 1} of {total_pages}")
        with col3:
            if st.button("Next Page (Firms)", disabled=(st.session_state.firms_page >= total_pages - 1)):
                st.session_state.firms_page += 1
                st.rerun()

        # Selection for details/edit
        selected_firm_name = st.selectbox(
            "Select a firm to view/edit details:",
            options=[''] + sorted(firms_df['name'].tolist()),
            key='select_firm_to_edit'
        )
        if selected_firm_name:
            st.session_state.selected_firm_id = firms_df[firms_df['name'] == selected_firm_name].index[0]
            st.session_state.show_edit_firm_modal = True
            st.session_state.edit_firm_data = firms_df.loc[st.session_state.selected_firm_id].to_dict()
            st.rerun() # Rerun to show modal
    else:
        st.info("No firm records yet. Use 'Add New Firm' or 'Data Extraction' to add some.")

elif st.session_state.current_view == 'extraction':
    st.header("⚙️ AI-Powered Data Extraction")
    st.info("Upload text files (e.g., newsletters, articles) for AI extraction of hedge fund intelligence.")

    if not GENAI_AVAILABLE:
        st.error("Google Generative AI library is not installed. Please install it to use AI features: `pip install google-generativeai`")
        st.stop() # Stop further execution if AI not available

    api_key = st.text_input("Enter your Google Gemini API Key:", type="password", help="Get your API key from Google AI Studio: https://aistudio.google.com/app/apikey")
    
    if api_key:
        model = setup_gemini(api_key)
        if model:
            st.success("Gemini model loaded. Ready for extraction!")
            
            # Preprocessing options
            st.subheader("Text Preprocessing Options")
            st.session_state.preprocessing_mode = st.radio(
                "Select preprocessing mode:",
                options=["balanced", "minimal", "aggressive", "none"],
                index=["balanced", "minimal", "aggressive", "none"].index(st.session_state.preprocessing_mode),
                help="Balanced: removes common noise. Minimal: light cleaning. Aggressive: heavy cleaning, may remove some context. None: no cleaning."
            )
            
            st.session_state.chunk_size_preference = st.radio(
                "Preferred text chunking for AI (for very large files):",
                options=["auto", "small", "medium", "large"],
                index=["auto", "small", "medium", "large"].index(st.session_state.chunk_size_preference),
                help="Auto: Let the system decide. Small: ~500 words. Medium: ~1000 words. Large: ~2000 words. Adjust if you face API limits or context issues."
            )

            uploaded_file = st.file_uploader("Upload a text file (.txt)", type=["txt"], key="newsletter_file_uploader")
            
            if uploaded_file is not None:
                success, file_content, error_msg = load_file_content(uploaded_file)
                
                if success:
                    preprocessed_text = preprocess_newsletter_text(file_content, st.session_state.preprocessing_mode)
                    
                    # Determine chunk size based on preference and model limits
                    # Gemini 1.5 Flash has a context window of 1 million tokens.
                    # A typical word is about 1.3 tokens. So 1000 words is ~1300 tokens.
                    # We can send fairly large chunks.
                    
                    max_chunk_size_tokens = 900000 # Keep well below 1M token limit
                    
                    if st.session_state.chunk_size_preference == "small":
                        target_words = 500
                    elif st.session_state.chunk_size_preference == "medium":
                        target_words = 1000
                    elif st.session_state.chunk_size_preference == "large":
                        target_words = 2000
                    else: # "auto"
                        # For 'auto', we can adjust based on the total size of the preprocessed text.
                        # If text is small, send as one chunk. If large, use larger chunks.
                        if len(preprocessed_text.split()) < 3000: # If less than ~3000 words
                            target_words = len(preprocessed_text.split()) # Send as one chunk
                        else:
                            target_words = 1500 # Default for larger files
                    
                    chunk_size = int(target_words * 5) # Rough estimate: 5 chars per word + spaces
                    
                    # Split text into chunks
                    chunks = []
                    current_chunk = ""
                    for paragraph in preprocessed_text.split('\n\n'):
                        if len(current_chunk) + len(paragraph) + 2 < chunk_size:
                            current_chunk += (paragraph + "\n\n")
                        else:
                            if current_chunk:
                                chunks.append(current_chunk.strip())
                            current_chunk = paragraph + "\n\n"
                    if current_chunk:
                        chunks.append(current_chunk.strip())

                    st.info(f"📄 Text split into {len(chunks)} chunks for AI processing (Target words per chunk: {target_words}).")
                    logging.info(f"Text split into {len(chunks)} chunks. Max chunk size: {chunk_size} chars.")

                    if st.button("🚀 Start AI Extraction"):
                        all_extracted_people = []
                        all_extracted_firms = []
                        all_extracted_performance = []

                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        total_chunks = len(chunks)
                        for i, chunk in enumerate(chunks):
                            status_text.text(f"Processing chunk {i+1}/{total_chunks}...")
                            progress_bar.progress((i + 1) / total_chunks)
                            
                            with st.spinner(f"Extracting data from chunk {i+1}/{total_chunks}..."):
                                try:
                                    extracted_people, extracted_firms, extracted_performance = extract_single_chunk_safe(chunk, model)
                                    all_extracted_people.extend(extracted_people)
                                    all_extracted_firms.extend(extracted_firms)
                                    all_extracted_performance.extend(extracted_performance)
                                    logging.info(f"Chunk {i+1} processed. Extracted {len(extracted_people)} people, {len(extracted_firms)} firms, {len(extracted_performance)} performance metrics.")
                                except Exception as e:
                                    st.error(f"Error processing chunk {i+1}: {e}")
                                    logging.error(f"Error processing chunk {i+1}: {e}", exc_info=True)
                                    continue # Continue to next chunk even if one fails
                                
                                # Add a small delay to avoid hitting rate limits quickly and for better UX
                                time.sleep(0.5) 

                        # Generate a unique ID for this extraction run
                        extraction_run_id = str(uuid.uuid4())
                        
                        # Aggregate and deduplicate all extracted data
                        aggregate_and_deduplicate(all_extracted_people, all_extracted_firms, all_extracted_performance, extraction_run_id)
                        
                        st.success(f"🎉 Extraction complete! Added {len(all_extracted_people)} new people, {len(all_extracted_firms)} new firms, and {len(all_extracted_performance)} new performance metrics.")
                        status_text.empty()
                        progress_bar.empty()
                        
                        # Trigger an async save after extraction
                        save_data_async()
                        st.info("Data saved after extraction.")
                        
                        # Optionally switch to a dashboard view to see the results
                        st.session_state.current_view = 'people' # Or 'firms' or a summary view
                        st.rerun()

                else:
                    st.error(f"Failed to load file: {error_msg}")
        else:
            st.warning("Please enter a valid Google Gemini API Key to enable AI extraction.")

elif st.session_state.current_view == 'search_results':
    st.header(f"🔍 Search Results for \"{st.session_state.global_search}\"")
    matching_people, matching_firms, matching_metrics = enhanced_global_search(st.session_state.global_search)
    
    st.subheader(f"People ({len(matching_people)})")
    if matching_people:
        st.dataframe(pd.DataFrame(matching_people)[[
            'name', 'current_title', 'current_company_name', 'location', 'expertise'
        ]])
    else:
        st.info("No matching people found.")

    st.subheader(f"Firms ({len(matching_firms)})")
    if matching_firms:
        st.dataframe(pd.DataFrame(matching_firms)[[
            'name', 'firm_type', 'strategy', 'location', 'aum'
        ]])
    else:
        st.info("No matching firms found.")

    st.subheader(f"Performance Metrics ({len(matching_metrics)})")
    if matching_metrics:
        st.dataframe(pd.DataFrame(matching_metrics)[[
            'fund_name', 'metric_type', 'value', 'period', 'date', 'additional_info'
        ]])
    else:
        st.info("No matching performance metrics found.")

# --- Modals for Add/Edit Person/Firm (Keep these as they are core to data entry) ---

# Add Person Modal
if st.session_state.show_add_person_modal:
    with st.form("add_person_form"):
        st.subheader("Add New Person")
        new_person_name = st.text_input("Name", key="new_person_name")
        new_person_current_title = handle_dynamic_input("current_title", "", "people", context="new_person")
        new_person_current_company_name = handle_dynamic_input("current_company_name", "", "firms", context="new_person") # Firms for company suggestions
        new_person_location = handle_dynamic_input("location", "", "people", context="new_person")
        new_person_email = st.text_input("Email", key="new_person_email")
        new_person_linkedin_profile_url = st.text_input("LinkedIn Profile URL", key="new_person_linkedin_profile_url")
        new_person_phone = st.text_input("Phone", key="new_person_phone")
        new_person_education = st.text_area("Education (comma-separated)", key="new_person_education")
        new_person_expertise = st.text_area("Expertise (comma-separated)", key="new_person_expertise")
        new_person_aum_managed = st.text_input("AUM Managed (e.g., '1.5B USD')", key="new_person_aum_managed")
        new_person_strategy = st.text_area("Strategy", key="new_person_strategy")

        submitted = st.form_submit_button("Add Person")
        if submitted:
            if new_person_name and new_person_current_company_name:
                person_id = str(uuid.uuid4())
                st.session_state.people.append({
                    "id": person_id,
                    "name": new_person_name,
                    "current_title": new_person_current_title if new_person_current_title else "Unknown",
                    "current_company_name": new_person_current_company_name if new_person_current_company_name else "Unknown",
                    "location": new_person_location if new_person_location else "Unknown",
                    "email": new_person_email if new_person_email else "Unknown",
                    "linkedin_profile_url": new_person_linkedin_profile_url if new_person_linkedin_profile_url else "Unknown",
                    "phone": new_person_phone if new_person_phone else "Unknown",
                    "education": new_person_education if new_person_education else "Unknown",
                    "expertise": new_person_expertise if new_person_expertise else "Unknown",
                    "aum_managed": new_person_aum_managed if new_person_aum_managed else "Unknown",
                    "strategy": new_person_strategy if new_person_strategy else "Unknown"
                })
                # Add current employment if company is specified
                if new_person_current_company_name:
                    st.session_state.employments.append({
                        "id": str(uuid.uuid4()),
                        "person_id": person_id,
                        "company_name": new_person_current_company_name,
                        "title": new_person_current_title if new_person_current_title else "Unknown",
                        "start_date": date.today(),
                        "end_date": None,
                        "location": new_person_location if new_person_location else "Unknown",
                        "strategy": new_person_strategy if new_person_strategy else "Unknown"
                    })
                save_data_async()
                st.success(f"Person '{new_person_name}' added!")
                st.session_state.show_add_person_modal = False
                st.rerun()
            else:
                st.error("Name and Current Company are required.")
    if st.button("Cancel Add Person"):
        st.session_state.show_add_person_modal = False
        st.rerun()

# Edit Person Modal
if st.session_state.show_edit_person_modal and st.session_state.edit_person_data:
    person_data = st.session_state.edit_person_data
    with st.form("edit_person_form"):
        st.subheader(f"Edit Person: {person_data.get('name', '')}")
        edited_name = st.text_input("Name", value=person_data.get('name', ''), key="edit_person_name")
        edited_current_title = handle_dynamic_input("current_title", person_data.get('current_title', ''), "people", context=person_data['id'])
        edited_current_company_name = handle_dynamic_input("current_company_name", person_data.get('current_company_name', ''), "firms", context=person_data['id'])
        edited_location = handle_dynamic_input("location", person_data.get('location', ''), "people", context=person_data['id'])
        edited_email = st.text_input("Email", value=person_data.get('email', ''), key="edit_person_email")
        edited_linkedin_profile_url = st.text_input("LinkedIn Profile URL", value=person_data.get('linkedin_profile_url', ''), key="edit_person_linkedin_profile_url")
        edited_phone = st.text_input("Phone", value=person_data.get('phone', ''), key="edit_person_phone")
        edited_education = st.text_area("Education (comma-separated)", value=person_data.get('education', ''), key="edit_person_education")
        edited_expertise = st.text_area("Expertise (comma-separated)", value=person_data.get('expertise', ''), key="edit_person_expertise")
        edited_aum_managed = st.text_input("AUM Managed (e.g., '1.5B USD')", value=person_data.get('aum_managed', ''), key="edit_person_aum_managed")
        edited_strategy = st.text_area("Strategy", value=person_data.get('strategy', ''), key="edit_person_strategy")

        # Display Employment History for selected person
        st.subheader("Employment History")
        person_employments = [emp for emp in st.session_state.employments if emp['person_id'] == person_data['id']]
        if person_employments:
            employment_df = pd.DataFrame(person_employments)
            # Convert date objects to strings for display
            employment_df['start_date'] = employment_df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else 'Present')
            employment_df['end_date'] = employment_df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else 'Present')
            st.dataframe(employment_df[['company_name', 'title', 'start_date', 'end_date', 'location', 'strategy']])
        else:
            st.info("No employment history recorded for this person.")

        col_save, col_add_emp, col_delete = st.columns([1, 1, 1])
        with col_save:
            submitted = st.form_submit_button("Save Changes")
        with col_add_emp:
            add_employment_button = st.form_submit_button("Add Employment") # Not a form submit button for actual add
        with col_delete:
            delete_button = st.form_submit_button("Delete Person", help="Permanently delete this person and their employments.")

        if submitted:
            # Update the person's data
            for i, p in enumerate(st.session_state.people):
                if p['id'] == person_data['id']:
                    st.session_state.people[i].update({
                        "name": edited_name,
                        "current_title": edited_current_title,
                        "current_company_name": edited_current_company_name,
                        "location": edited_location,
                        "email": edited_email,
                        "linkedin_profile_url": edited_linkedin_profile_url,
                        "phone": edited_phone,
                        "education": edited_education,
                        "expertise": edited_expertise,
                        "aum_managed": edited_aum_managed,
                        "strategy": edited_strategy
                    })
                    break
            
            # Update current employment in employments list
            found_current_employment = False
            for emp in st.session_state.employments:
                if emp['person_id'] == person_data['id'] and emp['end_date'] is None:
                    emp['company_name'] = edited_current_company_name
                    emp['title'] = edited_current_title
                    emp['location'] = edited_location
                    found_current_employment = True
                    break
            if not found_current_employment and edited_current_company_name: # If no current employment found, add one
                 st.session_state.employments.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_data['id'],
                    "company_name": edited_current_company_name,
                    "title": edited_current_title,
                    "start_date": date.today(), # Default to today if new
                    "end_date": None,
                    "location": edited_location,
                    "strategy": edited_strategy
                })
            
            save_data_async()
            st.success(f"Changes to '{edited_name}' saved!")
            st.session_state.show_edit_person_modal = False
            st.session_state.selected_person_id = None
            st.rerun()
        
        if add_employment_button:
            st.session_state.add_employment_for_person_id = person_data['id']
            st.session_state.show_add_employment_modal = True
            # No rerun here, let the add employment modal logic handle it
        
        if delete_button:
            if st.warning(f"Are you sure you want to delete {person_data.get('name')} and all their employment records? This action cannot be undone."):
                if st.button("Confirm Delete"):
                    # Delete person
                    st.session_state.people = [p for p in st.session_state.people if p['id'] != person_data['id']]
                    # Delete associated employments
                    st.session_state.employments = [emp for emp in st.session_state.employments if emp['person_id'] != person_data['id']]
                    save_data_async()
                    st.success(f"Person '{person_data.get('name')}' and associated employments deleted.")
                    st.session_state.show_edit_person_modal = False
                    st.session_state.selected_person_id = None
                    st.rerun()
    if st.button("Cancel Edit Person"):
        st.session_state.show_edit_person_modal = False
        st.session_state.selected_person_id = None
        st.rerun()

# Add Employment Modal (triggered from Edit Person)
if st.session_state.get('show_add_employment_modal', False) and st.session_state.get('add_employment_for_person_id'):
    person_id_for_employment = st.session_state.add_employment_for_person_id
    person_name_for_employment = next((p['name'] for p in st.session_state.people if p['id'] == person_id_for_employment), "Unknown Person")
    
    with st.form("add_employment_form"):
        st.subheader(f"Add New Employment for {person_name_for_employment}")
        new_emp_company_name = handle_dynamic_input("company_name", "", "firms", context=f"new_emp_{person_id_for_employment}")
        new_emp_title = st.text_input("Title", key="new_emp_title")
        new_emp_start_date = st.date_input("Start Date", value=date.today(), key="new_emp_start_date")
        new_emp_end_date = st.date_input("End Date (leave blank for current)", value=None, key="new_emp_end_date", min_value=new_emp_start_date)
        new_emp_location = handle_dynamic_input("location", "", "people", context=f"new_emp_loc_{person_id_for_employment}")
        new_emp_strategy = st.text_area("Strategy", key="new_emp_strategy")

        submitted_emp = st.form_submit_button("Add Employment Record")
        if submitted_emp:
            if new_emp_company_name and new_emp_title:
                st.session_state.employments.append({
                    "id": str(uuid.uuid4()),
                    "person_id": person_id_for_employment,
                    "company_name": new_emp_company_name,
                    "title": new_emp_title,
                    "start_date": new_emp_start_date,
                    "end_date": new_emp_end_date,
                    "location": new_emp_location if new_emp_location else "Unknown",
                    "strategy": new_emp_strategy if new_emp_strategy else "Unknown"
                })
                save_data_async()
                st.success(f"Employment at '{new_emp_company_name}' added for {person_name_for_employment}!")
                st.session_state.show_add_employment_modal = False
                st.session_state.add_employment_for_person_id = None
                st.rerun()
            else:
                st.error("Company Name and Title are required for employment.")
    if st.button("Cancel Add Employment"):
        st.session_state.show_add_employment_modal = False
        st.session_state.add_employment_for_person_id = None
        st.rerun()


# Add Firm Modal
if st.session_state.show_add_firm_modal:
    with st.form("add_firm_form"):
        st.subheader("Add New Firm")
        new_firm_name = st.text_input("Firm Name", key="new_firm_name")
        new_firm_type = handle_dynamic_input("firm_type", "", "firms", context="new_firm_type")
        new_firm_strategy = st.text_area("Strategy", key="new_firm_strategy")
        new_firm_location = handle_dynamic_input("location", "", "firms", context="new_firm_location")
        new_firm_headquarters = st.text_input("Headquarters", key="new_firm_headquarters")
        new_firm_aum = st.text_input("AUM (e.g., '50B USD')", key="new_firm_aum")
        new_firm_founded = st.text_input("Founded Year (YYYY)", key="new_firm_founded")
        new_firm_website = st.text_input("Website", key="new_firm_website")
        new_firm_description = st.text_area("Description", key="new_firm_description")
        new_firm_status = st.selectbox("Status", options=["operating", "launching", "expanding", "closing", "acquired"], key="new_firm_status")

        submitted = st.form_submit_button("Add Firm")
        if submitted:
            if new_firm_name and new_firm_type:
                st.session_state.firms.append({
                    "id": str(uuid.uuid4()),
                    "name": new_firm_name,
                    "firm_type": new_firm_type,
                    "strategy": new_firm_strategy if new_firm_strategy else "Unknown",
                    "location": new_firm_location if new_firm_location else "Unknown",
                    "headquarters": new_firm_headquarters if new_firm_headquarters else "Unknown",
                    "aum": new_firm_aum if new_firm_aum else "Unknown",
                    "founded": new_firm_founded if new_firm_founded else "Unknown",
                    "website": new_firm_website if new_firm_website else "Unknown",
                    "description": new_firm_description if new_firm_description else "Unknown",
                    "status": new_firm_status
                })
                save_data_async()
                st.success(f"Firm '{new_firm_name}' added!")
                st.session_state.show_add_firm_modal = False
                st.rerun()
            else:
                st.error("Firm Name and Type are required.")
    if st.button("Cancel Add Firm"):
        st.session_state.show_add_firm_modal = False
        st.rerun()

# Edit Firm Modal
if st.session_state.show_edit_firm_modal and st.session_state.edit_firm_data:
    firm_data = st.session_state.edit_firm_data
    with st.form("edit_firm_form"):
        st.subheader(f"Edit Firm: {firm_data.get('name', '')}")
        edited_name = st.text_input("Firm Name", value=firm_data.get('name', ''), key="edit_firm_name")
        edited_firm_type = handle_dynamic_input("firm_type", firm_data.get('firm_type', ''), "firms", context=firm_data['id'])
        edited_strategy = st.text_area("Strategy", value=firm_data.get('strategy', ''), key="edit_firm_strategy")
        edited_location = handle_dynamic_input("location", firm_data.get('location', ''), "firms", context=firm_data['id'])
        edited_headquarters = st.text_input("Headquarters", value=firm_data.get('headquarters', ''), key="edit_firm_headquarters")
        edited_aum = st.text_input("AUM (e.g., '50B USD')", value=firm_data.get('aum', ''), key="edit_firm_aum")
        edited_founded = st.text_input("Founded Year (YYYY)", value=firm_data.get('founded', ''), key="edit_firm_founded")
        edited_website = st.text_input("Website", value=firm_data.get('website', ''), key="edit_firm_website")
        edited_description = st.text_area("Description", value=firm_data.get('description', ''), key="edit_firm_description")
        edited_status = st.selectbox("Status", options=["operating", "launching", "expanding", "closing", "acquired"], index=["operating", "launching", "expanding", "closing", "acquired"].index(firm_data.get('status', 'operating')), key="edit_firm_status")

        # Display Performance Metrics for selected firm
        st.subheader("Performance Metrics")
        if firm_data.get('performance_metrics'):
            metrics_df = pd.DataFrame(firm_data['performance_metrics'])
            st.dataframe(metrics_df[['metric_type', 'value', 'period', 'date', 'additional_info']])
        else:
            st.info("No performance metrics recorded for this firm.")
        
        # Add a button to add new performance metric
        if st.button("➕ Add New Performance Metric", key="add_metric_button"):
            st.session_state.add_metric_for_firm_id = firm_data['id']
            st.session_state.show_add_metric_modal = True
            # No rerun here, let the modal handle it

        col_save, col_delete = st.columns(2)
        with col_save:
            submitted = st.form_submit_button("Save Changes")
        with col_delete:
            delete_button = st.form_submit_button("Delete Firm", help="Permanently delete this firm and its performance metrics.")

        if submitted:
            for i, f in enumerate(st.session_state.firms):
                if f['id'] == firm_data['id']:
                    st.session_state.firms[i].update({
                        "name": edited_name,
                        "firm_type": edited_firm_type,
                        "strategy": edited_strategy,
                        "location": edited_location,
                        "headquarters": edited_headquarters,
                        "aum": edited_aum,
                        "founded": edited_founded,
                        "website": edited_website,
                        "description": edited_description,
                        "status": edited_status
                    })
                    break
            save_data_async()
            st.success(f"Changes to '{edited_name}' saved!")
            st.session_state.show_edit_firm_modal = False
            st.session_state.selected_firm_id = None
            st.rerun()
        
        if delete_button:
            if st.warning(f"Are you sure you want to delete {firm_data.get('name')} and all its performance metrics? This action cannot be undone."):
                if st.button("Confirm Delete Firm"):
                    st.session_state.firms = [f for f in st.session_state.firms if f['id'] != firm_data['id']]
                    # No need to delete employments if they are linked by firm ID and not explicitly tied to a firm in the firm's structure itself
                    save_data_async()
                    st.success(f"Firm '{firm_data.get('name')}' deleted.")
                    st.session_state.show_edit_firm_modal = False
                    st.session_state.selected_firm_id = None
                    st.rerun()

    if st.button("Cancel Edit Firm"):
        st.session_state.show_edit_firm_modal = False
        st.session_state.selected_firm_id = None
        st.rerun()

# Add Performance Metric Modal (triggered from Edit Firm)
if st.session_state.get('show_add_metric_modal', False) and st.session_state.get('add_metric_for_firm_id'):
    firm_id_for_metric = st.session_state.add_metric_for_firm_id
    firm_name_for_metric = next((f['name'] for f in st.session_state.firms if f['id'] == firm_id_for_metric), "Unknown Firm")
    
    with st.form("add_metric_form"):
        st.subheader(f"Add New Performance Metric for {firm_name_for_metric}")
        new_metric_type = st.selectbox("Metric Type", options=["return", "irr", "sharpe", "information_ratio", "drawdown", "alpha", "beta", "volatility", "aum", "tracking_error", "correlation"], key="new_metric_type")
        new_metric_value = st.text_input("Value (numeric only, e.g., '12.5' or '1.8')", key="new_metric_value")
        new_metric_period = st.selectbox("Period", options=["YTD", "Q1", "Q2", "Q3", "Q4", "1Y", "3Y", "5Y", "ITD", "Monthly", "Current", "Unknown"], key="new_metric_period")
        new_metric_date = st.text_input("Date (YYYY or MM-DD-YYYY)", value=str(date.today().year), key="new_metric_date")
        new_metric_benchmark = st.text_input("Benchmark (if applicable)", key="new_metric_benchmark")
        new_metric_info = st.text_area("Additional Info (units, context)", key="new_metric_info")

        submitted_metric = st.form_submit_button("Add Performance Metric")
        if submitted_metric:
            if new_metric_type and new_metric_value:
                # Find the firm and add the metric
                for i, f in enumerate(st.session_state.firms):
                    if f['id'] == firm_id_for_metric:
                        if 'performance_metrics' not in st.session_state.firms[i]:
                            st.session_state.firms[i]['performance_metrics'] = []
                        st.session_state.firms[i]['performance_metrics'].append({
                            "id": str(uuid.uuid4()),
                            "fund_name": firm_name_for_metric, # Store firm name for easier reference
                            "metric_type": new_metric_type,
                            "value": new_metric_value,
                            "period": new_metric_period,
                            "date": new_metric_date,
                            "benchmark": new_metric_benchmark if new_metric_benchmark else "Unknown",
                            "additional_info": new_metric_info if new_metric_info else "Unknown"
                        })
                        break
                save_data_async()
                st.success(f"Performance metric '{new_metric_type}' added for {firm_name_for_metric}!")
                st.session_state.show_add_metric_modal = False
                st.session_state.add_metric_for_firm_id = None
                st.rerun()
            else:
                st.error("Metric Type and Value are required.")
    if st.button("Cancel Add Performance Metric"):
        st.session_state.show_add_metric_modal = False
        st.session_state.add_metric_for_firm_id = None
        st.rerun()


# --- Data Export Section ---
st.sidebar.markdown("---")
st.sidebar.header("📥 Export Data")

@st.cache_data(ttl=3600)
def convert_df_to_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data(ttl=3600)
def convert_df_to_excel(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def export_all_data_to_excel_zip(people_data, firms_data, employments_data, extractions_data):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        # Export People
        if people_data:
            people_df = pd.DataFrame(people_data)
            people_csv = people_df.to_csv(index=False, encoding='utf-8')
            zip_file.writestr("people_data.csv", people_csv)
            if EXCEL_AVAILABLE:
                people_excel_io = BytesIO()
                with pd.ExcelWriter(people_excel_io, engine='openpyxl') as writer:
                    people_df.to_excel(writer, index=False, sheet_name='People')
                zip_file.writestr("people_data.xlsx", people_excel_io.getvalue())
        
        # Export Firms
        if firms_data:
            # Flatten firms data to include top-level firm info and performance metrics
            firms_flat_data = []
            for firm in firms_data:
                base_firm = {k: v for k, v in firm.items() if k != 'performance_metrics'}
                if firm.get('performance_metrics'):
                    for metric in firm['performance_metrics']:
                        firms_flat_data.append({**base_firm, **metric})
                else:
                    firms_flat_data.append(base_firm) # Add firms without metrics too
            
            firms_df = pd.DataFrame(firms_flat_data)
            firms_csv = firms_df.to_csv(index=False, encoding='utf-8')
            zip_file.writestr("firms_data.csv", firms_csv)
            if EXCEL_AVAILABLE:
                firms_excel_io = BytesIO()
                with pd.ExcelWriter(firms_excel_io, engine='openpyxl') as writer:
                    firms_df.to_excel(writer, index=False, sheet_name='Firms')
                zip_file.writestr("firms_data.xlsx", firms_excel_io.getvalue())

        # Export Employments
        if employments_data:
            employments_df = pd.DataFrame(employments_data)
            # Convert date objects to string for export
            employments_df['start_date'] = employments_df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
            employments_df['end_date'] = employments_df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
            
            employments_csv = employments_df.to_csv(index=False, encoding='utf-8')
            zip_file.writestr("employments_data.csv", employments_csv)
            if EXCEL_AVAILABLE:
                employments_excel_io = BytesIO()
                with pd.ExcelWriter(employments_excel_io, engine='openpyxl') as writer:
                    employments_df.to_excel(writer, index=False, sheet_name='Employments')
                zip_file.writestr("employments_data.xlsx", employments_excel_io.getvalue())
        
        # Export Extractions
        if extractions_data:
            extractions_df = pd.DataFrame(extractions_data)
            extractions_csv = extractions_df.to_csv(index=False, encoding='utf-8')
            zip_file.writestr("extractions_data.csv", extractions_csv)
            if EXCEL_AVAILABLE:
                extractions_excel_io = BytesIO()
                with pd.ExcelWriter(extractions_excel_io, engine='openpyxl') as writer:
                    extractions_df.to_excel(writer, index=False, sheet_name='Extractions')
                zip_file.writestr("extractions_data.xlsx", extractions_excel_io.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer

st.sidebar.markdown("---")
st.sidebar.header("Export Extracted Data")

# Export All Data as ZIP
if st.sidebar.button("📦 Export All Data (.zip of CSV/Excel)"):
    try:
        if st.session_state.people or st.session_state.firms or st.session_state.employments or st.session_state.all_extractions:
            zip_output = export_all_data_to_excel_zip(
                st.session_state.people, 
                st.session_state.firms, 
                st.session_state.employments, 
                st.session_state.all_extractions
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.sidebar.download_button(
                label="Download All Data (ZIP)",
                data=zip_output.getvalue(),
                file_name=f"hedge_fund_data_export_{timestamp}.zip",
                mime="application/zip",
                use_container_width=True
            )
            st.sidebar.success("✅ All data packaged for download!")
        else:
            st.sidebar.warning("No data available to export.")
    except Exception as e:
        st.sidebar.error(f"Error creating ZIP export: {str(e)}")

# Individual CSV exports
st.sidebar.subheader("Export Individual CSVs")
if st.session_state.people:
    people_export_df = pd.DataFrame(st.session_state.people)
    st.sidebar.download_button(
        label="Download People CSV",
        data=convert_df_to_csv(people_export_df),
        file_name="people_data.csv",
        mime="text/csv",
        key="download_people_csv",
        use_container_width=True
    )
else:
    st.sidebar.info("No people data to export (CSV).")

if st.session_state.firms:
    # Flatten firms data for CSV export to include performance metrics alongside firm info
    firms_flat_data = []
    for firm in st.session_state.firms:
        base_firm = {k: v for k, v in firm.items() if k != 'performance_metrics'}
        if firm.get('performance_metrics'):
            for metric in firm['performance_metrics']:
                firms_flat_data.append({**base_firm, **metric})
        else:
            firms_flat_data.append(base_firm) # Add firms without metrics too
    
    firms_export_df = pd.DataFrame(firms_flat_data)
    st.sidebar.download_button(
        label="Download Firms & Metrics CSV",
        data=convert_df_to_csv(firms_export_df),
        file_name="firms_data.csv",
        mime="text/csv",
        key="download_firms_csv",
        use_container_width=True
    )
else:
    st.sidebar.info("No firm data to export (CSV).")

if st.session_state.employments:
    employments_export_df = pd.DataFrame(st.session_state.employments)
    employments_export_df['start_date'] = employments_export_df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
    employments_export_df['end_date'] = employments_export_df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
    st.sidebar.download_button(
        label="Download Employments CSV",
        data=convert_df_to_csv(employments_export_df),
        file_name="employments_data.csv",
        mime="text/csv",
        key="download_employments_csv",
        use_container_width=True
    )
else:
    st.sidebar.info("No employment data to export (CSV).")

if st.session_state.all_extractions:
    extractions_export_df = pd.DataFrame(st.session_state.all_extractions)
    st.sidebar.download_button(
        label="Download Extractions Log CSV",
        data=convert_df_to_csv(extractions_export_df),
        file_name="extractions_log.csv",
        mime="text/csv",
        key="download_extractions_csv",
        use_container_width=True
    )
else:
    st.sidebar.info("No extraction log data to export (CSV).")

# Individual Excel exports
if EXCEL_AVAILABLE:
    st.sidebar.subheader("Export Individual Excels")
    if st.session_state.people:
        people_export_df = pd.DataFrame(st.session_state.people)
        st.sidebar.download_button(
            label="Download People Excel",
            data=convert_df_to_excel(people_export_df),
            file_name="people_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_people_excel",
            use_container_width=True
        )
    else:
        st.sidebar.info("No people data to export (Excel).")

    if st.session_state.firms:
        firms_flat_data = []
        for firm in st.session_state.firms:
            base_firm = {k: v for k, v in firm.items() if k != 'performance_metrics'}
            if firm.get('performance_metrics'):
                for metric in firm['performance_metrics']:
                    firms_flat_data.append({**base_firm, **metric})
            else:
                firms_flat_data.append(base_firm)
        firms_export_df = pd.DataFrame(firms_flat_data)
        st.sidebar.download_button(
            label="Download Firms & Metrics Excel",
            data=convert_df_to_excel(firms_export_df),
            file_name="firms_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_firms_excel",
            use_container_width=True
        )
    else:
        st.sidebar.info("No firm data to export (Excel).")

    if st.session_state.employments:
        employments_export_df = pd.DataFrame(st.session_state.employments)
        employments_export_df['start_date'] = employments_export_df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
        employments_export_df['end_date'] = employments_export_df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) else x)
        st.sidebar.download_button(
            label="Download Employments Excel",
            data=convert_df_to_excel(employments_export_df),
            file_name="employments_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_employments_excel",
            use_container_width=True
        )
    else:
        st.sidebar.info("No employment data to export (Excel).")

    if st.session_state.all_extractions:
        extractions_export_df = pd.DataFrame(st.session_state.all_extractions)
        st.sidebar.download_button(
            label="Download Extractions Log Excel",
            data=convert_df_to_excel(extractions_export_df),
            file_name="extractions_log.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_extractions_excel",
            use_container_width=True
        )
    else:
        st.sidebar.info("No extraction log data to export (Excel).")

st.sidebar.markdown("---")
st.sidebar.subheader("Complete Data Backup (JSON)")
if st.sidebar.button("💾 Create Full Backup (.json)"):
    try:
        full_backup_data = {
            "people": st.session_state.people,
            "firms": st.session_state.firms,
            "employments": [
                {k: (v.strftime('%Y-%m-%d') if isinstance(v, date) else v) for k, v in emp.items()}
                for emp in st.session_state.employments
            ],
            "extractions_log": st.session_state.all_extractions
        }
        export_json = json.dumps(full_backup_data, indent=2, ensure_ascii=False)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.sidebar.download_button(
            label="Download Full Backup",
            data=export_json,
            file_name=f"hedge_fund_full_backup_{timestamp}.json",
            mime="application/json",
            use_container_width=True
        )
        st.sidebar.success("✅ Full backup ready!")
    except Exception as e:
        st.sidebar.error(f"Complete JSON export failed: {str(e)}")

# --- FOOTER ---
st.markdown("---")
st.markdown("### 👥 Asian Hedge Fund Talent Intelligence Platform")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("**🔍 Global Search**")
with col2:
    st.markdown("**📊 Performance Tracking**") 
with col3:
    st.markdown("**🤝 Professional Networks**")
with col4:
    st.markdown("**📋 Smart Review System** (Simplified)")
