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

# --- Enhanced File Loading with Encoding Detection ---
def load_file_content_enhanced(uploaded_file):
    """
    Loads content from an uploaded file, attempting different encodings.
    Logs file size for monitoring.
    """
    file_size = len(uploaded_file.getvalue())
    file_size_mb = file_size / (1024 * 1024)
    logger.info(f"Loading file: {uploaded_file.name} ({file_size_mb:.1f} MB)")

    # Define a list of common encodings to try
    encodings = ['utf-8', 'latin-1', 'cp1252']
    for encoding in encodings:
        try:
            raw_data = uploaded_file.getvalue()
            return raw_data.decode(encoding)
        except UnicodeDecodeError:
            logger.warning(f"Failed to decode with {encoding}, trying next encoding...")
    
    # If all encodings fail, raise an error or return an empty string
    st.error(f"Failed to decode file '{uploaded_file.name}' with common encodings. Please ensure it's a text-based file.")
    return None


# --- Data Storage Initialization ---
DATA_DIR = "hedge_fund_data"
PEOPLE_FILE = os.path.join(DATA_DIR, "people.json")
FIRMS_FILE = os.path.join(DATA_DIR, "firms.json")
EMPLOYMENTS_FILE = os.path.join(DATA_DIR, "employments.json")
EXTRACTIONS_FILE = os.path.join(DATA_DIR, "extractions.json") # New file for all extractions

os.makedirs(DATA_DIR, exist_ok=True)

def load_data():
    data = {
        'people': {},
        'firms': {},
        'employments': {},
        'all_extractions': [] # Initialize as empty list
    }
    try:
        if os.path.exists(PEOPLE_FILE):
            with open(PEOPLE_FILE, 'r') as f:
                data['people'] = json.load(f)
        if os.path.exists(FIRMS_FILE):
            with open(FIRMS_FILE, 'r') as f:
                data['firms'] = json.load(f)
        if os.path.exists(EMPLOYMENTS_FILE):
            with open(EMPLOYMENTS_FILE, 'r') as f:
                data['employments'] = json.load(f)
        if os.path.exists(EXTRACTIONS_FILE):
            with open(EXTRACTIONS_FILE, 'r') as f:
                data['all_extractions'] = json.load(f)
    except Exception as e:
        st.error(f"Error loading data: {e}. Starting with empty data.")
    return data

def save_data():
    try:
        with open(PEOPLE_FILE, 'w') as f:
            json.dump(st.session_state.people, f, indent=4)
        with open(FIRMS_FILE, 'w') as f:
            json.dump(st.session_state.firms, f, indent=4)
        with open(EMPLOYMENTS_FILE, 'w') as f:
            json.dump(st.session_state.employments, f, indent=4)
        with open(EXTRACTIONS_FILE, 'w') as f:
            json.dump(st.session_state.all_extractions, f, indent=4)
        logger.info("Data saved successfully.")
    except Exception as e:
        st.error(f"Error saving data: {e}")

# Initialize session state variables
if 'data_loaded' not in st.session_state:
    initial_data = load_data()
    st.session_state.people = initial_data['people']
    st.session_state.firms = initial_data['firms']
    st.session_state.employments = initial_data['employments']
    st.session_state.all_extractions = initial_data['all_extractions'] # Load all extractions
    st.session_state.data_loaded = True
    st.session_state.last_auto_save = datetime.now() # Initialize auto-save time

if 'pending_review_data' not in st.session_state:
    st.session_state.pending_review_data = [] # List to hold data requiring review
if 'show_review_interface' not in st.session_state:
    st.session_state.show_review_interface = False
if 'review_start_time' not in st.session_state:
    st.session_state.review_start_time = None
if 'review_timeout_minutes' not in st.session_state:
    st.session_state.review_timeout_minutes = 5 # Default review timeout

def get_review_time_remaining():
    if st.session_state.review_start_time:
        elapsed = (datetime.now() - st.session_state.review_start_time).total_seconds()
        remaining = st.session_state.review_timeout_minutes * 60 - elapsed
        return max(0, int(remaining))
    return 0

# --- CRUD Operations for People ---
def save_person(person_data):
    person_id = person_data.get('id', str(uuid.uuid4()))
    st.session_state.people[person_id] = {
        'id': person_id,
        'name': safe_get(person_data, 'name'),
        'current_firm_id': safe_get(person_data, 'current_firm_id'),
        'current_title': safe_get(person_data, 'current_title'),
        'linkedin': safe_get(person_data, 'linkedin'),
        'education': safe_get(person_data, 'education'),
        'expertise': safe_get(person_data, 'expertise'),
        'notes': safe_get(person_data, 'notes'),
        'source_text': safe_get(person_data, 'source_text'),
        'extraction_timestamp': datetime.now().isoformat()
    }
    logger.info(f"Person '{person_data.get('name')}' saved/updated.")
    return person_id

def delete_person(person_id):
    if person_id in st.session_state.people:
        del st.session_state.people[person_id]
        # Also delete related employments
        st.session_state.employments = {
            emp_id: emp_data for emp_id, emp_data in st.session_state.employments.items()
            if emp_data['person_id'] != person_id
        }
        st.success(f"Person {person_id} and related employments deleted.")
        save_data()
        st.rerun()

# --- CRUD Operations for Firms ---
def save_firm(firm_data):
    firm_id = firm_data.get('id', str(uuid.uuid4()))
    st.session_state.firms[firm_id] = {
        'id': firm_id,
        'name': safe_get(firm_data, 'name'),
        'type': safe_get(firm_data, 'type'), # Key is 'type'
        'strategy': safe_get(firm_data, 'strategy'),
        'location': safe_get(firm_data, 'location'),
        'aum': safe_get(firm_data, 'aum'),
        'founded_year': safe_get(firm_data, 'founded_year'), # Key is 'founded_year'
        'website': safe_get(firm_data, 'website'),
        'notes': safe_get(firm_data, 'notes'),
        'source_text': safe_get(firm_data, 'source_text'),
        'extraction_timestamp': datetime.now().isoformat()
    }
    logger.info(f"Firm '{firm_data.get('name')}' saved/updated.")
    return firm_id

def delete_firm(firm_id):
    if firm_id in st.session_state.firms:
        del st.session_state.firms[firm_id]
        # Update people who were associated with this firm
        for person_id, person_data in st.session_state.people.items():
            if person_data.get('current_firm_id') == firm_id:
                person_data['current_firm_id'] = None # Disassociate
        # Delete related employments
        st.session_state.employments = {
            emp_id: emp_data for emp_id, emp_data in st.session_state.employments.items()
            if emp_data['firm_id'] != firm_id
        }
        st.success(f"Firm {firm_id} and related associations deleted.")
        save_data()
        st.rerun()

# --- CRUD Operations for Employments ---
def save_employment(employment_data):
    employment_id = employment_data.get('id', str(uuid.uuid4()))
    st.session_state.employments[employment_id] = {
        'id': employment_id,
        'person_id': safe_get(employment_data, 'person_id'),
        'firm_id': safe_get(employment_data, 'firm_id'),
        'start_date': safe_get(employment_data, 'start_date'),
        'end_date': safe_get(employment_data, 'end_date'),
        'title': safe_get(employment_data, 'title'),
        'current': safe_get(employment_data, 'current', False),
        'source_text': safe_get(employment_data, 'source_text'),
        'extraction_timestamp': datetime.now().isoformat()
    }
    logger.info(f"Employment {employment_id} saved/updated.")
    return employment_id

def delete_employment(employment_id):
    if employment_id in st.session_state.employments:
        del st.session_state.employments[employment_id]
        st.success(f"Employment {employment_id} deleted.")
        save_data()
        st.rerun()

# --- Gemini API Integration for Extraction ---
if GENAI_AVAILABLE:
    # Use Streamlit secrets for API key
    gemini_api_key = st.secrets.get("GEMINI_API_KEY")

    if gemini_api_key:
        genai.configure(api_key=gemini_api_key)
    else:
        st.error("Gemini API Key not found in Streamlit secrets. Please add it to .streamlit/secrets.toml")
        gemini_api_key_manual = st.text_input("Enter your Gemini API Key (fallback):", type="password")
        if gemini_api_key_manual:
            genai.configure(api_key=gemini_api_key_manual)
            st.success("Gemini API Key configured from manual input.")


    def create_cached_context():
        """Creates a cached model with specific instructions for entity extraction."""
        @st.cache_resource(ttl=3600) # Cache for 1 hour
        def _create_model():
            system_instruction = """
            You are an expert in extracting information about hedge funds, investment firms, and associated talent.
            Your task is to identify and extract structured data about:
            1. People (individuals)
            2. Firms (companies, especially investment firms, hedge funds, asset managers)
            3. Employment relationships between people and firms
            4. Performance data associated with people or firms
            5. Movements (people changing roles/firms)

            Strictly output the information in JSON format. Each top-level key should be a list of dictionaries for the corresponding entity type.
            Ensure all IDs are unique UUIDs. If a field is not found, omit it or set it to null, do not invent data.

            Example Output Format:
            ```json
            {
              "people": [
                {
                  "id": "uuid-1",
                  "name": "John Doe",
                  "current_firm_id": "firm-uuid-1",
                  "current_title": "Portfolio Manager",
                  "linkedin": "[https://linkedin.com/in/johndoe](https://linkedin.com/in/johndoe)",
                  "education": "University of XYZ",
                  "expertise": "Equity long/short, TMT sector",
                  "notes": "Experienced PM with focus on tech."
                }
              ],
              "firms": [
                {
                  "id": "firm-uuid-1",
                  "name": "Alpha Capital",
                  "type": "Hedge Fund",
                  "strategy": "Long/Short Equity",
                  "location": "New York, NY",
                  "aum": "5 Billion USD",
                  "founded_year": "2005",
                  "website": "[https://alphacap.com](https://alphacap.com)",
                  "notes": "Focused on TMT and healthcare."
                }
              ],
              "employments": [
                {
                  "id": "emp-uuid-1",
                  "person_id": "uuid-1",
                  "firm_id": "firm-uuid-1",
                  "start_date": "2020-01-01",
                  "end_date": null,
                  "title": "Portfolio Manager",
                  "current": true
                }
              ],
              "performance_data": [
                {
                  "id": "perf-uuid-1",
                  "entity_id": "firm-uuid-1",
                  "entity_type": "firm",
                  "period": "2023-FY",
                  "return": "15%",
                  "notes": "Strong performance in a down market."
                }
              ],
              "movements": [
                {
                  "id": "move-uuid-1",
                  "person_id": "uuid-1",
                  "from_firm_id": "old-firm-uuid",
                  "to_firm_id": "firm-uuid-1",
                  "date_of_move": "2020-01-01",
                  "notes": "Joined Alpha Capital from Beta Investments."
                }
              ]
            }
            ```
            If the input text provides a unique identifier for a person or firm (e.g., an existing internal ID or a widely recognized external ID), prefer to use that as the 'id' field. Otherwise, generate a UUID.
            When extracting dates, use 'YYYY-MM-DD' format if possible, otherwise 'YYYY-MM' or 'YYYY'.
            Ensure all extracted fields for a given entity are present if available in the text.
            If a person's current firm or title is mentioned, update `current_firm_id` and `current_title` in the person object, and create/update an `employment` entry.
            Ensure unique UUIDs are generated for each new entity (person, firm, employment, performance, movement). If an entity can be matched to an existing one by name or other unique identifier, update the existing entity's details instead of creating a duplicate. However, for this extraction task, assume new entities unless a clear ID is provided.
            """
            return genai.GenerativeModel('gemini-pro', system_instruction=system_instruction) # or 'gemini-1.5-flash' for faster results
        
        return _create_model()
    
    model = create_cached_context()

    # Function to extract structured data from text
    @st.spinner("Extracting insights with Gemini...")
    def extract_info_gemini(text):
        if not GENAI_AVAILABLE or not gemini_api_key:
            st.error("Gemini API is not configured. Please enter your API key.")
            return None

        try:
            response = model.generate_content(text)
            response_text = response.text.strip()
            
            # Attempt to parse JSON
            extracted_data = json.loads(response_text)
            st.success("Extraction complete!")
            return extracted_data
        except ValueError as ve:
            st.error(f"Failed to parse JSON from Gemini response: {ve}. Response was: {response_text[:500]}...")
            return None
        except Exception as e:
            st.error(f"Error during Gemini API call: {e}")
            if hasattr(e, '_error_response') and e._error_response:
                st.error(f"API Error Details: {e._error_response}")
            return None

    def process_extracted_data(extracted_data, source_text):
        new_extractions = []

        # Process People
        if 'people' in extracted_data:
            for person in extracted_data['people']:
                person['source_text'] = source_text
                new_person_id = save_person(person)
                person['id'] = new_person_id # Ensure ID is set for employment linking
                new_extractions.append({'type': 'person', 'data': person})

        # Process Firms
        if 'firms' in extracted_data:
            for firm in extracted_data['firms']:
                firm['source_text'] = source_text
                new_firm_id = save_firm(firm)
                firm['id'] = new_firm_id # Ensure ID is set for employment linking
                new_extractions.append({'type': 'firm', 'data': firm})
        
        # Process Employments (must come after people and firms for ID linking)
        if 'employments' in extracted_data:
            for emp in extracted_data['employments']:
                # Link by ID if available, otherwise by name (less reliable)
                person_id = emp.get('person_id')
                firm_id = emp.get('firm_id')

                # If IDs are not directly provided by Gemini, try to find them from saved data
                if not person_id and emp.get('person_name'):
                    for p_id, p_data in st.session_state.people.items():
                        if p_data['name'].lower() == emp['person_name'].lower():
                            person_id = p_id
                            break
                if not firm_id and emp.get('firm_name'):
                    for f_id, f_data in st.session_state.firms.items():
                        if f_data['name'].lower() == emp['firm_name'].lower():
                            firm_id = f_id
                            break
                
                if person_id and firm_id:
                    emp['person_id'] = person_id
                    emp['firm_id'] = firm_id
                    emp['source_text'] = source_text
                    save_employment(emp)
                    new_extractions.append({'type': 'employment', 'data': emp})
                else:
                    logger.warning(f"Skipping employment due to missing linked person/firm: {emp}")

        # Process Performance Data (save as generic extractions for now)
        if 'performance_data' in extracted_data:
            for perf in extracted_data['performance_data']:
                perf['source_text'] = source_text
                new_extractions.append({'type': 'performance', 'data': perf})

        # Process Movements (save as generic extractions for now)
        if 'movements' in extracted_data:
            for move in extracted_data['movements']:
                move['source_text'] = source_text
                new_extractions.append({'type': 'movement', 'data': move})
        
        st.session_state.all_extractions.extend(new_extractions)
        save_data() # Save after processing each extraction

else:
    st.warning("`google-generativeai` library not found. Please install it (`pip install google-generativeai`) to enable AI extraction features.")

# --- Review System ---
def start_review_timeout():
    if not st.session_state.review_start_time:
        st.session_state.review_start_time = datetime.now()

def auto_save_pending_reviews():
    saved_count = 0
    approved_queue = queue.Queue() # Thread-safe queue for approved items
    
    for review_item in st.session_state.pending_review_data:
        # For simplicity, auto-approving all items on timeout for now
        # In a real app, you'd have logic to decide approval based on criteria
        if review_item['type'] == 'person':
            save_person(review_item['data'])
            approved_queue.put(('person', review_item['data']))
            saved_count += 1
        elif review_item['type'] == 'firm':
            save_firm(review_item['data'])
            approved_queue.put(('firm', review_item['data']))
            saved_count += 1
        elif review_item['type'] == 'employment':
            save_employment(review_item['data'])
            approved_queue.put(('employment', review_item['data']))
            saved_count += 1
    
    # Clear pending reviews after auto-saving
    st.session_state.pending_review_data = []
    st.session_state.show_review_interface = False
    st.session_state.review_start_time = None
    
    # Save all data to disk after auto-saving
    save_data()
    return saved_count

def review_interface():
    st.title("Review Pending Extractions 📋")
    st.warning(f"Review session active. Auto-save in {st.session_state.review_timeout_minutes} minutes if no interaction.")

    remaining_time = get_review_time_remaining()
    if remaining_time > 0:
        st.info(f"Time remaining for review: {remaining_time // 60}m {remaining_time % 60}s")
    else:
        st.warning("Review time expired! Auto-saving pending items.")
        saved_count = auto_save_pending_reviews()
        if saved_count > 0:
            st.success(f"⏰ Auto-saved {saved_count} items from review queue!")
        st.rerun() # Rerun to refresh the UI after auto-save

    if not st.session_state.pending_review_data:
        st.success("No items currently pending review. All clear!")
        st.session_state.show_review_interface = False
        st.session_state.review_start_time = None
        if st.button("Back to Data Entry"):
            st.session_state.show_review_interface = False
            st.rerun()
        return

    col_review_action, col_review_time = st.columns([3, 1])
    with col_review_action:
        st.markdown(f"**Items to review: {len(st.session_state.pending_review_data)}**")
    with col_review_time:
        st.empty() # Placeholder for potential countdown, handled by rerun

    for i, item in enumerate(st.session_state.pending_review_data):
        st.subheader(f"Review Item {i+1}: {item['type'].capitalize()}")
        st.json(item['data']) # Display raw extracted data

        col_approve, col_edit, col_reject = st.columns(3)
        with col_approve:
            if st.button(f"✅ Approve {item['type'].capitalize()}", key=f"approve_{item['data']['id']}"):
                if item['type'] == 'person':
                    save_person(item['data'])
                elif item['type'] == 'firm':
                    save_firm(item['data'])
                elif item['type'] == 'employment':
                    save_employment(item['data'])
                
                st.session_state.pending_review_data.pop(i)
                st.success(f"Approved {item['type'].capitalize()}!")
                save_data()
                st.rerun() # Rerun to update the list
        with col_edit:
            # Implement edit functionality here (e.g., open a modal or form)
            st.button(f"✏️ Edit {item['type'].capitalize()}", key=f"edit_{item['data']['id']}", disabled=True) # Placeholder
        with col_reject:
            if st.button(f"❌ Reject {item['type'].capitalize()}", key=f"reject_{item['data']['id']}"):
                st.session_state.pending_review_data.pop(i)
                st.warning(f"Rejected {item['type'].capitalize()}.")
                st.rerun() # Rerun to update the list

    st.markdown("---")
    if st.button("Approve All Remaining"):
        saved_count = auto_save_pending_reviews() # This function now auto-approves and saves
        st.success(f"✅ Approved and saved {saved_count} items from review queue!")
        st.rerun()
    if st.button("Reject All Remaining"):
        st.session_state.pending_review_data = []
        st.session_state.show_review_interface = False
        st.session_state.review_start_time = None
        st.warning("All pending reviews rejected.")
        st.rerun()

# --- Main Application Layout ---
def main_app():
    st.sidebar.title("Data Operations")
    
    # Sidebar for data upload and extraction
    with st.sidebar:
        st.subheader("Upload Document for Extraction")
        uploaded_file = st.file_uploader("Upload a text document (.txt, .md, .pdf, .docx)", type=["txt", "md", "pdf", "docx"])
        
        if uploaded_file is not None:
            if uploaded_file.type == "text/plain" or uploaded_file.type == "text/markdown":
                document_content = load_file_content_enhanced(uploaded_file)
            elif uploaded_file.type == "application/pdf":
                # For PDF, you'd typically need a PDF parsing library like PyPDF2 or pdfminer.six
                st.warning("PDF parsing not implemented in this demo. Please upload text files.")
                document_content = None
            elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                # For DOCX, you'd need a library like python-docx
                st.warning("DOCX parsing not implemented in this demo. Please upload text files.")
                document_content = None
            else:
                st.warning("Unsupported file type. Please upload a .txt or .md file.")
                document_content = None

            if document_content and GENAI_AVAILABLE and gemini_api_key:
                if st.button("Extract Entities"):
                    extracted_json = extract_info_gemini(document_content)
                    if extracted_json:
                        # Decide which items need review based on some criteria (e.g., confidence score, specific types)
                        # For this example, let's say all extracted items are put into pending review
                        
                        temp_pending = []
                        if 'people' in extracted_json:
                            for p in extracted_json['people']:
                                temp_pending.append({'type': 'person', 'data': p})
                        if 'firms' in extracted_json:
                            for f in extracted_json['firms']:
                                temp_pending.append({'type': 'firm', 'data': f})
                        if 'employments' in extracted_json:
                            for e in extracted_json['employments']:
                                temp_pending.append({'type': 'employment', 'data': e})
                        
                        if temp_pending:
                            st.session_state.pending_review_data.extend(temp_pending)
                            st.session_state.show_review_interface = True
                            start_review_timeout()
                            st.rerun() # Rerun to switch to review interface
                        else:
                            st.info("No entities extracted from the document or all were directly saved.")
                            # If no items go to review, process and save directly
                            process_extracted_data(extracted_json, document_content)
                    
            elif not (GENAI_AVAILABLE and gemini_api_key):
                st.info("AI extraction requires Gemini API configuration.")


    # Main Tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Dashboard", "👥 People", "🏢 Firms",
        "🔗 Employments", "🔍 All Extractions", "⚙️ Admin & Export"
    ])

    with tab1:
        st.header("Asian Hedge Fund Talent Dashboard")
        
        # --- Key Metrics ---
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total People", len(st.session_state.people))
        with col2:
            st.metric("Total Firms", len(st.session_state.firms))
        with col3:
            st.metric("Total Employments", len(st.session_state.employments))

        st.markdown("---")

        # --- Firms by Type (Pie Chart) ---
        if st.session_state.firms:
            firms_df = pd.DataFrame(list(st.session_state.firms.values()))
            # Rename columns to match display expectations
            firms_df = firms_df.rename(columns={
                'type': 'firm_type',
                'founded_year': 'founded'
            })

            firm_type_counts = firms_df['firm_type'].value_counts().reset_index()
            firm_type_counts.columns = ['Firm Type', 'Count']
            
            st.subheader("Firms by Type")
            fig = px.pie(
                firm_type_counts, 
                values='Count', 
                names='Firm Type', 
                title='Distribution of Firms by Type',
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No firm data to display in dashboard yet.")

        st.markdown("---")

        # --- People by Current Firm (Bar Chart - Top 10) ---
        if st.session_state.people and st.session_state.firms:
            people_df = pd.DataFrame(list(st.session_state.people.values()))
            
            # Map firm_id to firm_name
            people_df['current_firm_name'] = people_df['current_firm_id'].map(
                {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
            ).fillna('Unknown Firm')

            firm_people_counts = people_df['current_firm_name'].value_counts().nlargest(10).reset_index()
            firm_people_counts.columns = ['Firm Name', 'Number of People']

            st.subheader("Top 10 Firms by Number of Associated People")
            fig = px.bar(
                firm_people_counts,
                x='Firm Name',
                y='Number of People',
                title='People Distribution Across Firms',
                color='Number of People',
                color_continuous_scale=px.colors.sequential.Plasma
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No people or firm data to display in dashboard yet.")


    with tab2:
        st.header("👥 People Database")

        people_data = list(st.session_state.people.values())
        if people_data:
            people_df = pd.DataFrame(people_data)
            
            # Add firm name for display
            people_df['current_firm_name'] = people_df['current_firm_id'].map(
                {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
            ).fillna('N/A')

            # Display with pagination and search
            search_query = st.text_input("Search People (Name, Title, Education, Expertise)", key="person_search")
            if search_query:
                people_df = people_df[
                    people_df.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)
                ]

            st.dataframe(
                people_df[[
                    'name', 'current_firm_name', 'current_title', 'education', 'expertise', 'linkedin', 'notes'
                ]],
                use_container_width=True,
                height=400,
                hide_index=True
            )

            # Detail view/edit for selected person
            st.subheader("Person Details / Edit")
            person_names = ["-- Select a Person --"] + sorted([p['name'] for p in people_data])
            selected_person_name = st.selectbox("Select a person to view/edit:", person_names, key="edit_person_select")

            if selected_person_name != "-- Select a Person --":
                selected_person = next(p for p in people_data if p['name'] == selected_person_name)
                
                with st.form(key=f"edit_person_form_{selected_person['id']}"):
                    edited_name = st.text_input("Name", value=selected_person['name'])
                    
                    # Dropdown for current firm
                    firm_options = {f['name']: f['id'] for f in st.session_state.firms.values()}
                    current_firm_name_selected = selected_person.get('current_firm_name', 'N/A')
                    current_firm_index = list(firm_options.keys()).index(current_firm_name_selected) if current_firm_name_selected in firm_options else 0
                    
                    selected_firm_name_for_person = st.selectbox(
                        "Current Firm", 
                        options=["-- Select Firm --"] + list(firm_options.keys()),
                        index=current_firm_index + 1 if current_firm_index >= 0 else 0, # Adjust index for '-- Select Firm --'
                        key=f"firm_select_{selected_person['id']}"
                    )
                    edited_current_firm_id = firm_options.get(selected_firm_name_for_person) if selected_firm_name_for_person != "-- Select Firm --" else None
                    
                    edited_current_title = st.text_input("Current Title", value=selected_person['current_title'])
                    edited_linkedin = st.text_input("LinkedIn Profile", value=selected_person['linkedin'])
                    edited_education = st.text_area("Education", value=selected_person['education'])
                    edited_expertise = st.text_area("Expertise", value=selected_person['expertise'])
                    edited_notes = st.text_area("Notes", value=selected_person['notes'])
                    
                    col_update_person, col_delete_person = st.columns(2)
                    with col_update_person:
                        if st.form_submit_button("Update Person"):
                            updated_person_data = selected_person.copy()
                            updated_person_data.update({
                                'name': edited_name,
                                'current_firm_id': edited_current_firm_id,
                                'current_title': edited_current_title,
                                'linkedin': edited_linkedin,
                                'education': edited_education,
                                'expertise': edited_expertise,
                                'notes': edited_notes
                            })
                            save_person(updated_person_data)
                            save_data()
                            st.success("Person updated successfully!")
                            st.rerun()
                    with col_delete_person:
                        if st.form_submit_button("Delete Person"):
                            delete_person(selected_person['id'])

        else:
            st.info("No people data available yet. Upload a document for extraction or add manually.")

    with tab3:
        st.header("🏢 Firms Database")

        firms_data = list(st.session_state.firms.values())
        if firms_data:
            firms_df = pd.DataFrame(firms_data)
            # --- FIX FOR KEYERROR ---
            # Rename columns to match the display expectations
            firms_df = firms_df.rename(columns={
                'type': 'firm_type',
                'founded_year': 'founded'
            })
            # --- END FIX ---

            # Display with pagination and search
            search_query = st.text_input("Search Firms (Name, Type, Strategy, Location)", key="firm_search")
            if search_query:
                firms_df = firms_df[
                    firms_df.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)
                ]
            
            # Pagination for Firms
            page_size = 10
            num_pages = (len(firms_df) - 1) // page_size + 1
            current_page = st.number_input("Page", min_value=1, max_value=num_pages, value=1, key="firm_page_num")
            
            start_index = (current_page - 1) * page_size
            end_index = start_index + page_size
            paginated_firms_df = firms_df.iloc[start_index:end_index]

            st.dataframe(
                paginated_firms_df[[
                    'name', 'firm_type', 'strategy', 'location', 'aum', 'founded', 'website'
                ]],
                use_container_width=True,
                height=400,
                hide_index=True
            )
            
            # Detail view/edit for selected firm
            st.subheader("Firm Details / Edit")
            firm_names = ["-- Select a Firm --"] + sorted([f['name'] for f in firms_data])
            selected_firm_name = st.selectbox("Select a firm to view/edit:", firm_names, key="edit_firm_select")

            if selected_firm_name != "-- Select a Firm --":
                selected_firm = next(f for f in firms_data if f['name'] == selected_firm_name)

                with st.form(key=f"edit_firm_form_{selected_firm['id']}"):
                    edited_name = st.text_input("Name", value=selected_firm['name'])
                    edited_type = st.text_input("Type", value=selected_firm['type'])
                    edited_strategy = st.text_input("Strategy", value=selected_firm['strategy'])
                    edited_location = st.text_input("Location", value=selected_firm['location'])
                    edited_aum = st.text_input("AUM", value=selected_firm['aum'])
                    edited_founded_year = st.text_input("Founded Year", value=selected_firm['founded_year'])
                    edited_website = st.text_input("Website", value=selected_firm['website'])
                    edited_notes = st.text_area("Notes", value=selected_firm['notes'])
                    
                    col_update_firm, col_delete_firm = st.columns(2)
                    with col_update_firm:
                        if st.form_submit_button("Update Firm"):
                            updated_firm_data = selected_firm.copy()
                            updated_firm_data.update({
                                'name': edited_name,
                                'type': edited_type,
                                'strategy': edited_strategy,
                                'location': edited_location,
                                'aum': edited_aum,
                                'founded_year': edited_founded_year,
                                'website': edited_website,
                                'notes': edited_notes
                            })
                            save_firm(updated_firm_data)
                            save_data()
                            st.success("Firm updated successfully!")
                            st.rerun()
                    with col_delete_firm:
                        if st.form_submit_button("Delete Firm"):
                            delete_firm(selected_firm['id'])
        else:
            st.info("No firm data available yet. Upload a document for extraction or add manually.")

    with tab4:
        st.header("🔗 Employment Relationships")
        employments_data = list(st.session_state.employments.values())
        if employments_data:
            employments_df = pd.DataFrame(employments_data)
            
            # Map IDs to names for display
            employments_df['person_name'] = employments_df['person_id'].map(
                {p_id: st.session_state.people[p_id]['name'] for p_id in st.session_state.people}
            ).fillna('N/A')
            employments_df['firm_name'] = employments_df['firm_id'].map(
                {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
            ).fillna('N/A')

            st.dataframe(
                employments_df[['person_name', 'firm_name', 'title', 'start_date', 'end_date', 'current']],
                use_container_width=True,
                height=400,
                hide_index=True
            )

            # Detail view/edit for selected employment
            st.subheader("Employment Details / Edit")
            employment_display_names = ["-- Select an Employment --"] + [
                f"{emp['person_name']} at {emp['firm_name']} ({emp['title']})" for _, emp in employments_df.iterrows()
            ]
            selected_employment_display = st.selectbox("Select an employment to view/edit:", employment_display_names, key="edit_employment_select")

            if selected_employment_display != "-- Select an Employment --":
                selected_employment_row = employments_df[
                    employments_df.apply(lambda row: f"{row['person_name']} at {row['firm_name']} ({row['title']})" == selected_employment_display, axis=1)
                ].iloc[0]
                selected_employment_id = selected_employment_row['id']
                selected_employment = st.session_state.employments[selected_employment_id]

                with st.form(key=f"edit_employment_form_{selected_employment_id}"):
                    # Dropdowns for person and firm (using current names)
                    person_options = {p['name']: p['id'] for p in st.session_state.people.values()}
                    firm_options = {f['name']: f['id'] for f in st.session_state.firms.values()}

                    current_person_name = st.session_state.people.get(selected_employment['person_id'], {}).get('name', 'N/A')
                    current_firm_name = st.session_state.firms.get(selected_employment['firm_id'], {}).get('name', 'N/A')

                    selected_person_name_emp = st.selectbox(
                        "Person", 
                        options=["-- Select Person --"] + list(person_options.keys()),
                        index=list(person_options.keys()).index(current_person_name) + 1 if current_person_name in person_options else 0,
                        key=f"emp_person_select_{selected_employment_id}"
                    )
                    edited_person_id = person_options.get(selected_person_name_emp) if selected_person_name_emp != "-- Select Person --" else None

                    selected_firm_name_emp = st.selectbox(
                        "Firm", 
                        options=["-- Select Firm --"] + list(firm_options.keys()),
                        index=list(firm_options.keys()).index(current_firm_name) + 1 if current_firm_name in firm_options else 0,
                        key=f"emp_firm_select_{selected_employment_id}"
                    )
                    edited_firm_id = firm_options.get(selected_firm_name_emp) if selected_firm_name_emp != "-- Select Firm --" else None

                    edited_title = st.text_input("Title", value=selected_employment['title'])
                    edited_start_date = st.text_input("Start Date (YYYY-MM-DD)", value=selected_employment['start_date'])
                    edited_end_date = st.text_input("End Date (YYYY-MM-DD, or leave blank if current)", value=selected_employment['end_date'] if selected_employment['end_date'] else "")
                    edited_current = st.checkbox("Current Employment", value=selected_employment['current'])
                    
                    col_update_emp, col_delete_emp = st.columns(2)
                    with col_update_emp:
                        if st.form_submit_button("Update Employment"):
                            updated_employment_data = selected_employment.copy()
                            updated_employment_data.update({
                                'person_id': edited_person_id,
                                'firm_id': edited_firm_id,
                                'title': edited_title,
                                'start_date': edited_start_date,
                                'end_date': edited_end_date if edited_end_date else None,
                                'current': edited_current
                            })
                            save_employment(updated_employment_data)
                            save_data()
                            st.success("Employment updated successfully!")
                            st.rerun()
                    with col_delete_emp:
                        if st.form_submit_button("Delete Employment"):
                            delete_employment(selected_employment['id'])
        else:
            st.info("No employment data available yet.")

    with tab5:
        st.header("🔍 All Raw Extractions")
        if st.session_state.all_extractions:
            all_extractions_df = pd.DataFrame(st.session_state.all_extractions)
            # Flatten 'data' column for display, if it's structured
            # For simplicity, we'll just display the type and the raw data
            st.dataframe(all_extractions_df[['type', 'data']], use_container_width=True, height=600)
        else:
            st.info("No raw extraction data available yet.")

    with tab6:
        st.header("⚙️ Admin & Export")
        st.subheader("Data Management")
        if st.button("Save All Data Now"):
            save_data()
            st.success("All current data manually saved!")
        
        st.subheader("Export Data")
        export_format = st.radio("Select Export Format:", ("CSV", "JSON", "Excel (if openpyxl installed)"))

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if export_format == "CSV":
            if st.session_state.people:
                people_df = pd.DataFrame(list(st.session_state.people.values()))
                people_csv = people_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download People CSV",
                    data=people_csv,
                    file_name=f"people_data_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            if st.session_state.firms:
                firms_df = pd.DataFrame(list(st.session_state.firms.values()))
                # Rename for export consistency if needed, or export as is
                firms_df = firms_df.rename(columns={'type': 'firm_type', 'founded_year': 'founded'})
                firms_csv = firms_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Firms CSV",
                    data=firms_csv,
                    file_name=f"firms_data_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            if st.session_state.employments:
                employments_df = pd.DataFrame(list(st.session_state.employments.values()))
                # Add human-readable names for export
                employments_df['person_name'] = employments_df['person_id'].map(
                    {p_id: st.session_state.people[p_id]['name'] for p_id in st.session_state.people}
                ).fillna('N/A')
                employments_df['firm_name'] = employments_df['firm_id'].map(
                    {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
                ).fillna('N/A')
                employments_csv = employments_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Employments CSV",
                    data=employments_csv,
                    file_name=f"employments_data_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            if not (st.session_state.people or st.session_state.firms or st.session_state.employments):
                st.info("No data to export to CSV.")

        elif export_format == "JSON":
            export_data = {
                'people': list(st.session_state.people.values()),
                'firms': list(st.session_state.firms.values()),
                'employments': list(st.session_state.employments.values()),
                'all_extractions': st.session_state.all_extractions
            }
            export_json = json.dumps(export_data, indent=4).encode('utf-8')
            st.download_button(
                label="Download All Data as JSON",
                data=export_json,
                file_name=f"hedge_fund_full_backup_{timestamp}.json",
                mime="application/json",
                use_container_width=True
            )
            st.success("✅ Full backup ready!")

        elif export_format == "Excel (if openpyxl installed)":
            if EXCEL_AVAILABLE:
                if st.session_state.people or st.session_state.firms or st.session_state.employments:
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        if st.session_state.people:
                            people_df = pd.DataFrame(list(st.session_state.people.values()))
                            people_df['current_firm_name'] = people_df['current_firm_id'].map(
                                {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
                            ).fillna('N/A')
                            people_df.to_excel(writer, sheet_name='People', index=False)
                        if st.session_state.firms:
                            firms_df = pd.DataFrame(list(st.session_state.firms.values()))
                            firms_df = firms_df.rename(columns={'type': 'firm_type', 'founded_year': 'founded'})
                            firms_df.to_excel(writer, sheet_name='Firms', index=False)
                        if st.session_state.employments:
                            employments_df = pd.DataFrame(list(st.session_state.employments.values()))
                            employments_df['person_name'] = employments_df['person_id'].map(
                                {p_id: st.session_state.people[p_id]['name'] for p_id in st.session_state.people}
                            ).fillna('N/A')
                            employments_df['firm_name'] = employments_df['firm_id'].map(
                                {f_id: st.session_state.firms[f_id]['name'] for f_id in st.session_state.firms}
                            ).fillna('N/A')
                            employments_df.to_excel(writer, sheet_name='Employments', index=False)
                        if st.session_state.all_extractions:
                            all_extractions_df = pd.DataFrame(st.session_state.all_extractions)
                            # You might need to flatten 'data' column more carefully for Excel
                            all_extractions_df.to_excel(writer, sheet_name='All_Extractions', index=False)
                    
                    st.download_button(
                        label="Download All Data as Excel (XLSX)",
                        data=output.getvalue(),
                        file_name=f"hedge_fund_data_{timestamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    st.success("✅ Excel file ready!")
                else:
                    st.info("No data available for Excel export.")
            else:
                st.warning("Please install `openpyxl` (`pip install openpyxl`) to enable Excel export.")

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
    st.markdown("**📋 Smart Review System**")

# Auto-save functionality
current_time = datetime.now()
if 'last_auto_save' not in st.session_state:
    st.session_state.last_auto_save = current_time

time_since_save = (current_time - st.session_state.last_auto_save).total_seconds()
if time_since_save > 30 and (st.session_state.people or st.session_state.firms or st.session_state.all_extractions):
    save_data()
    st.session_state.last_auto_save = current_time

# Handle review timeout (if review interface is shown)
if st.session_state.show_review_interface and st.session_state.pending_review_data:
    if get_review_time_remaining() <= 0:
        saved_count = auto_save_pending_reviews()
        if saved_count > 0:
            st.sidebar.success(f"⏰ Auto-saved {saved_count} items from review queue!")
            st.rerun()

# Determine which interface to show
if st.session_state.show_review_interface:
    review_interface()
else:
    main_app()
