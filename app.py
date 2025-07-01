import streamlit as st
import pandas as pd
import google.generativeai as genai
import time
import re

# ==============================================================================
# 1. GEMINI API CONFIGURATION AND HELPER FUNCTIONS
# ==============================================================================

# Define the structure (schema) for the data you want to extract.
extraction_schema = {
    "name": "extract_hedge_fund_intelligence",
    "description": "Extracts key people, organizations, roles, locations, and performance metrics from a financial news text.",
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "persons": {"type": "ARRAY", "description": "List of names of individuals mentioned.", "items": {"type": "STRING"}},
            "organizations": {"type": "ARRAY", "description": "List of names of hedge funds or firms.", "items": {"type": "STRING"}},
            "roles": {"type": "ARRAY", "description": "List of job titles or specific roles mentioned.", "items": {"type": "STRING"}},
            "locations": {"type": "ARRAY", "description": "List of geographical locations.", "items": {"type": "STRING"}},
            "performance_metrics": {"type": "ARRAY", "description": "List of quantitative performance data, AuM, or returns.", "items": {"type": "STRING"}},
            "summary": {"type": "STRING", "description": "A concise, one-sentence summary of the key event."},
            "focus_region": {"type": "STRING", "description": "Primary geographical focus: 'Asia', 'Europe', 'North America', 'Global', or 'Unspecified'."}
        },
        "required": ["persons", "organizations", "roles", "locations", "performance_metrics", "summary", "focus_region"]
    }
}

# Use Streamlit's cache to avoid re-running the same API call for the same text and model.
@st.cache_data(show_spinner=False)
def extract_entities_with_gemini(text_chunk: str, model_name: str):
    """
    Uses the Gemini API to extract structured data from a text snippet.
    """
    try:
        if not genai.conf.api_key:
             st.error("Gemini API key not configured.")
             st.stop()
             
        model = genai.GenerativeModel(model_name=model_name, tools=[extraction_schema])
    except Exception as e:
        st.error(f"Error initializing Gemini model: {e}")
        return None

    prompt = f"""
    You are an expert financial analyst. Analyze the following text and extract key intelligence using the `extract_hedge_fund_intelligence` tool. If a category is empty, return an empty list.
    Text to analyze:
    ---
    {text_chunk}
    ---
    """
    try:
        response = model.generate_content(prompt, tool_config={'function_calling_config': 'ANY'})
        if response.candidates and response.candidates[0].content.parts:
            func_call = response.candidates[0].content.parts[0].function_call
            if func_call.name == "extract_hedge_fund_intelligence":
                return dict(func_call.args)
    except Exception as e:
        st.warning(f"API Error for one item: {e}. Skipping.", icon="⚠️")
        return None
    return None

def split_emails_into_items(full_text: str) -> list:
    delimiters = re.findall(r'\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{4}\t', full_text)
    if not delimiters: return [full_text]
    items = re.split(r'\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{4}\t', full_text)
    items.pop(0)
    processed_items = []
    for i, item_text in enumerate(items):
        cleaned_text = re.sub(r'<https?://\S+>', '', item_text)
        cleaned_text = re.sub(r'(\n\s*){3,}', '\n\n', cleaned_text)
        if len(cleaned_text.strip()) > 50 and "FolioMetrics" not in cleaned_text and "Unsubscribe" not in cleaned_text:
            processed_items.append(delimiters[i].strip() + "\n" + cleaned_text.strip())
    return processed_items

@st.cache_data
def convert_df_to_csv(df_to_convert):
    df_copy = df_to_convert.copy()
    for col in ['persons', 'organizations', 'roles', 'locations', 'performance_metrics']:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    return df_copy.to_csv(index=False).encode('utf-8')

# ==============================================================================
# 2. STREAMLIT APPLICATION UI
# ==============================================================================

st.set_page_config(layout="wide", page_title="Hedge Fund Intelligence Extractor")

st.title("Hedge Fund Talent Intelligence Extractor")
st.markdown("Upload a large `.txt` file. The app will process it in manageable batches to respect API rate limits and prevent timeouts.")

# --- Sidebar for Configuration ---
st.sidebar.header("⚙️ Configuration")

# API Key Input
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    st.sidebar.success("API key loaded from secrets!", icon="✅")
except (FileNotFoundError, KeyError):
    st.sidebar.warning("API key not found in secrets.", icon="⚠️")
    api_key_local = st.sidebar.text_input("Enter your Google Gemini API Key", type="password")
    if api_key_local:
        genai.configure(api_key=api_key_local)
        st.sidebar.success("API key loaded locally!", icon="✅")

# Model Selection
GEMINI_MODELS = {
    "Gemini 1.5 Flash (Fast & Cheap)": "gemini-1.5-flash",
    "Gemini 1.5 Pro (Smarter & Slower)": "gemini-1.5-pro-latest"
}
selected_model_name_key = st.sidebar.selectbox("Choose a Gemini Model:", options=GEMINI_MODELS.keys())
selected_model = GEMINI_MODELS[selected_model_name_key]

# Rate Limiting and Batching Controls
st.sidebar.subheader("Processing Controls")
batch_size = st.sidebar.slider("Batch Size (items per run):", min_value=1, max_value=50, value=10)
rpm_limit = st.sidebar.slider("Rate Limit (requests per minute):", min_value=5, max_value=60, value=50)
delay_between_calls = 60.0 / rpm_limit

# --- Initialize Session State ---
if 'processed_results' not in st.session_state:
    st.session_state.processed_results = []
if 'all_items' not in st.session_state:
    st.session_state.all_items = []
if 'processed_count' not in st.session_state:
    st.session_state.processed_count = 0
if 'current_file_id' not in st.session_state:
    st.session_state.current_file_id = None

# --- Main App Logic ---
uploaded_file = st.file_uploader("Choose a newsletter .txt file", type="txt")

if uploaded_file is not None:
    # If a new file is uploaded, reset the state
    if uploaded_file.id != st.session_state.current_file_id:
        st.session_state.current_file_id = uploaded_file.id
        with st.spinner("Analyzing file structure..."):
            raw_text = uploaded_file.read().decode("utf-8")
            st.session_state.all_items = split_emails_into_items(raw_text)
        st.session_state.processed_results = []
        st.session_state.processed_count = 0
        st.info(f"New file loaded. Found {len(st.session_state.all_items)} potential news items.")

    total_items = len(st.session_state.all_items)
    items_left = total_items - st.session_state.processed_count

    # --- Processing Controls UI ---
    col_prog, col_btn = st.columns([3, 1])
    with col_prog:
        st.progress(st.session_state.processed_count / total_items if total_items > 0 else 0)
        st.write(f"Processed {st.session_state.processed_count} of {total_items} items. ({items_left} remaining).")

    with col_btn:
        if st.button(f"Process Next Batch ({min(batch_size, items_left)} items)", disabled=(items_left == 0), type="primary"):
            if not genai.conf.api_key:
                st.error("Please provide your Gemini API key in the sidebar to proceed.")
            else:
                with st.spinner(f"Processing batch... This will take ~{min(batch_size, items_left) * delay_between_calls:.1f} seconds."):
                    start_index = st.session_state.processed_count
                    end_index = start_index + batch_size
                    batch_items = st.session_state.all_items[start_index:end_index]

                    for item in batch_items:
                        result = extract_entities_with_gemini(item, selected_model)
                        if result:
                            result['original_text'] = item
                            st.session_state.processed_results.append(result)
                        st.session_state.processed_count += 1
                        time.sleep(delay_between_calls) # Respect the rate limit
                st.rerun()

# --- Display Results ---
if st.session_state.processed_results:
    st.header("Extracted Intelligence")
    df = pd.DataFrame(st.session_state.processed_results)

    # --- Filters and Export ---
    st.subheader("Filter and Export")
    filter_col, export_col = st.columns([3, 1])
    with filter_col:
        available_regions = sorted(df['focus_region'].unique().tolist())
        selected_region = st.selectbox("Filter by Region:", options=['All'] + available_regions)

    if selected_region != 'All':
        filtered_df = df[df['focus_region'] == selected_region].reset_index(drop=True)
    else:
        filtered_df = df

    with export_col:
        st.write("") # Spacer
        csv_data = convert_df_to_csv(filtered_df)
        st.download_button(
           label="📥 Download as CSV",
           data=csv_data,
           file_name=f'intelligence_export_{selected_region.lower()}.csv',
           mime='text/csv',
           use_container_width=True
        )

    st.metric("Total Records Found (in filter)", len(filtered_df))
    st.markdown("---")

    # Display each record
    for index, row in filtered_df.iterrows():
        with st.expander(f"**{row['summary']}** (`Focus: {row['focus_region']}`)"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### People"); st.dataframe(pd.DataFrame(row['persons'], columns=['Name']), use_container_width=True, hide_index=True)
                st.markdown("##### Firms"); st.dataframe(pd.DataFrame(row['organizations'], columns=['Organization']), use_container_width=True, hide_index=True)
            with col2:
                st.markdown("##### Roles"); st.dataframe(pd.DataFrame(row['roles'], columns=['Role']), use_container_width=True, hide_index=True)
                st.markdown("##### Metrics"); st.dataframe(pd.DataFrame(row['performance_metrics'], columns=['Metric']), use_container_width=True, hide_index=True)
            st.markdown(f"**Locations:** `{', '.join(row['locations'])}`" if row['locations'] else "**Locations:** `N/A`")
else:
    st.info("Upload a file to begin processing.")
