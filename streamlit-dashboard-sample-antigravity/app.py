import streamlit as st
import polars as pl
import pandas as pd
from data_manager import DataManager
import math
import os

st.set_page_config(layout="wide", page_title="Polars Dashboard")

# --- Session State Initialization ---
if 'page' not in st.session_state:
    st.session_state.page = 1
if 'page_size' not in st.session_state:
    st.session_state.page_size = 100
if 'sort_col' not in st.session_state:
    st.session_state.sort_col = "id"
if 'sort_desc' not in st.session_state:
    st.session_state.sort_desc = False
if 'pending_edits' not in st.session_state:
    st.session_state.pending_edits = {} # {id: {col: val}}
if 'current_page_ids' not in st.session_state:
    st.session_state.current_page_ids = []
if 'source_config' not in st.session_state:
    st.session_state.source_config = {"type": "local", "path": "data/large_dataset.parquet"}
if 'manager' not in st.session_state:
    st.session_state.manager = None

# --- Sidebar Controls ---
with st.sidebar:
    st.title("Data Source")
    
    source_type = st.selectbox("Source Type", ["Local", "Cloud", "Database"], index=0)
    
    config = {}
    if source_type == "Local":
        default_path = "data/large_dataset.parquet"
        path = st.text_input("File Path (Parquet or CSV)", value=default_path)
        if st.button("Load Data"):
            st.session_state.source_config = {"type": "local", "path": path}
            st.session_state.manager = DataManager("local", {"path": path})
            st.session_state.page = 1
            st.session_state.pending_edits = {}
            st.rerun()
            
    elif source_type == "Cloud":
        path = st.text_input("Cloud URI (s3://, gs://, etc.) - Parquet or CSV")
        if st.button("Load Data"):
            st.session_state.source_config = {"type": "cloud", "path": path}
            st.session_state.manager = DataManager("cloud", {"path": path})
            st.session_state.page = 1
            st.session_state.pending_edits = {}
            st.rerun()
            
    elif source_type == "Database":
        conn_str = st.text_input("Connection String (e.g. postgresql://...)")
        query = st.text_area("SQL Query", value="SELECT * FROM my_table")
        if st.button("Load Data"):
            st.session_state.source_config = {"type": "database", "connection_string": conn_str, "query": query}
            st.session_state.manager = DataManager("database", {"connection_string": conn_str, "query": query})
            st.session_state.page = 1
            st.session_state.pending_edits = {}
            st.rerun()

    st.divider()

    # Initialize manager if not present (first run)
    if st.session_state.manager is None:
        # Try to load default
        if os.path.exists("data/large_dataset.parquet"):
             st.session_state.manager = DataManager("local", {"path": "data/large_dataset.parquet"})
        else:
            st.warning("Please configure a data source.")
            st.stop()

    manager = st.session_state.manager

    st.title("Controls")
    try:
        total_rows = manager.get_total_rows()
        st.metric("Total Rows", total_rows)
    except Exception as e:
        st.error(f"Error accessing data: {e}")
        st.stop()
    
    # Page Size
    new_page_size = st.number_input("Page Size", value=st.session_state.page_size, min_value=10, max_value=1000)
    if new_page_size != st.session_state.page_size:
        st.session_state.page_size = new_page_size
        st.session_state.page = 1 # Reset to page 1 on size change
        st.rerun()

    # Pagination
    if total_rows > 0:
        total_pages = math.ceil(total_rows / st.session_state.page_size)
    else:
        total_pages = 1
        
    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button("Prev") and st.session_state.page > 1:
            st.session_state.page -= 1
            st.rerun()
    with c2:
        if st.button("Next") and st.session_state.page < total_pages:
            st.session_state.page += 1
            st.rerun()
            
    new_page = st.number_input("Page", value=st.session_state.page, min_value=1, max_value=total_pages)
    if new_page != st.session_state.page:
        st.session_state.page = new_page
        st.rerun()
    
    st.divider()
    
    # Save Button
    if st.button("Save Changes", type="primary"):
        if st.session_state.pending_edits:
            with st.spinner("Saving changes to disk..."):
                out_path = manager.save_edits(st.session_state.pending_edits)
                st.session_state.pending_edits = {}
                st.success(f"Saved successfully to {out_path}!")
                # We do NOT reload the data from the new file automatically to keep the source consistent,
                # but we clear the edits.
        else:
            st.info("No changes to save.")
            
    if st.session_state.pending_edits:
        st.warning(f"Unsaved edits: {len(st.session_state.pending_edits)} rows")

# --- Process Edits from Previous Interaction ---
if "editor" in st.session_state:
    edits = st.session_state["editor"].get("edited_rows", {})
    
    if edits and st.session_state.current_page_ids:
        # Map indices to IDs using the IDs from the PREVIOUS render
        for idx, changes in edits.items():
            # Safety check
            if idx < len(st.session_state.current_page_ids):
                row_id = st.session_state.current_page_ids[idx]
                
                if row_id not in st.session_state.pending_edits:
                    st.session_state.pending_edits[row_id] = {}
                st.session_state.pending_edits[row_id].update(changes)

# --- Main Area ---
st.title("High-Performance Data Dashboard")

# Sorting Controls
try:
    cols = manager.get_columns()
except Exception as e:
    st.error(f"Could not read columns: {e}")
    st.stop()

c1, c2, c3 = st.columns([2, 1, 4])
with c1:
    # Ensure sort_col is valid
    if st.session_state.sort_col not in cols and cols:
        st.session_state.sort_col = cols[0]
        
    sort_col = st.selectbox("Sort By", cols, index=cols.index(st.session_state.sort_col) if st.session_state.sort_col in cols else 0)
with c2:
    sort_desc = st.checkbox("Descending", value=st.session_state.sort_desc)

if sort_col != st.session_state.sort_col or sort_desc != st.session_state.sort_desc:
    st.session_state.sort_col = sort_col
    st.session_state.sort_desc = sort_desc
    st.rerun()

# --- Fetch Data ---
# Get raw data for current page
try:
    df_pl = manager.get_data(st.session_state.page, st.session_state.page_size, st.session_state.sort_col, st.session_state.sort_desc)
except Exception as e:
    st.error(f"Error fetching data: {e}")
    st.stop()

# --- Apply Pending Edits to View ---
# We need to patch the dataframe so the user sees their unsaved changes
if st.session_state.pending_edits:
    # Convert to pandas for easier row-wise patching or use Polars
    # Since we are displaying in st.data_editor which takes pandas/arrow, let's convert to pandas first
    # It's only one page (e.g. 100 rows), so it's fast.
    df_pd = df_pl.to_pandas()
    
    # Apply edits
    # We iterate through the rows in the current page
    for idx, row in df_pd.iterrows():
        r_id = row['id']
        if r_id in st.session_state.pending_edits:
            changes = st.session_state.pending_edits[r_id]
            for col, val in changes.items():
                if col in df_pd.columns:
                    df_pd.at[idx, col] = val
                    
    display_df = df_pd
else:
    display_df = df_pl.to_pandas()

# --- Update Current Page IDs for Next Run ---
# Important: Store the IDs of the rows we are ABOUT to display
if "id" in display_df.columns:
    st.session_state.current_page_ids = display_df["id"].tolist()
else:
    st.error("Dataset must have an 'id' column for editing to work.")
    st.session_state.current_page_ids = []

# --- Display Editor ---
edited_df = st.data_editor(
    display_df,
    key="editor",
    use_container_width=True,
    height=600,
    disabled=["id"] # Prevent editing ID
)
