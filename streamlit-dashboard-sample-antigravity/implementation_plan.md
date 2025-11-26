# High-Performance Data Dashboard Implementation Plan

## Goal
Create a fast, responsive data dashboard using Streamlit, Polars, and PyArrow. The dashboard must handle large datasets via server-side pagination and processing, allowing users to view, sort, edit, and save data.

## User Review Required
- **Framework Choice**: Using Streamlit as implied by the workspace name.
- **Editing Strategy**: To handle "regardless of size", we will use server-side pagination. Edits will be applied by tracking unique row IDs, as `st.data_editor` only edits the visible slice.

## Proposed Changes

### Project Setup
- Initialize `uv` project.
- Dependencies: `streamlit`, `polars`, `pyarrow`, `faker` (for data gen).

### Data Management (`data_manager.py`)
- **Class `DataManager`**:
    - `load_data(path)`: Returns Polars LazyFrame.
    - `get_page(page, page_size, sort_col, sort_desc)`: Returns a slice of the data as a DataFrame for display.
    - `apply_edits(edits, original_file_path)`: Updates the underlying Parquet file. *Note: Updating Parquet requires rewriting the file or partition. For this demo, we might rewrite the file or use a delta approach if feasible, but rewriting is standard for simple Parquet usage.*

### UI (`app.py`)
- **Layout**:
    - Sidebar: File selector, Page controls (Page #, Page Size), Save button.
    - Main: `st.data_editor`.
- **Logic**:
    - Maintain state of current page and sort order.
    - On edit: Capture edits from `st.data_editor`.
    - Since `st.data_editor` edits are transient, we need to persist them to a session state "change buffer" or apply immediately.
    - **Optimized Edit Flow**:
        1. User edits a cell.
        2. `st.data_editor` returns `edited_rows`.
        3. We map these edits to the unique ID of the row.
        4. We update a "pending changes" dictionary in `st.session_state`.
        5. When "Save" is clicked, we load the full dataset (or stream it), apply changes, and write back to Parquet.

### Data Source Selection
- **UI**: Add Sidebar inputs for Source Type (Local, Cloud, Database).
- **Local**: Input for file path.
- **Cloud**: Input for URL (s3://, gs://, etc.).
- **Database**: Input for Connection String and Query.
- **DataManager Refactor**:
    - `load_data` will switch based on source type.
    - For DB, use `pl.read_database` (lazy if possible, or eager if needed). Polars `read_database` is eager by default, `read_database_uri` can be lazy with `connectorx`.
    - **Saving**: Always save as Parquet to a specified output folder, regardless of source. This avoids complex write-back logic for DBs/Cloud for now, satisfying the "save to file folder as parquet" requirement.

### CSV Support
- **DataManager**:
    - Detect file extension or explicit type.
    - Use `pl.scan_csv` for local/cloud CSVs.
    - Ensure `save_edits` still saves as Parquet (as per original requirement "save ... as parquet files"), or offer CSV save option? User said "save the modified data to file folder as parquet files", so we stick to Parquet for output.

### Data Generator (`generate_data.py`)
- Create a script to generate a dummy parquet file with 1M+ rows to test performance.

## Verification Plan
### Automated Tests
- None planned for UI, but manual verification is key.
### Manual Verification
- Run `generate_data.py`.
- Run `uv run streamlit run app.py`.
- Test pagination speed.
- Test sorting speed.
- Edit a value on Page 5.
- Change page, come back, verify edit (if we implement session persistence).
- Click Save.
- Restart app, verify edit is saved to disk.
