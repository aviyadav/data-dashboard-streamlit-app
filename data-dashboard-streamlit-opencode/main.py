import streamlit as st
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
import boto3
from sqlalchemy import create_engine
import tempfile

def load_data_from_local(file_path, file_type):
    if file_type == 'parquet':
        return pl.scan_parquet(file_path)
    elif file_type == 'csv':
        return pl.scan_csv(file_path)
    else:
        raise ValueError("Unsupported file type")

def load_data_from_s3(bucket, key, file_type, aws_access_key=None, aws_secret_key=None):
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key)
    with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_type}') as tmp:
        tmp_path = tmp.name
    try:
        s3_client.download_file(bucket, key, tmp_path)
        data = load_data_from_local(tmp_path, file_type)
        return data
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def load_data_from_db(connection_string, query):
    engine = create_engine(connection_string)
    return pl.read_database(query, engine).lazy()

def main():
    st.title("Data Dashboard")

    # Sidebar for data source selection
    st.sidebar.header("Data Source")

    source_type = st.sidebar.selectbox("Select Data Source", ["Local File", "S3", "Database"])

    # Initialize from session state if available
    data = st.session_state.get('loaded_data', None)
    file_type = st.session_state.get('file_type', None)

    if source_type == "Local File":
        file_path = st.sidebar.text_input("File Path")
        if st.sidebar.button("Load Local File"):
            if file_path:
                path = Path(file_path)
                if path.exists():
                    file_type = path.suffix[1:].lower()  # remove dot
                    if file_type in ['parquet', 'csv']:
                        try:
                            data = load_data_from_local(file_path, file_type)
                            st.session_state.loaded_data = data
                            st.session_state.file_type = file_type
                            st.sidebar.success("Data loaded successfully")
                        except Exception as e:
                            st.sidebar.error(f"Error loading data: {e}")
                    else:
                        st.sidebar.error("Unsupported file type. Use .parquet or .csv")
                else:
                    st.sidebar.error("File does not exist")
            else:
                st.sidebar.error("Please enter a file path")

    elif source_type == "S3":
        bucket = st.sidebar.text_input("S3 Bucket")
        key = st.sidebar.text_input("S3 Key")
        file_type_input = st.sidebar.selectbox("File Type", ["parquet", "csv"])
        aws_access_key = st.sidebar.text_input("AWS Access Key", type="password")
        aws_secret_key = st.sidebar.text_input("AWS Secret Key", type="password")
        if st.sidebar.button("Load from S3"):
            try:
                data = load_data_from_s3(bucket, key, file_type_input, aws_access_key, aws_secret_key)
                st.session_state.loaded_data = data
                st.session_state.file_type = file_type_input
                file_type = file_type_input
                st.sidebar.success("Data loaded successfully")
            except Exception as e:
                st.sidebar.error(f"Error loading data: {e}")

    elif source_type == "Database":
        connection_string = st.sidebar.text_input("Connection String", type="password")
        query = st.sidebar.text_area("SQL Query")
        if st.sidebar.button("Execute Query"):
            try:
                data = load_data_from_db(connection_string, query)
                st.session_state.loaded_data = data
                st.session_state.file_type = 'db'
                file_type = 'db'  # arbitrary
                st.sidebar.success("Data loaded successfully")
            except Exception as e:
                st.sidebar.error(f"Error loading data: {e}")

    # Display data if loaded
    if data is not None:
        total_rows = data.select(pl.len()).collect().item()
        st.write(f"Total rows: {total_rows}")

        # Performance: if large dataset, keep lazy, no editing
        allow_editing = total_rows <= 10000
        if allow_editing:
            if 'full_data' not in st.session_state:
                st.session_state.full_data = data.collect()
            data_to_use = st.session_state.full_data
        else:
            data_to_use = data
            st.warning("Dataset is large. Editing disabled. Only display and save original data.")

        st.header("Data Preview")

        # Pagination
        page_size = st.slider("Rows per page", 10, 1000, 100)
        max_page = (total_rows // page_size) + 1
        page = int(st.number_input("Page", min_value=1, max_value=max_page, value=1))

        start_row = (page - 1) * page_size

        # Column sorting
        if allow_editing:
            columns = data_to_use.columns
            sorted_data = data_to_use
        else:
            columns = data_to_use.collect_schema().names()
            sorted_data = data_to_use
        
        sort_col = st.selectbox("Sort by column", columns)
        sort_order = st.selectbox("Sort order", ["None", "Ascending", "Descending"])
        
        if sort_order == "Ascending":
            sorted_data = sorted_data.sort(sort_col) if not allow_editing else sorted_data.lazy().sort(sort_col)
        elif sort_order == "Descending":
            sorted_data = sorted_data.sort(sort_col, descending=True) if not allow_editing else sorted_data.lazy().sort(sort_col, descending=True)

        # Get page data
        if allow_editing and sort_order == "None":
            df_page = sorted_data.slice(start_row, page_size)
        else:
            df_page = sorted_data.slice(start_row, page_size).collect() if hasattr(sorted_data, 'collect') else sorted_data.slice(start_row, page_size)

        # Display data
        st.dataframe(df_page, width='stretch')

        # Cell editing
        if allow_editing:
            st.header("Edit Data")
            edited_df = st.data_editor(df_page, num_rows="dynamic", key=f"editor_page_{page}")
            if st.button("Apply Edits to Full Data"):
                try:
                    # Convert edited page back to polars if needed
                    if not isinstance(edited_df, pl.DataFrame):
                        edited_df = pl.from_pandas(edited_df)
                    
                    # Merge edits back into full dataset
                    if edited_df.height <= df_page.height:
                        # Update existing rows
                        rows_before = st.session_state.full_data.slice(0, start_row)
                        rows_after = st.session_state.full_data.slice(start_row + edited_df.height)
                        st.session_state.full_data = pl.concat([rows_before, edited_df, rows_after])
                    else:
                        # Handle new rows added
                        original_rows = df_page.height
                        updated_existing = edited_df.slice(0, original_rows)
                        new_rows = edited_df.slice(original_rows)
                        
                        rows_before = st.session_state.full_data.slice(0, start_row)
                        rows_after = st.session_state.full_data.slice(start_row + original_rows)
                        st.session_state.full_data = pl.concat([rows_before, updated_existing, new_rows, rows_after])
                    
                    st.success("Edits applied successfully")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error applying edits: {e}")
        else:
            st.header("Editing Disabled")
            st.info("For large datasets, editing is disabled to maintain performance.")

        # Save to Parquet
        st.header("Save Data")
        save_path = st.text_input("Save Path (for Parquet)")
        if st.button("Save to Parquet"):
            if save_path:
                try:
                    if allow_editing and 'full_data' in st.session_state:
                        st.session_state.full_data.write_parquet(save_path)
                    else:
                        data.collect().write_parquet(save_path)
                    st.success(f"Data saved to {save_path}")
                except Exception as e:
                    st.error(f"Error saving data: {e}")
            else:
                st.error("Please provide a save path")

if __name__ == "__main__":
    main()
