# High-Performance Data Dashboard Walkthrough

## Overview
This dashboard allows users to view, sort, edit, and save large datasets (Parquet/CSV) efficiently using Polars and Streamlit. It supports local files, cloud URIs, and database connections.

## Features
- **High Performance**: Uses Polars LazyFrames and server-side pagination to handle millions of rows.
- **Data Editing**: Edit data directly in the grid. Changes are tracked and applied to the source upon saving.
- **Multi-Source Support**:
    - **Local**: Parquet or CSV files.
    - **Cloud**: S3, GCS, etc. (via `fsspec`).
    - **Database**: SQL queries via `connectorx`.
- **Sorting**: Server-side sorting.

## Usage
1. **Start the App**:
   ```bash
   uv run streamlit run app.py
   ```
2. **Select Data Source**:
   - In the sidebar, choose "Local", "Cloud", or "Database".
   - Enter the path or connection details.
   - Click "Load Data".
3. **Navigate**:
   - Use "Prev"/"Next" buttons or enter a page number.
   - Change "Page Size" to control rows per page.
4. **Sort**:
   - Use the "Sort By" dropdown and "Descending" checkbox.
5. **Edit**:
   - Double-click a cell to edit.
   - Edits are persisted in memory until you click "Save".
6. **Save**:
   - Click "Save Changes" in the sidebar.
   - Modified data is saved to a new Parquet file in `data/modified/`.

## Verification Results
- **Performance**: Verified with 1M row dataset. Pagination and sorting are near-instant.
- **Editing**: Edits are correctly mapped to row IDs and saved.
- **CSV Support**: Verified loading CSV files (auto-detected).
