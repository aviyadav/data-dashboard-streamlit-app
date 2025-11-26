# High-Performance Data Dashboard

A high-performance data dashboard built with **Streamlit**, **Polars**, and **PyArrow**. Designed to handle large datasets efficiently with server-side pagination, sorting, and editing capabilities.

## Features

- **High Performance**: Handles millions of rows using Polars LazyFrames and server-side processing.
- **Multi-Source Support**:
    - **Local Files**: Parquet and CSV.
    - **Cloud Storage**: S3, GCS, Azure Blob (via `fsspec` URIs).
    - **Databases**: SQL queries via `connectorx`.
- **Interactive Editing**: Edit data directly in the grid. Edits are tracked and applied to the source upon saving.
- **Efficient Saving**: Modified data is saved as optimized Parquet files.

## Project Documentation

- [Implementation Plan](implementation_plan.md): Detailed technical plan and architecture.
- [Task List](task.md): Breakdown of development tasks and status.
- [Walkthrough](walkthrough.md): Guide to using the dashboard and verification results.

## Setup

This project uses `uv` for dependency management.

1.  **Initialize and Install Dependencies**:
    ```bash
    uv sync
    ```

## Usage

### 1. Generate Test Data
To test the dashboard with a large dataset (1M rows), run the generator script:
```bash
uv run generate_data.py
```
This will create `data/large_dataset.parquet` and `data/large_dataset.csv`.

### 2. Run the Dashboard
Start the Streamlit app:
```bash
uv run streamlit run app.py
```

### 3. Using the Dashboard
- **Load Data**: Select "Local", "Cloud", or "Database" in the sidebar.
    - For local testing, use `data/large_dataset.parquet` or `data/large_dataset.csv`.
- **Navigate**: Use the pagination controls in the sidebar.
- **Sort**: Use the sorting controls above the grid.
- **Edit**: Double-click cells to edit.
- **Save**: Click "Save Changes" in the sidebar to write your edits to a new Parquet file in `data/modified/`.

## Dependencies
- `streamlit`
- `polars`
- `pyarrow`
- `faker`
- `connectorx`
- `fsspec`, `s3fs`, `adlfs`
