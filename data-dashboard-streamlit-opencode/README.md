# Data Dashboard App

A powerful Streamlit-based data dashboard for viewing, editing, and managing data from multiple sources including local files, S3, and databases.

## Features

- 📁 **Multiple Data Sources**: Load data from local files (CSV/Parquet), AWS S3, or databases
- 🔍 **Data Preview**: Paginated view with customizable rows per page
- 🔀 **Sorting**: Sort data by any column in ascending or descending order
- ✏️ **Editing**: Edit data cells and add/remove rows (for datasets ≤ 10,000 rows)
- 💾 **Export**: Save modified data to Parquet format
- ⚡ **Performance**: Efficient lazy loading with Polars for large datasets

## Installation

### Prerequisites

- Python 3.13 or higher
- pip package manager

### Setup

1. **Clone or navigate to the project directory**:
   ```bash
   cd /home/avinash/home/codebase/ai-gen-folder/oc-stuff/data-dashboard-app
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -e .
   ```

   Or install directly from pyproject.toml:
   ```bash
   pip install streamlit polars pyarrow sqlalchemy boto3 connectorx
   ```

## Usage

### Running the Dashboard

Start the Streamlit application:
```bash
streamlit run main.py --server.headless true
```

The dashboard will open in your default web browser at `http://localhost:8501`

### Loading Data

#### Local Files
1. Select "Local File" from the sidebar
2. Enter the file path (e.g., `data/medical_records.csv`)
3. Click "Load Local File"
4. Supported formats: `.csv`, `.parquet`

#### AWS S3
1. Select "S3" from the sidebar
2. Enter S3 bucket name and key
3. Select file type (parquet or csv)
4. Enter AWS credentials
5. Click "Load from S3"

#### Database
1. Select "Database" from the sidebar
2. Enter connection string (e.g., `postgresql://user:pass@localhost/db`)
3. Enter SQL query
4. Click "Execute Query"

**Note**: Database connections require the `connectorx` package.

### Navigating Data

- **Rows per page**: Adjust the slider (10-1000 rows)
- **Page navigation**: Use the page number input
- **Sorting**: Select column and sort order (None/Ascending/Descending)

### Editing Data

For datasets with ≤ 10,000 rows:
1. View data in the editor
2. Make changes directly in cells
3. Add or remove rows using the editor controls
4. Click "Apply Edits to Full Data" to save changes

### Saving Data

1. Enter a file path in "Save Path (for Parquet)"
2. Click "Save to Parquet"
3. File will be saved in Parquet format

## Generating Sample Data

Generate synthetic test data similar to the existing CSV files:

```bash
python generate_data.py
```

This creates:
- `data/medical_records_synthetic.csv` (1,000 records)
- `data/health_insurance_synthetic.csv` (10,000 records)
- Parquet versions of both files

To customize the number of generated rows, edit `generate_data.py` and modify the `num_rows` parameters.

## Project Structure

```
data-dashboard-app/
├── main.py                    # Main Streamlit application
├── generate_data.py           # Synthetic data generator
├── pyproject.toml            # Project dependencies
├── README.md                 # This file
└── data/                     # Data directory
    ├── medical_records.csv
    ├── ntrarogyaseva.csv
    └── *_synthetic.*         # Generated files
```

## Dependencies

- **streamlit**: Web application framework
- **polars**: Fast DataFrame library with lazy execution
- **pyarrow**: Arrow memory format and Parquet support
- **sqlalchemy**: Database connectivity
- **boto3**: AWS S3 client
- **connectorx**: Fast database connector for Polars

## Performance Notes

- Datasets with > 10,000 rows: Editing disabled, lazy loading enabled
- Datasets with ≤ 10,000 rows: Full editing capabilities, eager loading
- Parquet format recommended for large datasets (faster loading)

## Troubleshooting

### Button requires multiple presses
This has been fixed. Data is now stored in session state for persistence.

### Page navigation not working
This has been fixed. Data persists across page changes.

### Database connection errors
Ensure `connectorx` is installed:
```bash
pip install connectorx
```

### S3 connection issues
- Verify AWS credentials are correct
- Check bucket name and key path
- Ensure IAM permissions allow S3 read access

### Memory issues with large datasets
- Use Parquet format instead of CSV
- The app uses lazy loading for large datasets automatically
- Consider filtering data in SQL queries before loading

## Examples

### Load CSV file:
```
File Path: data/medical_records.csv
```

### Load Parquet file:
```
File Path: data/medical_records_synthetic.parquet
```

### Database query example:
```sql
SELECT * FROM patients WHERE age > 50 LIMIT 1000
```

### PostgreSQL connection string:
```
postgresql://username:password@localhost:5432/database_name
```

### SQLite connection string:
```
sqlite:///path/to/database.db
```

## License

This project is provided as-is for data analysis and visualization purposes.

## Support

For issues or questions, please refer to the documentation or create an issue in the project repository.
