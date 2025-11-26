import polars as pl
import numpy as np
from faker import Faker
import os

def generate_data(num_rows=1_000_000, output_file="data/large_dataset.parquet"):
    print(f"Generating {num_rows} rows of data...")
    fake = Faker()
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Generate data using numpy for speed where possible
    # We'll create a few columns: ID, Name, Category, Value, Date, Active
    
    ids = np.arange(num_rows)
    categories = np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows)
    values = np.random.rand(num_rows) * 1000
    actives = np.random.choice([True, False], num_rows)
    
    # For names and dates, it's a bit slower with pure python/faker in loop, 
    # so let's use some repetition or simpler generation for speed if we want 1M rows quickly.
    # Or just use random strings.
    
    # Let's stick to simple efficient generation for the bulk
    
    df = pl.DataFrame({
        "id": ids,
        "category": categories,
        "value": values,
        "is_active": actives,
        # Add a string column that is slightly more varied
        "department": np.random.choice(['Sales', 'Engineering', 'HR', 'Marketing', 'Support'], num_rows)
    })
    
    # Save to parquet
    print(f"Saving to {output_file}...")
    df.write_parquet(output_file)
    
    # Save to CSV
    csv_file = output_file.replace(".parquet", ".csv")
    print(f"Saving to {csv_file}...")
    df.write_csv(csv_file)
    
    print("Done!")

if __name__ == "__main__":
    generate_data()
