import pandas as pd

def extract_from_csv(file_path):
    """Extract data from a CSV file."""
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"Extracted {len(df)} rows from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None