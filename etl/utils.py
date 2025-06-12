import pandas as pd

def load_data(file_path):
    """Load sales data from a CSV file."""
    return pd.read_csv(file_path)

def clean_data(df):
    """Clean sales data by removing nulls and fixing types."""
    df = df.dropna(subset=['product_id', 'rating_count'])
    df['SalesAmount'] = df['SalesAmount'].astype(float)
    return df

def calculate_total_sales(df):
    """Calculate total sales."""
    return df['SalesAmount'].sum()

def calculate_average_order_value(df):
    """Calculate average order value."""
    return df['SalesAmount'].mean()

def save_results(df, file_path):
    """Save DataFrame to CSV."""
    df.to_csv(file_path, index=False)
