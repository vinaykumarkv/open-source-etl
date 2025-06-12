import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import sqlite3
from etl.extract import extract_from_csv
from etl.transform import transform_data
from etl.load import load_to_sqlite, save_analysis_results

def test_etl_pipeline():
    # Test extraction
    df = extract_from_csv('data/amazon_data.csv')
    assert df is not None, "Extraction failed"
    assert len(df) > 0, "No rows extracted"
    assert 'product_id' in df.columns, "product_id column missing"

    # Test transformation
    df_transformed = transform_data(df)
    assert df_transformed is not None, "Transformation failed"
    assert df_transformed['discounted_price'].dtype == float, "discounted_price not float"
    assert df_transformed['rating'].dtype == float, "rating not float"
    assert 'main_category' in df_transformed.columns, "main_category column missing"
    assert df_transformed['rating'].isnull().sum() == 0, "Missing ratings not handled"
    assert df_transformed['rating_count'].isnull().sum() == 0, "Missing rating_count not handled"
    assert df_transformed['rating_count'].dtype == int, "rating_count not integer"

    # Test loading
    db_name = 'test.db'
    table_name = 'products'
    load_to_sqlite(df_transformed, db_name, table_name)
    save_analysis_results(df_transformed, db_name)
    conn = sqlite3.connect(db_name)
    df_loaded = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    avg_discount = pd.read_sql("SELECT * FROM avg_discount_by_category", conn)
    top_products = pd.read_sql("SELECT * FROM top_products", conn)
    avg_rating = pd.read_sql("SELECT * FROM avg_rating_by_category", conn)
    conn.close()
    assert len(df_loaded) == len(df_transformed), "Loading failed"
    assert len(avg_discount) > 0, "Average discount analysis not saved"
    assert len(top_products) <= 5, "Top products analysis incorrect"
    assert len(avg_rating) > 0, "Average rating analysis not saved"

    # Clean up
    os.remove(db_name)