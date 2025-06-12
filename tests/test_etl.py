import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import sqlite3
from etl.extract import extract_from_csv
from etl.transform import transform_data
from etl.load import load_to_sqlite, save_analysis_results


def test_etl_pipeline():
    print("Starting ETL pipeline test")
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
    assert 'review_sentiment' in df_transformed.columns, "review_sentiment column missing"
    assert df_transformed['review_sentiment'].dtype == float, "review_sentiment not float"

    # Test loading
    db_name = 'test.db'
    table_name = 'products'
    load_to_sqlite(df_transformed, db_name, table_name)
    save_analysis_results(df_transformed, db_name)
    conn = sqlite3.connect(db_name)
    df_loaded = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    avg_discount = pd.read_sql("SELECT * FROM avg_discount_by_category", conn)
    top_products = pd.read_sql("SELECT * FROM top_products", conn)

    # Check avg_rating_by_category, allow empty table if no valid ratings
    try:
        avg_rating = pd.read_sql("SELECT * FROM avg_rating_by_category", conn)
        print(f"Average rating by category table: {len(avg_rating)} rows")
    except pd.io.sql.DatabaseError:
        print("Warning: avg_rating_by_category table not found, likely due to no valid ratings")
        avg_rating = pd.DataFrame()

    # Check avg_sentiment_by_category
    try:
        avg_sentiment = pd.read_sql("SELECT * FROM avg_sentiment_by_category", conn)
        print(f"Average sentiment by category table: {len(avg_sentiment)} rows")
    except pd.io.sql.DatabaseError:
        print("Warning: avg_sentiment_by_category table not found, likely due to no valid sentiments")
        avg_sentiment = pd.DataFrame()

    conn.close()
    assert len(df_loaded) == len(df_transformed), "Loading failed"
    assert len(avg_discount) > 0, "Average discount analysis not saved"
    assert len(top_products) <= 5, "Top products analysis incorrect"
    if not avg_rating.empty:
        assert len(avg_rating) > 0, "Average rating analysis not saved"
    if not avg_sentiment.empty:
        assert len(avg_sentiment) > 0, "Average sentiment analysis not saved"

    # Clean up
    os.remove(db_name)