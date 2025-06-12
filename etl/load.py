import sqlite3
import pandas as pd


def load_to_sqlite(df, db_name, table_name):
    """Load data into a SQLite database."""
    if df is None or df.empty:
        print("Error: Input DataFrame is None or empty.")
        return

    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Loaded {len(df)} rows into {table_name} in {db_name}")
        conn.close()
    except Exception as e:
        print(f"Error during loading: {e}")


def save_analysis_results(df, db_name):
    """Save analysis results to SQLite."""
    if df is None or df.empty:
        print("Error: Input DataFrame is None or empty in save_analysis_results.")
        return

    try:
        conn = sqlite3.connect(db_name)

        # Average discount by main_category
        avg_discount = df.groupby('main_category')['discount_percentage'].mean().reset_index()
        print(f"Average discount DataFrame: {len(avg_discount)} rows")
        avg_discount.to_sql('avg_discount_by_category', conn, if_exists='replace', index=False)
        print("Saved average discount analysis to SQLite.")

        # Top 5 products by rating_count
        top_products = df[['product_id', 'product_name', 'rating', 'rating_count']].sort_values(by='rating_count',
                                                                                                ascending=False).head(5)
        print(f"Top products DataFrame: {len(top_products)} rows")
        top_products.to_sql('top_products', conn, if_exists='replace', index=False)
        print("Saved top products analysis to SQLite.")

        # Top 5 categories by average rating
        avg_rating_by_category = df.groupby('main_category')['rating'].mean().reset_index().sort_values(by='rating',
                                                                                                        ascending=False).head(
            5)
        print(f"Average rating by category DataFrame: {len(avg_rating_by_category)} rows")
        if avg_rating_by_category.empty:
            print("Warning: avg_rating_by_category DataFrame is empty!")
        avg_rating_by_category.to_sql('avg_rating_by_category', conn, if_exists='replace', index=False)
        print("Saved average rating by category analysis to SQLite.")

        # Average sentiment by main_category
        avg_sentiment_by_category = df.groupby('main_category')['review_sentiment'].mean().reset_index().sort_values(
            by='review_sentiment', ascending=False).head(5)
        print(f"Average sentiment by category DataFrame: {len(avg_sentiment_by_category)} rows")
        if avg_sentiment_by_category.empty:
            print("Warning: avg_sentiment_by_category DataFrame is empty!")
        avg_sentiment_by_category.to_sql('avg_sentiment_by_category', conn, if_exists='replace', index=False)
        print("Saved average sentiment by category analysis to SQLite.")

        conn.close()
    except Exception as e:
        print(f"Error saving analysis results: {e}")