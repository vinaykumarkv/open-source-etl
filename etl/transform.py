import pandas as pd
from textblob import TextBlob

def transform_data(df):
    """Transform the Amazon dataset."""
    if df is None or df.empty:
        print("Error: Input DataFrame is None or empty.")
        return None

    # Create a copy to avoid modifying the original
    df_transformed = df.copy()

    # Debug: Inspect rating and category columns
    print("Sample rating values before cleaning:", df_transformed['rating'].head(10).tolist())
    print("Rows with non-numeric ratings:",
          df_transformed[~df_transformed['rating'].astype(str).str.replace('.', '', 1).str.isnumeric()]['rating'].tolist())
    print("Sample category values:", df_transformed['category'].head(10).tolist())
    print("Rows with missing category:", df_transformed[df_transformed['category'].isna()]['product_id'].tolist())

    # Clean price columns (remove ₹, convert to numeric)
    df_transformed['discounted_price'] = pd.to_numeric(
        df_transformed['discounted_price'].str.replace('₹', '').str.replace(',', ''),
        errors='coerce'
    ).fillna(0)

    df_transformed['actual_price'] = pd.to_numeric(
        df_transformed['actual_price'].str.replace('₹', '').str.replace(',', ''),
        errors='coerce'
    ).fillna(0)

    # Clean discount_percentage (remove %, convert to numeric)
    df_transformed['discount_percentage'] = pd.to_numeric(
        df_transformed['discount_percentage'].str.replace('%', ''),
        errors='coerce'
    ).fillna(0)

    # Clean rating_count (remove commas, handle NaN, convert to integer)
    df_transformed['rating_count'] = pd.to_numeric(
        df_transformed['rating_count'].str.replace(',', '', regex=False),
        errors='coerce'
    ).fillna(0).astype(int)

    # Clean rating column (convert to numeric, handle invalid values)
    df_transformed['rating'] = pd.to_numeric(df_transformed['rating'], errors='coerce')
    # Fill missing ratings with the mean of valid ratings
    df_transformed['rating'] = df_transformed['rating'].fillna(df_transformed['rating'].mean())

    # Handle other missing values
    df_transformed['about_product'] = df_transformed['about_product'].fillna('No description')
    df_transformed['review_content'] = df_transformed['review_content'].fillna('No review')

    # Add sentiment analysis on review_content
    df_transformed['review_sentiment'] = df_transformed['review_content'].apply(
        lambda x: TextBlob(str(x)).sentiment.polarity
    )

    # Split category into main_category and sub_category
    df_transformed['main_category'] = df_transformed['category'].apply(
        lambda x: x.split('|')[0] if pd.notna(x) and '|' in str(x) else 'Unknown'
    )
    df_transformed['sub_category'] = df_transformed['category'].apply(
        lambda x: x.split('|')[1] if pd.notna(x) and len(x.split('|')) > 1 else 'Unknown'
    )

    # Debug: Inspect main_category and sentiment
    print("Sample main_category values:", df_transformed['main_category'].head(10).tolist())
    print("Unique main_category values:", df_transformed['main_category'].unique().tolist())
    print("Sample review_sentiment values:", df_transformed['review_sentiment'].head(10).tolist())

    # Calculate savings
    df_transformed['savings'] = df_transformed['actual_price'] - df_transformed['discounted_price']

    print(f"Transformed data: {len(df_transformed)} rows")
    return df_transformed