import matplotlib.pyplot as plt
from etl.extract import extract_from_csv
from etl.transform import transform_data
from etl.load import load_to_sqlite, save_analysis_results

def generate_visualizations(df):
    """Generate visualizations for the Amazon dataset."""
    if df is None or df.empty:
        return

    # Bar chart: Average discount by main category
    avg_discount = df.groupby('main_category')['discount_percentage'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    plt.bar(avg_discount['main_category'], avg_discount['discount_percentage'], color='skyblue')
    plt.xlabel('Main Category')
    plt.ylabel('Average Discount (%)')
    plt.title('Average Discount by Main Category')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('avg_discount_by_category.png')
    plt.close()
    print("Saved visualization to avg_discount_by_category.png")

    # Bar chart: Average rating by main category
    avg_rating = df.groupby('main_category')['rating'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    plt.bar(avg_rating['main_category'], avg_rating['rating'], color='lightgreen')
    plt.xlabel('Main Category')
    plt.ylabel('Average Rating')
    plt.title('Average Rating by Main Category')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('avg_rating_by_category.png')
    plt.close()
    print("Saved visualization to avg_rating_by_category.png")

    # Scatter plot: High-value products (discount vs. rating)
    high_value = df[(df['discount_percentage'] > 50) & (df['rating'] > 4.0)]
    if not high_value.empty:
        plt.figure(figsize=(10, 6))
        plt.scatter(high_value['discount_percentage'], high_value['rating'], alpha=0.5, color='purple')
        plt.xlabel('Discount Percentage (%)')
        plt.ylabel('Rating')
        plt.title('High-Value Products (Discount > 50%, Rating > 4.0)')
        plt.tight_layout()
        plt.savefig('high_value_products.png')
        plt.close()
        print("Saved visualization to high_value_products.png")

    # Bar chart: Average sentiment by main category
    avg_sentiment = df.groupby('main_category')['review_sentiment'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    plt.bar(avg_sentiment['main_category'], avg_sentiment['review_sentiment'], color='coral')
    plt.xlabel('Main Category')
    plt.ylabel('Average Sentiment Polarity')
    plt.title('Average Review Sentiment by Main Category')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('avg_sentiment_by_category.png')
    plt.close()
    print("Saved visualization to avg_sentiment_by_category.png")

    plt.figure(figsize=(10, 6))
    plt.hist(df['review_sentiment'], bins=20, color='orange')
    plt.xlabel('Review Sentiment Polarity')
    plt.ylabel('Frequency')
    plt.title('Distribution of Review Sentiment')
    plt.savefig('sentiment_distribution.png')
    plt.close()
    print("Saved visualization to sentiment_distribution.png")

def run_etl():
    # Extract
    df = extract_from_csv('data/amazon_data.csv')

    # Transform
    df_transformed = transform_data(df)

    # Load
    load_to_sqlite(df_transformed, 'amazon.db', 'products')
    save_analysis_results(df_transformed, 'amazon.db')

    # Generate correlations
    print("Correlation between rating, discount_percentage, rating_count, and review_sentiment:")
    correlation_matrix = df_transformed[['rating', 'discount_percentage', 'rating_count', 'review_sentiment']].corr()
    print(correlation_matrix)

    # Generate visualizations
    generate_visualizations(df_transformed)

    print("ETL pipeline completed!")

if __name__ == "__main__":
    run_etl()