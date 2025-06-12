import matplotlib.pyplot as plt
from etl.extract import extract_from_csv
from etl.transform import transform_data
from etl.load import load_to_sqlite, save_analysis_results
from textblob import TextBlob

def generate_visualization(df):
    """Generate a bar chart of average discount by main category."""
    if df is None or df.empty:
        return

    avg_discount = df.groupby('main_category')['discount_percentage'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    plt.bar(avg_discount['main_category'], avg_discount['discount_percentage'], color='skyblue')
    plt.xlabel('Main Category')
    plt.ylabel('Average Discount (%)')
    plt.title('Average Discount by Main Category')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('avg_discount_by_category.png')
    plt.scatter(df['discounted_price'], df['rating'], alpha=0.5)
    plt.xlabel('Discounted Price')
    plt.ylabel('Rating')
    plt.title('Price vs. Rating')
    plt.savefig('price_vs_rating.png')
    df['sentiment'] = df['review_content'].apply(lambda x: TextBlob(x).sentiment.polarity)
    print("Saved visualization to avg_discount_by_category.png")

def run_etl():
    # Extract
    df = extract_from_csv('data/amazon_data.csv')

    # Transform
    df_transformed = transform_data(df)

    # Load
    load_to_sqlite(df_transformed, 'amazon.db', 'products')
    save_analysis_results(df_transformed, 'amazon.db')

    # Generate visualization
    generate_visualization(df_transformed)

    print("ETL pipeline completed!")




if __name__ == "__main__":
    run_etl()