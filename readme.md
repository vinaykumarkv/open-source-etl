# Amazon ETL Framework
A Python-based ETL framework for processing Amazon product data.

## Setup
1. Install Python 3.8+.
2. Clone this repository.
3. Create a virtual environment: `python -m venv venv`
4. Activate it: `source venv/bin/activate` (Linux/macOS) or `venv\Scripts\activate` (Windows).
5. Install dependencies: `pip install -r requirements.txt`
6. Place your Amazon dataset in `data/amazon_data.csv`.
7. Run the ETL pipeline: `python main.py`

## Features
- Extracts data from a CSV file.
- Transforms data (cleans prices, splits categories, handles missing values).
- Loads data into a SQLite database.
- Performs basic analysis (e.g., average discount by category).
- Generates visualizations (e.g., discount distribution).
- Includes automated tests and CI/CD pipeline.