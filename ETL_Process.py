import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
from io import StringIO

# Instead of passing response.text directly to read_html, wrap it in StringIO


# Function to log progress
def log_progress(message):
    with open("code_log.txt", "a") as log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{timestamp} : {message}\n")


# Function to extract relevant data
def extract(url):
    log_progress("Starting data extraction process")

    # Fetch the webpage content
    response = requests.get(url)

    # Parse all tables from the page using StringIO
    tables = pd.read_html(StringIO(response.text))  # Use response.text here

    # Parse the first table
    df = tables[0]  # Assume the first table is the correct one

    # Normalize column names
    df.columns = df.columns.str.strip().str.replace("\n", "", regex=True)

    # Debug: Print available columns
    print("Extracted columns:", df.columns)

    # Extract the relevant columns directly
    try:
        df = df[['Bank name', 'Market cap (US$ billion)']].copy()
    except KeyError as e:
        log_progress(f"KeyError during extraction: {e}")
        raise

    # Rename columns for consistency
    df.rename(columns={
        'Bank name': 'Bank_Name',
        'Market cap (US$ billion)': 'MC_USD_Billion'
    }, inplace=True)

    log_progress("Data extraction complete")
    print(df)
    return df

# Function to transform data
def transform(df):
    exchange_rate = pd.read_csv('exchange_rate.csv')
    exchange_rate = exchange_rate.set_index('Currency').T
    exchange_rate = exchange_rate.loc['Rate']
    print(exchange_rate)

    # Convert Market Cap to numeric, handling errors
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')

    # Add converted market cap values in other currencies
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete")
    print(df)
    return df


# Function to load data into a CSV file
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    log_progress(f"Data saved to CSV file at {csv_path}")


# Function to load data into a SQLite database
def load_to_db(df, db_path, table_name):
    # Ensure all columns are supported by SQLite
    df = df.convert_dtypes()  # Automatically convert to appropriate dtypes

    # Specifically ensure that all 'Series' type columns are converted to numeric types where necessary
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    df['MC_GBP_Billion'] = pd.to_numeric(df['MC_GBP_Billion'], errors='coerce')
    df['MC_EUR_Billion'] = pd.to_numeric(df['MC_EUR_Billion'], errors='coerce')
    df['MC_INR_Billion'] = pd.to_numeric(df['MC_INR_Billion'], errors='coerce')

    conn = sqlite3.connect(db_path)

    try:
        # Load data into the database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        log_progress(f"Data saved to SQL database at {db_path}, table {table_name}")
    except Exception as e:
        print(f"Error saving data to SQL: {e}")
    finally:
        conn.close()



# Function to run and display SQL queries
def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    print(f"Query: {query_statement}")
    for result in results:
        print(result)


# Main ETL process
def main():
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

    log_progress("Starting ETL process")

    # Extract phase
    df = extract(url)

    # Transform phase
    transformed_df = transform(df)

    # Load phase
    csv_path = './Largest_banks_data.csv'
    load_to_csv(transformed_df, csv_path)

    db_path = './Banks.db'
    table_name = 'Largest_banks'
    load_to_db(transformed_df, db_path, table_name)

    # Querying the database
    conn = sqlite3.connect(db_path)

    print("\nFull table data:")
    run_query("SELECT * FROM Largest_banks", conn)

    print("\nAverage market cap in GBP:")
    run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)

    print("\nTop 5 bank names:")
    run_query("SELECT Bank_Name FROM Largest_banks LIMIT 5", conn)

    conn.close()
    log_progress("ETL process complete")


if __name__ == "__main__":
    main()


