from config import init_pool, get_connection, release_connection
import pandas as pd
import re
import oracledb

def extract_csv(path="data/input.csv"):
    return pd.read_csv(path)

def transform_data(df):
    df['account_number'] = df['account_number'].astype(str).str.zfill(10)

    pan_regex = re.compile(r'[A-Z]{5}[0-9]{4}[A-Z]{1}')
    df['pan_valid'] = df['pan'].apply(lambda x: bool(pan_regex.match(str(x))))

    df = df[df['pan_valid']]

    return df

def load_to_oracle(df):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        for index, row in df.iterrows():
            try:
                query = """
                INSERT INTO customer_data (account_number, pan, amount, age)
                VALUES (:account_number, :pan, :amount, TO_DATE(:age, 'YYYY-MM-DD'))
                """
                cursor.execute(query, {
                    'account_number': row['account_number'],
                    'pan': row['pan'],
                    'amount': row['amount'],
                    'age': row['age']
                })
            except oracledb.IntegrityError as e:
                # This will catch unique constraint violations
                print(f"Duplicate account_number {row['account_number']} skipped. Error: {e}")

            except Exception as e:
                # Catch any other DB errors
                print(f"Unexpected error inserting {row['account_number']}: {e}")

        conn.commit()
    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)

def fetch_from_oracle():
    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT account_number, pan, amount, age FROM customer_data"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    release_connection(conn)
    return rows

def print_data(rows):
    for row in rows:
        print(f'Account Number: {row[0]}, PAN: {row[1]}, Amount: {row[2]}, Age: {row[3]}')

if __name__ == "__main__":
    init_pool()
    raw = extract_csv()
    clean = transform_data(raw)
    clean.to_csv("data/output.csv", index=False)
    load_to_oracle(clean)
    rows = fetch_from_oracle()
    print_data(rows)
    print("ETL pipeline complete.")