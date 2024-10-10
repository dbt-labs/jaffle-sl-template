import duckdb
import os
from pathlib import Path

# Set up paths
root = Path(__file__).parent.parent
data_dir = root / "jaffle-data"
db_file = data_dir / 'jaffle.db'

if __name__ == "__main__":
    # Connect to DuckDB
    conn = duckdb.connect(str(db_file))

    # Create the schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS dbt_sl_test")

    # Get all CSV files in the jaffle-data directory
    csv_files = list(data_dir.glob('*.csv'))

    # Read each CSV file and create a table in DuckDB
    for csv_file in csv_files:
        table_name = csv_file.stem  # Use filename without extension as table name
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS dbt_sl_test.{table_name} AS
            SELECT * FROM read_csv_auto('{csv_file}')
        """)
        print(f"Imported {csv_file} into table dbt_sl_test.{table_name}")

    # Close the connection
    conn.close()

    print(f"All CSV files have been imported into {db_file} under the 'dbt_sl_test' schema")

# To make the SELECT statement valid, you would use:
# conn.execute('SELECT * FROM "jaffle"."dbt_sl_test"."raw_stores"')
