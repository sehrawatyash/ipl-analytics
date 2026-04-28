from database_connection import get_connection
import pandas as pd

def create_bronze_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.points_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pos VARCHAR(10), grp VARCHAR(10), team VARCHAR(100),
            pld VARCHAR(10), w VARCHAR(10), l VARCHAR(10), nr VARCHAR(10),
            pts VARCHAR(10), nrr VARCHAR(20), qualification VARCHAR(200),
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.batting_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            runs VARCHAR(20), player VARCHAR(100), team VARCHAR(100),
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bowling_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            wickets VARCHAR(20), player VARCHAR(100), team VARCHAR(100),
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Bronze tables created.")

def load_csv_to_table(cursor, conn, csv_file, table, columns):
    df = pd.read_csv(csv_file).fillna("")
    df.columns = [c.lower() for c in df.columns]
    cursor.execute(f"TRUNCATE TABLE {table}")
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        values = tuple(str(row[c]) if c in df.columns else "" for c in columns)
        cursor.execute(sql, values)
    conn.commit()
    print(f"Loaded {len(df)} rows into {table}")

if __name__ == "__main__":
    conn = get_connection()
    if not conn:
        print("Could not connect to database.")
        exit(1)
    cursor = conn.cursor()
    create_bronze_tables(cursor)
    load_csv_to_table(cursor, conn, "points_table_raw.csv", "bronze.points_table",
        ["pos", "grp", "team", "pld", "w", "l", "nr", "pts", "nrr", "qualification"])
    load_csv_to_table(cursor, conn, "batting_stats_raw.csv", "bronze.batting_stats",
        ["runs", "player", "team"])
    load_csv_to_table(cursor, conn, "bowling_stats_raw.csv", "bronze.bowling_stats",
        ["wickets", "player", "team"])
    cursor.close()
    conn.close()
    print("\nBronze layer loading complete.")
