import mysql.connector
from mysql.connector import Error

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "Yash@123"
DATABASE = "ipl_analytics"

def get_connection(use_db=True):
    try:
        conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE if use_db else None
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Connection error: {e}")
        return None

def create_database_and_schemas():
    conn = get_connection(use_db=False)
    if not conn:
        return
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cursor.execute(f"USE {DATABASE}")

    cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Database '{DATABASE}' and schemas bronze/silver/gold are ready.")

if __name__ == "__main__":
    create_database_and_schemas()

    conn = get_connection()
    if conn:
        print(f"Connected to MySQL at {HOST}:{PORT} as '{USER}'")
        conn.close()
