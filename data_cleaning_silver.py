from database_connection import get_connection
import pandas as pd
import re

# ─────────────────────────────────────────────
# CLEANING SUMMARY
# ─────────────────────────────────────────────
# POINTS TABLE:
#   1. Cast pos, pld, w, l, nr, pts from VARCHAR to INT
#   2. Cast nrr from VARCHAR to FLOAT  (handles unicode minus sign − → -)
#   3. Clean qualification column: fix "Advance to thequalifier 1" → "Qualifier 1"
#      and "Advance to theeliminator" → "Eliminator", fill blanks as "Eliminated"
#   4. Strip extra whitespace from team names
#   5. Add win_pct column = wins / matches_played * 100
#
# BATTING STATS:
#   1. Cast runs from VARCHAR to INT
#   2. Strip whitespace from player / team names
#   3. Add rank column (1–5)
#
# BOWLING STATS:
#   1. Remove rows where wickets is not a number (e.g. "5 players", player names in wrong column)
#   2. Cast wickets from VARCHAR to INT
#   3. Strip whitespace from player / team names
#   4. Add rank column
# ─────────────────────────────────────────────

def create_silver_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver.points_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pos INT,
            grp VARCHAR(5),
            team VARCHAR(100),
            pld INT,
            w INT,
            l INT,
            nr INT,
            pts INT,
            nrr FLOAT,
            qualification VARCHAR(100),
            win_pct FLOAT,
            cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver.batting_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rank_pos INT,
            player VARCHAR(100),
            team VARCHAR(100),
            runs INT,
            cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver.bowling_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rank_pos INT,
            player VARCHAR(100),
            team VARCHAR(100),
            wickets INT,
            cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Silver tables created.")

def clean_points_table(df):
    df = df.copy()

    # Fix unicode minus sign (−) to standard hyphen (-)
    df['nrr'] = df['nrr'].astype(str).str.replace('\u2212', '-', regex=False)

    # Cast numeric columns
    for col in ['pos', 'pld', 'w', 'l', 'nr', 'pts']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    df['nrr'] = pd.to_numeric(df['nrr'], errors='coerce').fillna(0.0)

    # Clean team names
    df['team'] = df['team'].str.strip()

    # Fix qualification column
    def fix_qual(val):
        val = str(val).strip()
        if 'qualifier 1' in val.lower() or 'qualifier1' in val.lower():
            return 'Qualifier 1'
        elif 'eliminator' in val.lower():
            return 'Eliminator'
        elif val == '' or val.lower() == 'none' or val.lower() == 'nan':
            return 'Eliminated'
        return val
    df['qualification'] = df['qualification'].apply(fix_qual)

    # Add win percentage
    df['win_pct'] = (df['w'] / df['pld'].replace(0, 1) * 100).round(2)

    return df[['pos', 'grp', 'team', 'pld', 'w', 'l', 'nr', 'pts', 'nrr', 'qualification', 'win_pct']]

def clean_batting_stats(df):
    df = df.copy()
    df['player'] = df['player'].str.strip()
    df['team'] = df['team'].str.strip()
    df['runs'] = pd.to_numeric(df['runs'], errors='coerce')
    df = df.dropna(subset=['runs'])
    df['runs'] = df['runs'].astype(int)
    df = df.sort_values('runs', ascending=False).reset_index(drop=True)
    df['rank_pos'] = df.index + 1
    return df[['rank_pos', 'player', 'team', 'runs']]

def clean_bowling_stats(df):
    df = df.copy()
    # Remove bad rows (e.g. "Prince Yadav" in wickets col, "5 players" text)
    df['wickets'] = pd.to_numeric(df['wickets'], errors='coerce')
    df = df.dropna(subset=['wickets'])
    df['player'] = df['player'].str.strip()
    df['team'] = df['team'].str.strip()
    df['wickets'] = df['wickets'].astype(int)
    df = df.sort_values('wickets', ascending=False).reset_index(drop=True)
    df['rank_pos'] = df.index + 1
    return df[['rank_pos', 'player', 'team', 'wickets']]

def load_to_silver(cursor, conn, df, table, columns):
    cursor.execute(f"TRUNCATE TABLE {table}")
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        values = tuple(row[c] for c in columns)
        cursor.execute(sql, values)
    conn.commit()
    print(f"Loaded {len(df)} rows into {table}")

if __name__ == "__main__":
    conn = get_connection()
    if not conn:
        print("Could not connect to database.")
        exit(1)
    cursor = conn.cursor()
    create_silver_tables(cursor)

    # Load from bronze
    cursor.execute("SELECT pos, grp, team, pld, w, l, nr, pts, nrr, qualification FROM bronze.points_table")
    pts_df = pd.DataFrame(cursor.fetchall(), columns=['pos','grp','team','pld','w','l','nr','pts','nrr','qualification'])

    cursor.execute("SELECT runs, player, team FROM bronze.batting_stats")
    bat_df = pd.DataFrame(cursor.fetchall(), columns=['runs','player','team'])

    cursor.execute("SELECT wickets, player, team FROM bronze.bowling_stats")
    bowl_df = pd.DataFrame(cursor.fetchall(), columns=['wickets','player','team'])

    # Clean
    pts_clean   = clean_points_table(pts_df)
    bat_clean   = clean_batting_stats(bat_df)
    bowl_clean  = clean_bowling_stats(bowl_df)

    # Load to silver
    load_to_silver(cursor, conn, pts_clean, "silver.points_table",
        ['pos','grp','team','pld','w','l','nr','pts','nrr','qualification','win_pct'])
    load_to_silver(cursor, conn, bat_clean, "silver.batting_stats",
        ['rank_pos','player','team','runs'])
    load_to_silver(cursor, conn, bowl_clean, "silver.bowling_stats",
        ['rank_pos','player','team','wickets'])

    # Preview
    print("\n=== SILVER: POINTS TABLE ===")
    cursor.execute("SELECT pos, team, pts, nrr, qualification, win_pct FROM silver.points_table")
    for r in cursor.fetchall(): print(r)

    print("\n=== SILVER: BATTING STATS ===")
    cursor.execute("SELECT rank_pos, player, team, runs FROM silver.batting_stats")
    for r in cursor.fetchall(): print(r)

    print("\n=== SILVER: BOWLING STATS ===")
    cursor.execute("SELECT rank_pos, player, team, wickets FROM silver.bowling_stats")
    for r in cursor.fetchall(): print(r)

    cursor.close()
    conn.close()
    print("\nSilver layer cleaning complete.")
