from database_connection import get_connection
import pandas as pd

# ─────────────────────────────────────────────
# GOLD LAYER — Business-ready aggregated tables
#
# gold.team_performance   : full team scorecard with rank, form label, nrr tier
# gold.player_leaderboard : unified batting + bowling leaderboard with performance label
# gold.match_summary      : per-team win/loss breakdown for visualisation
# ─────────────────────────────────────────────

def create_gold_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.team_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pos INT, team VARCHAR(100), grp VARCHAR(5),
            pld INT, w INT, l INT, nr INT, pts INT,
            nrr FLOAT, win_pct FLOAT,
            qualification VARCHAR(100),
            nrr_tier VARCHAR(20),
            form_label VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.player_leaderboard (
            id INT AUTO_INCREMENT PRIMARY KEY,
            player VARCHAR(100), team VARCHAR(100),
            stat_type VARCHAR(20),
            stat_value INT, rank_pos INT,
            performance_label VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.match_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team VARCHAR(100),
            wins INT, losses INT, no_result INT,
            total_played INT, win_pct FLOAT,
            pts INT, qualification VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Gold tables created.")

def build_team_performance(pts_df):
    df = pts_df.copy()

    # NRR tier
    def nrr_tier(nrr):
        if nrr > 0.5:   return 'Strong'
        elif nrr > 0:   return 'Moderate'
        elif nrr > -0.5: return 'Weak'
        else:            return 'Poor'
    df['nrr_tier'] = df['nrr'].apply(nrr_tier)

    # Form label based on win %
    def form_label(win_pct):
        if win_pct >= 75:  return 'In Form'
        elif win_pct >= 50: return 'Average'
        else:               return 'Struggling'
    df['form_label'] = df['win_pct'].apply(form_label)

    return df[['pos','team','grp','pld','w','l','nr','pts','nrr','win_pct',
               'qualification','nrr_tier','form_label']]

def build_player_leaderboard(bat_df, bowl_df):
    bat = bat_df.copy()
    bat['stat_type'] = 'Batting'
    bat = bat.rename(columns={'runs': 'stat_value'})

    bowl = bowl_df.copy()
    bowl['stat_type'] = 'Bowling'
    bowl = bowl.rename(columns={'wickets': 'stat_value'})

    df = pd.concat([bat, bowl], ignore_index=True)

    def perf_label(row):
        if row['stat_type'] == 'Batting':
            if row['stat_value'] >= 230: return 'Elite'
            elif row['stat_value'] >= 210: return 'Good'
            else: return 'Average'
        else:
            if row['stat_value'] >= 10: return 'Elite'
            elif row['stat_value'] >= 8: return 'Good'
            else: return 'Average'
    df['performance_label'] = df.apply(perf_label, axis=1)

    return df[['player','team','stat_type','stat_value','rank_pos','performance_label']]

def build_match_summary(pts_df):
    df = pts_df[['team','w','l','nr','pld','win_pct','pts','qualification']].copy()
    df = df.rename(columns={'w':'wins','l':'losses','nr':'no_result','pld':'total_played'})
    return df

def load_to_gold(cursor, conn, df, table, columns):
    cursor.execute(f"TRUNCATE TABLE {table}")
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(row[c]) else row[c] for c in columns)
        cursor.execute(sql, values)
    conn.commit()
    print(f"Loaded {len(df)} rows into {table}")

if __name__ == "__main__":
    conn = get_connection()
    if not conn:
        print("Could not connect to database.")
        exit(1)
    cursor = conn.cursor()
    create_gold_tables(cursor)

    # Read from silver
    cursor.execute("SELECT pos,grp,team,pld,w,l,nr,pts,nrr,qualification,win_pct FROM silver.points_table")
    pts_df = pd.DataFrame(cursor.fetchall(),
        columns=['pos','grp','team','pld','w','l','nr','pts','nrr','qualification','win_pct'])

    cursor.execute("SELECT rank_pos, player, team, runs FROM silver.batting_stats")
    bat_df = pd.DataFrame(cursor.fetchall(), columns=['rank_pos','player','team','runs'])

    cursor.execute("SELECT rank_pos, player, team, wickets FROM silver.bowling_stats WHERE player != '5 players'")
    bowl_df = pd.DataFrame(cursor.fetchall(), columns=['rank_pos','player','team','wickets'])

    # Transform
    team_perf   = build_team_performance(pts_df)
    player_lb   = build_player_leaderboard(bat_df, bowl_df)
    match_sum   = build_match_summary(pts_df)

    # Load
    load_to_gold(cursor, conn, team_perf, "gold.team_performance",
        ['pos','team','grp','pld','w','l','nr','pts','nrr','win_pct','qualification','nrr_tier','form_label'])
    load_to_gold(cursor, conn, player_lb, "gold.player_leaderboard",
        ['player','team','stat_type','stat_value','rank_pos','performance_label'])
    load_to_gold(cursor, conn, match_sum, "gold.match_summary",
        ['team','wins','losses','no_result','total_played','win_pct','pts','qualification'])

    # Preview
    print("\n=== GOLD: TEAM PERFORMANCE ===")
    cursor.execute("SELECT pos, team, pts, win_pct, nrr_tier, form_label, qualification FROM gold.team_performance")
    for r in cursor.fetchall(): print(r)

    print("\n=== GOLD: PLAYER LEADERBOARD ===")
    cursor.execute("SELECT player, team, stat_type, stat_value, performance_label FROM gold.player_leaderboard")
    for r in cursor.fetchall(): print(r)

    print("\n=== GOLD: MATCH SUMMARY ===")
    cursor.execute("SELECT team, wins, losses, win_pct, qualification FROM gold.match_summary")
    for r in cursor.fetchall(): print(r)

    cursor.close()
    conn.close()
    print("\nGold layer transformation complete.")
