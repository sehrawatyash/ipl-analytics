# Data Cleaning Notes — Silver Layer

## What changed from Bronze → Silver

| Table | Issue Found | Fix Applied |
|-------|-------------|-------------|
| Points | All columns stored as `VARCHAR` | Cast `pos, pld, w, l, nr, pts` → `INT`; `nrr` → `FLOAT` |
| Points | Unicode minus sign `−` in NRR (e.g. `−0.804`) | Replaced `\u2212` with `-` before casting |
| Points | Broken qualification text (`"Advance to thequalifier 1"`) | Parsed and mapped to clean labels: `Qualifier 1`, `Eliminator`, `Eliminated` |
| Points | Empty qualification for mid-table teams | Filled with `"Eliminated"` |
| Points | No performance metric | Added `win_pct = (W / Pld) × 100` |
| Bowling | `"Prince Yadav"` appeared in the wickets column (Wikipedia table bug) | Dropped all non-numeric wickets rows |
| Bowling | `"5 players"` merged row from Wikipedia | Dropped — not a real player record |
| Batting/Bowling | Whitespace in player/team names | Stripped with `.str.strip()` |
| Batting/Bowling | No ranking column | Added `rank_pos` (1 = best) |

## Silver Table Schemas

### silver.points_table
| Column | Type | Notes |
|--------|------|-------|
| pos | INT | League position |
| grp | VARCHAR(5) | Group A or B |
| team | VARCHAR(100) | Team name |
| pld | INT | Matches played |
| w | INT | Wins |
| l | INT | Losses |
| nr | INT | No result |
| pts | INT | Points |
| nrr | FLOAT | Net run rate |
| qualification | VARCHAR(100) | Qualifier 1 / Eliminator / Eliminated |
| win_pct | FLOAT | Win % = W/Pld × 100 |

### silver.batting_stats
| Column | Type | Notes |
|--------|------|-------|
| rank_pos | INT | Rank by runs |
| player | VARCHAR(100) | Player name |
| team | VARCHAR(100) | Team name |
| runs | INT | Total runs scored |

### silver.bowling_stats
| Column | Type | Notes |
|--------|------|-------|
| rank_pos | INT | Rank by wickets |
| player | VARCHAR(100) | Player name |
| team | VARCHAR(100) | Team name |
| wickets | INT | Total wickets taken |
