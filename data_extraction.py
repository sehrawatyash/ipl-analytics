import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://en.wikipedia.org/wiki/2026_Indian_Premier_League"

def get_soup(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def scrape_points_table(soup):
    tables = soup.find_all("table", class_="wikitable")
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Pts" in headers and "Team" in headers:
            rows = []
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if cells:
                    rows.append(cells[:len(headers)])
            df = pd.DataFrame(rows, columns=headers[:max(len(r) for r in rows)])
            return df
    return pd.DataFrame()

def scrape_batting_stats(soup):
    tables = soup.find_all("table", class_="wikitable")
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Runs" in headers and "Player" in headers:
            rows = []
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            df = pd.DataFrame(rows, columns=headers[:max(len(r) for r in rows)])
            return df
    return pd.DataFrame()

def scrape_bowling_stats(soup):
    tables = soup.find_all("table", class_="wikitable")
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Wickets" in headers and "Player" in headers:
            rows = []
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            df = pd.DataFrame(rows, columns=headers[:max(len(r) for r in rows)])
            return df
    return pd.DataFrame()

if __name__ == "__main__":
    print("Scraping IPL 2026 data from Wikipedia...")
    soup = get_soup(BASE_URL)

    points_df = scrape_points_table(soup)
    batting_df = scrape_batting_stats(soup)
    bowling_df = scrape_bowling_stats(soup)

    print("\n=== POINTS TABLE ===")
    print(points_df.to_string(index=False))

    print("\n=== TOP BATSMEN ===")
    print(batting_df.to_string(index=False))

    print("\n=== TOP BOWLERS ===")
    print(bowling_df.to_string(index=False))

    points_df.to_csv("points_table_raw.csv", index=False)
    batting_df.to_csv("batting_stats_raw.csv", index=False)
    bowling_df.to_csv("bowling_stats_raw.csv", index=False)

    print("\nSaved: points_table_raw.csv, batting_stats_raw.csv, bowling_stats_raw.csv")
