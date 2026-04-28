import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
import numpy as np

st.set_page_config(page_title="IPL 2026 Analytics", layout="wide")
st.title("IPL 2026 — Data Analytics Dashboard")

@st.cache_data
def load_data():
    teams = pd.read_csv("team_performance.csv")
    players = pd.read_csv("player_leaderboard.csv")
    return teams, players

teams, players = load_data()
batters = players[players['stat_type'] == 'Batting'].reset_index(drop=True)
bowlers = players[players['stat_type'] == 'Bowling'].reset_index(drop=True)

st.header("1. Data Tables")
c1, c2 = st.columns(2)

with c1:
    st.subheader("Points Table")
    st.dataframe(teams[['pos','team','pld','w','l','pts','nrr','win_pct','qualification']]
                 .rename(columns={'pos':'Pos','team':'Team','pld':'P','w':'W','l':'L',
                                  'pts':'Pts','nrr':'NRR','win_pct':'Win%','qualification':'Status'}),
                 use_container_width=True, hide_index=True)

with c2:
    st.subheader("Player Leaderboard")
    st.dataframe(players[['player','team','stat_type','stat_value','performance_label']]
                 .rename(columns={'player':'Player','team':'Team','stat_type':'Type',
                                  'stat_value':'Value','performance_label':'Label'}),
                 use_container_width=True, hide_index=True)

st.header("2. Visualisations")
c3, c4, c5 = st.columns(3)

with c3:
    st.subheader("Wins by Team")
    fig, ax = plt.subplots(figsize=(5, 4))
    colors = ['#2ecc71' if q == 'Qualifier 1' else '#f39c12' if q == 'Eliminator' else '#e74c3c'
              for q in teams['qualification']]
    ax.barh(teams['team'], teams['w'], color=colors)
    ax.set_xlabel("Wins")
    ax.invert_yaxis()
    ax.set_title("Wins (Green=Q1, Orange=Elim, Red=Out)")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

with c4:
    st.subheader("Top Run Scorers")
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(batters['player'], batters['stat_value'], color='#3498db')
    ax.set_ylabel("Runs")
    ax.set_title("Top Batsmen — IPL 2026")
    plt.xticks(rotation=25, ha='right', fontsize=8)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

with c5:
    st.subheader("NRR vs Win %")
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.scatter(teams['nrr'], teams['win_pct'], s=100, color='#9b59b6')
    for _, row in teams.iterrows():
        ax.annotate(row['team'].split()[-1], (row['nrr'], row['win_pct']),
                    fontsize=7, ha='center', va='bottom')
    ax.set_xlabel("NRR")
    ax.set_ylabel("Win %")
    ax.set_title("NRR vs Win%")
    ax.axvline(0, color='grey', linestyle='--', linewidth=0.8)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

st.header("3. Predictions & Insights")
p1, p2, p3 = st.columns(3)

with p1:
    st.subheader("Predict: Points from Wins")
    X = teams[['w']].values
    y = teams['pts'].values
    model = LinearRegression().fit(X, y)
    wins_input = st.slider("Wins", 0, 7, 3)
    predicted_pts = round(float(model.predict([[wins_input]])[0]), 1)
    st.metric("Predicted Points", predicted_pts)
    fig, ax = plt.subplots(figsize=(4, 3))
    ax.scatter(teams['w'], teams['pts'], color='#e67e22', s=60, label='Actual')
    xs = np.linspace(0, 7, 50)
    ax.plot(xs, model.predict(xs.reshape(-1,1)), color='black', linewidth=1.5, label='Trend')
    ax.axvline(wins_input, color='blue', linestyle='--', linewidth=1)
    ax.scatter([wins_input], [predicted_pts], color='blue', s=80, zorder=5)
    ax.set_xlabel("Wins"); ax.set_ylabel("Points")
    ax.legend(fontsize=7)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

with p2:
    st.subheader("Predict: Top Scorer Next Match")
    st.markdown("Based on current run tally, projected next-match contribution (10% of total):")
    batters['projected'] = (batters['stat_value'] * 0.10).round(1)
    fig, ax = plt.subplots(figsize=(4, 3))
    ax.barh(batters['player'], batters['projected'], color='#1abc9c')
    ax.set_xlabel("Projected Runs")
    ax.invert_yaxis()
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()
    st.dataframe(batters[['player','stat_value','projected']]
                 .rename(columns={'player':'Player','stat_value':'Total Runs','projected':'Projected'}),
                 hide_index=True, use_container_width=True)

with p3:
    st.subheader("Cluster: Team Types (KMeans)")
    features = teams[['win_pct','nrr','pts']].copy()
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10).fit(features)
    teams['cluster'] = kmeans.labels_
    cluster_map = {i: f"Group {chr(65+i)}" for i in range(3)}
    teams['cluster_label'] = teams['cluster'].map(cluster_map)
    colors_k = ['#e74c3c','#3498db','#2ecc71']
    fig, ax = plt.subplots(figsize=(4, 3))
    for i in range(3):
        sub = teams[teams['cluster'] == i]
        ax.scatter(sub['win_pct'], sub['pts'], label=cluster_map[i],
                   color=colors_k[i], s=80)
        for _, row in sub.iterrows():
            ax.annotate(row['team'].split()[-1], (row['win_pct'], row['pts']),
                        fontsize=6, ha='center', va='bottom')
    ax.set_xlabel("Win %"); ax.set_ylabel("Points")
    ax.legend(fontsize=7)
    ax.set_title("Team Clusters")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()
    st.dataframe(teams[['team','cluster_label']]
                 .rename(columns={'team':'Team','cluster_label':'Cluster'}),
                 hide_index=True, use_container_width=True)

st.caption("Data Source: Wikipedia | Pipeline: Bronze → Silver → Gold | Built with Streamlit")