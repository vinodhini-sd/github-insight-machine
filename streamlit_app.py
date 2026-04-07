"""
GitHub Insight Machine — Streamlit Dashboard
Visualizes all 10 dbt mart models from VINO_GITHUB_INSIGHTS.MARTS
"""

import os
import streamlit as st
from datetime import timedelta

st.set_page_config(
    page_title="GitHub Insight Machine",
    page_icon=":octocat:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Connection ─────────────────────────────────────────────────────────────────
# Uses PAT auth — inject via cortex secrets at launch:
#   SNOWFLAKE_ACCOUNT="<SNOWFLAKE_ACCOUNT>" SNOWFLAKE_USER="<SNOWFLAKE_USER>" \
#   SFDEVREL_PAT="<SFDEVREL_PAT>" python3 -m streamlit run streamlit_app.py

@st.cache_resource
def get_conn():
    return st.connection(
        "snowflake",
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        password=os.environ["SFDEVREL_PAT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="VINO_GITHUB_INSIGHTS",
        schema="MARTS",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

conn = get_conn()


# ── Query helpers ───────────────────────────────────────────────────────────────

TTL = timedelta(hours=1)

def query(sql: str, params: dict | None = None):
    """Run a cached query; returns a DataFrame."""
    return conn.query(sql, ttl=TTL, params=params)


def mart_exists(table: str) -> bool:
    """Check if a mart table exists — used for graceful fallback on new marts."""
    try:
        result = query(
            "SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'MARTS' AND TABLE_NAME = :name",
            params={"name": table.upper()},
        )
        return result["N"].iloc[0] > 0
    except Exception:
        return False


def missing_mart_msg(table: str, model: str):
    st.info(
        f"**{table}** hasn't been materialized yet.\n\n"
        f"Run inside the Astro container:\n"
        f"```\ndbt run --select {model}\n```",
        icon="ℹ️",
    )


# ── Sidebar ─────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("GitHub Insight Machine")
    st.caption("Powered by GH Archive + Snowflake + dbt")

    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**Data**")
    st.markdown("`VINO_GITHUB_INSIGHTS.MARTS`")
    st.markdown("**Refreshes:** hourly via Airflow")

    st.divider()
    st.markdown("**Marts**")
    marts = [
        "mart_trending_repos", "mart_trending_by_topic",
        "mart_viral_velocity", "mart_new_repo_breakout",
        "mart_ai_ecosystem", "mart_release_momentum",
        "mart_fork_conversion", "mart_org_scoreboard",
        "mart_ecosystem_hourly", "mart_contributor_migration",
    ]
    for m in marts:
        exists = mart_exists(m)
        icon = "🟢" if exists else "🔴"
        st.caption(f"{icon} {m}")


# ── Tabs ─────────────────────────────────────────────────────────────────────────

tabs = st.tabs([
    "Trending",
    "Viral Velocity",
    "New Breakouts",
    "AI Ecosystem",
    "Release Momentum",
    "Fork Health",
    "Org Scoreboard",
    "OSS Pulse",
])


# ────────────────────────────────────────────────────────────────────────────────
# Tab 1 — Trending
# ────────────────────────────────────────────────────────────────────────────────

with tabs[0]:
    st.header("Trending Repos")
    st.caption("Top repos by composite activity score (stars + pushes + forks + PRs + issues), last 30 days")

    df = query("""
        SELECT repo_name, owner, activity_date, stars, pushes, forks,
               pull_requests, issues, activity_score, daily_rank,
               primary_language, repo_description, topics_flat
        FROM VINO_GITHUB_INSIGHTS.MARTS.MART_TRENDING_BY_TOPIC
        ORDER BY activity_score DESC
    """)

    # Filters
    col_lang, col_search = st.columns([2, 3])
    with col_lang:
        langs = ["All"] + sorted(df["PRIMARY_LANGUAGE"].dropna().unique().tolist())
        lang_filter = st.selectbox("Language", langs)
    with col_search:
        search = st.text_input("Search repo name or description", placeholder="e.g. llm, agent, rust...")

    filtered = df.copy()
    if lang_filter != "All":
        filtered = filtered[filtered["PRIMARY_LANGUAGE"] == lang_filter]
    if search:
        mask = (
            filtered["REPO_NAME"].str.contains(search, case=False, na=False) |
            filtered["REPO_DESCRIPTION"].str.contains(search, case=False, na=False) |
            filtered["TOPICS_FLAT"].str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]

    # KPIs
    today = filtered.groupby("REPO_NAME")["ACTIVITY_SCORE"].sum().nlargest(1)
    with st.container(horizontal=True):
        st.metric("Repos tracked", f"{filtered['REPO_NAME'].nunique():,}", border=True)
        st.metric("Total events", f"{filtered['ACTIVITY_SCORE'].sum():,.0f}", border=True)
        st.metric("Unique owners", f"{filtered['OWNER'].nunique():,}", border=True)

    # Top 20 bar chart
    top20 = (
        filtered.groupby("REPO_NAME")["ACTIVITY_SCORE"]
        .sum()
        .nlargest(20)
        .reset_index()
        .rename(columns={"REPO_NAME": "repo", "ACTIVITY_SCORE": "score"})
    )
    with st.container(border=True):
        st.subheader("Top 20 by Total Activity Score")
        st.bar_chart(top20.set_index("repo")["score"])

    # Table
    with st.container(border=True):
        st.subheader("Full Table")
        display_cols = ["REPO_NAME", "PRIMARY_LANGUAGE", "ACTIVITY_SCORE", "STARS", "PUSHES", "FORKS", "PULL_REQUESTS", "REPO_DESCRIPTION"]
        st.dataframe(
            filtered[display_cols].drop_duplicates("REPO_NAME").sort_values("ACTIVITY_SCORE", ascending=False).head(200),
            use_container_width=True,
            hide_index=True,
        )


# ────────────────────────────────────────────────────────────────────────────────
# Tab 2 — Viral Velocity
# ────────────────────────────────────────────────────────────────────────────────

with tabs[1]:
    st.header("Viral Velocity")
    st.caption("Repos with sudden star acceleration vs their own 7-day baseline")

    if not mart_exists("MART_VIRAL_VELOCITY"):
        missing_mart_msg("MART_VIRAL_VELOCITY", "mart_viral_velocity")
    else:
        df = query("""
            SELECT repo_name, owner, stars_recent, avg_daily_stars_7d,
                   velocity_ratio, stars_above_baseline, velocity_class, no_prior_baseline
            FROM VINO_GITHUB_INSIGHTS.MARTS.MART_VIRAL_VELOCITY
            ORDER BY velocity_ratio DESC NULLS LAST
        """)

        # Class breakdown KPIs
        class_counts = df["VELOCITY_CLASS"].value_counts()
        with st.container(horizontal=True):
            for cls, color in [("explosive", "🔴"), ("viral", "🟠"), ("accelerating", "🟡"), ("steady", "🟢")]:
                n = int(class_counts.get(cls, 0))
                st.metric(f"{color} {cls.title()}", n, border=True)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("Top 20 by Velocity Ratio")
                top = df.head(20)[["REPO_NAME", "VELOCITY_RATIO", "STARS_RECENT", "VELOCITY_CLASS"]]
                st.dataframe(top, use_container_width=True, hide_index=True)
        with col2:
            with st.container(border=True):
                st.subheader("Stars Recent vs Baseline (top 50)")
                chart_df = df.head(50)[["REPO_NAME", "STARS_RECENT", "AVG_DAILY_STARS_7D"]].set_index("REPO_NAME")
                st.bar_chart(chart_df)

        with st.container(border=True):
            st.subheader("All Repos")
            st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 3 — New Breakouts
# ────────────────────────────────────────────────────────────────────────────────

with tabs[2]:
    st.header("New Repo Breakouts")
    st.caption("Repos < 30 days old with disproportionate early traction")

    if not mart_exists("MART_NEW_REPO_BREAKOUT"):
        missing_mart_msg("MART_NEW_REPO_BREAKOUT", "mart_new_repo_breakout")
    else:
        df = query("""
            SELECT repo_name, owner, first_seen_date, age_days, total_stars,
                   stars_per_day, activity_per_day, total_prs, total_forks,
                   breakout_tier
            FROM VINO_GITHUB_INSIGHTS.MARTS.MART_NEW_REPO_BREAKOUT
            ORDER BY total_stars DESC
        """)

        tier_counts = df["BREAKOUT_TIER"].value_counts()
        with st.container(horizontal=True):
            for tier, icon in [("instant_hit", "🚀"), ("fast_mover", "⚡"), ("rising", "📈"), ("early_stage", "🌱")]:
                n = int(tier_counts.get(tier, 0))
                st.metric(f"{icon} {tier.replace('_', ' ').title()}", n, border=True)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("Stars per Day (top 20)")
                chart = df.nlargest(20, "STARS_PER_DAY")[["REPO_NAME", "STARS_PER_DAY"]].set_index("REPO_NAME")
                st.bar_chart(chart)
        with col2:
            with st.container(border=True):
                st.subheader("Age vs Activity Score")
                scatter_df = df[["REPO_NAME", "AGE_DAYS", "ACTIVITY_PER_DAY", "BREAKOUT_TIER"]].copy()
                st.dataframe(scatter_df.sort_values("ACTIVITY_PER_DAY", ascending=False).head(50),
                             use_container_width=True, hide_index=True)

        with st.container(border=True):
            st.subheader("All Breakout Repos")
            st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 4 — AI Ecosystem
# ────────────────────────────────────────────────────────────────────────────────

with tabs[3]:
    st.header("AI Ecosystem")
    st.caption("Trending AI / LLM / MCP / agent repos with week-over-week momentum")

    if not mart_exists("MART_AI_ECOSYSTEM"):
        missing_mart_msg("MART_AI_ECOSYSTEM", "mart_ai_ecosystem")
    else:
        df = query("""
            SELECT repo_name, owner, primary_language, repo_description,
                   total_stars, stars_this_week, star_wow_delta,
                   score_this_week, score_wow_ratio, momentum
            FROM VINO_GITHUB_INSIGHTS.MARTS.MART_AI_ECOSYSTEM
            ORDER BY score_this_week DESC
        """)

        momentum_counts = df["MOMENTUM"].value_counts()
        with st.container(horizontal=True):
            for m, icon in [("surging", "🚀"), ("growing", "📈"), ("new_to_radar", "🆕"), ("stable", "➡️")]:
                n = int(momentum_counts.get(m, 0))
                st.metric(f"{icon} {m.replace('_', ' ').title()}", n, border=True)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("Top 20 AI Repos This Week")
                top = df.head(20)[["REPO_NAME", "SCORE_THIS_WEEK", "STARS_THIS_WEEK", "MOMENTUM", "PRIMARY_LANGUAGE"]]
                st.dataframe(top, use_container_width=True, hide_index=True)
        with col2:
            with st.container(border=True):
                st.subheader("Week-over-Week Score Ratio (top 20)")
                surging = df.nlargest(20, "SCORE_WOW_RATIO")[["REPO_NAME", "SCORE_WOW_RATIO"]].set_index("REPO_NAME")
                st.bar_chart(surging)

        with st.container(border=True):
            st.subheader("Full AI Ecosystem Leaderboard")
            st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 5 — Release Momentum
# ────────────────────────────────────────────────────────────────────────────────

with tabs[4]:
    st.header("Release Momentum")
    st.caption("Projects cutting releases with sustained PR/push activity (90-day window)")

    if not mart_exists("MART_RELEASE_MOMENTUM"):
        missing_mart_msg("MART_RELEASE_MOMENTUM", "mart_release_momentum")
    else:
        df = query("""
            SELECT repo_name, owner, release_count, releases_per_30_days,
                   pr_count, unique_pr_authors, push_count, star_count,
                   prs_per_release, project_stage
            FROM VINO_GITHUB_INSIGHTS.MARTS.MART_RELEASE_MOMENTUM
            ORDER BY release_count DESC
        """)

        stage_counts = df["PROJECT_STAGE"].value_counts()
        with st.container(horizontal=True):
            for stage, icon in [("active_oss", "🌟"), ("growing", "📈"), ("early_release", "🌱"), ("solo_project", "👤")]:
                n = int(stage_counts.get(stage, 0))
                st.metric(f"{icon} {stage.replace('_', ' ').title()}", n, border=True)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("Top 20 by Release Count")
                top = df.head(20)[["REPO_NAME", "RELEASE_COUNT", "RELEASES_PER_30_DAYS", "PRS_PER_RELEASE", "PROJECT_STAGE"]]
                st.dataframe(top, use_container_width=True, hide_index=True)
        with col2:
            with st.container(border=True):
                st.subheader("PRs per Release (top 20 active_oss)")
                active = df[df["PROJECT_STAGE"] == "active_oss"].nlargest(20, "PRS_PER_RELEASE")
                if not active.empty:
                    st.bar_chart(active[["REPO_NAME", "PRS_PER_RELEASE"]].set_index("REPO_NAME"))
                else:
                    st.caption("No active_oss projects yet.")

        with st.container(border=True):
            st.subheader("All Projects")
            st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 6 — Fork Health
# ────────────────────────────────────────────────────────────────────────────────

with tabs[5]:
    st.header("Fork Health")
    st.caption("Fork-to-PR conversion rate — does forking actually lead to contributions?")

    df = query("""
        SELECT repo_name, owner, fork_count, unique_forkers,
               pr_count, unique_pr_authors, star_count,
               fork_to_pr_conversion, community_health
        FROM VINO_GITHUB_INSIGHTS.MARTS.MART_FORK_CONVERSION
        ORDER BY fork_count DESC
    """)

    health_counts = df["COMMUNITY_HEALTH"].value_counts()
    with st.container(horizontal=True):
        for h, icon in [("thriving", "💚"), ("growing", "🌱"), ("fork_graveyard", "💀")]:
            n = int(health_counts.get(h, 0))
            st.metric(f"{icon} {h.replace('_', ' ').title()}", n, border=True)

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Top 20 by Fork Count")
            top = df.head(20)[["REPO_NAME", "FORK_COUNT", "PR_COUNT", "FORK_TO_PR_CONVERSION", "COMMUNITY_HEALTH"]]
            st.dataframe(top, use_container_width=True, hide_index=True)
    with col2:
        with st.container(border=True):
            st.subheader("Highest Fork-to-PR Conversion (min 10 forks)")
            high_conv = df[df["FORK_COUNT"] >= 10].nlargest(20, "FORK_TO_PR_CONVERSION")
            if not high_conv.empty:
                st.bar_chart(high_conv[["REPO_NAME", "FORK_TO_PR_CONVERSION"]].set_index("REPO_NAME"))

    with st.container(border=True):
        st.subheader("All Repos")
        st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 7 — Org Scoreboard
# ────────────────────────────────────────────────────────────────────────────────

with tabs[6]:
    st.header("Org Scoreboard")
    st.caption("Which orgs are driving the most OSS activity? (30-day window)")

    df = query("""
        SELECT org_login, org_tier, repos_active, unique_contributors,
               total_events, push_events, pr_events, release_events, star_events,
               push_ratio, pr_ratio, contributors_per_repo, activity_rank
        FROM VINO_GITHUB_INSIGHTS.MARTS.MART_ORG_SCOREBOARD
        ORDER BY activity_rank
    """)

    tier_counts = df["ORG_TIER"].value_counts()
    with st.container(horizontal=True):
        for tier in sorted(tier_counts.index.tolist()):
            st.metric(f"Tier {tier}", int(tier_counts[tier]), border=True)

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Top 20 Orgs by Total Events")
            top = df.head(20)[["ORG_LOGIN", "TOTAL_EVENTS", "UNIQUE_CONTRIBUTORS", "REPOS_ACTIVE", "ORG_TIER"]]
            st.dataframe(top, use_container_width=True, hide_index=True)
    with col2:
        with st.container(border=True):
            st.subheader("Contributors per Repo (top 20)")
            cpr = df.nlargest(20, "CONTRIBUTORS_PER_REPO")[["ORG_LOGIN", "CONTRIBUTORS_PER_REPO"]].set_index("ORG_LOGIN")
            st.bar_chart(cpr)

    with st.container(border=True):
        st.subheader("Full Org Scoreboard")
        st.dataframe(df, use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────────────────────
# Tab 8 — OSS Pulse
# ────────────────────────────────────────────────────────────────────────────────

with tabs[7]:
    st.header("OSS Pulse")
    st.caption("Macro GitHub activity by event type — last 7 days, hourly granularity")

    df = query("""
        SELECT activity_hour, event_type, event_count,
               unique_actors, unique_repos, events_per_actor
        FROM VINO_GITHUB_INSIGHTS.MARTS.MART_ECOSYSTEM_HOURLY
        ORDER BY activity_hour
    """)

    # Pivot for line chart
    import pandas as pd
    pivot = df.pivot_table(
        index="ACTIVITY_HOUR", columns="EVENT_TYPE", values="EVENT_COUNT", aggfunc="sum"
    ).fillna(0)

    with st.container(horizontal=True):
        st.metric("Total events (7d)", f"{df['EVENT_COUNT'].sum():,.0f}", border=True)
        st.metric("Unique actors", f"{df['UNIQUE_ACTORS'].max():,.0f}", border=True)
        st.metric("Unique repos", f"{df['UNIQUE_REPOS'].max():,.0f}", border=True)

    with st.container(border=True):
        st.subheader("Event Volume by Type (hourly)")
        st.line_chart(pivot)

    with st.container(border=True):
        st.subheader("Events per Actor Over Time")
        epa = df.pivot_table(
            index="ACTIVITY_HOUR", columns="EVENT_TYPE", values="EVENTS_PER_ACTOR", aggfunc="mean"
        ).fillna(0)
        st.line_chart(epa)
