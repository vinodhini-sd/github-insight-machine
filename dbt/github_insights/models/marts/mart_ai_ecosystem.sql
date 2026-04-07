-- AI ecosystem tracker: trending repos in AI/LLM/MCP/agent/vector space
-- Chains from mart_trending_by_topic (which already has metadata enrichment)
-- Week-over-week comparison: score_wow_ratio surfaces what's accelerating right now
-- momentum: surging (≥3x WoW), growing (≥1.5x), new_to_radar (no prior week), stable
WITH ai_repos AS (
    SELECT *
    FROM {{ ref('mart_trending_by_topic') }}
    WHERE
        -- Topic tag matching (from GitHub REST API enrichment)
        topics_flat ILIKE ANY (
            '%llm%', '%ai-agent%', '%mcp%', '%rag%', '%langchain%',
            '%ollama%', '%vector-database%', '%embedding%', '%openai%',
            '%anthropic%', '%machine-learning%', '%deep-learning%',
            '%transformer%', '%diffusion%', '%fine-tuning%', '%inference%'
        )
        -- Python repos with AI keywords in description/topics (catches untagged AI projects)
        OR (
            primary_language = 'Python'
            AND search_text ILIKE ANY (
                '%language model%', '%ai agent%', '%mcp server%',
                '%retrieval augmented%', '%vector store%', '%llm%'
            )
        )
),

this_week AS (
    SELECT
        repo_id,
        repo_name,
        owner,
        primary_language,
        topic_tags,
        topics_flat,
        repo_description,
        total_stars,
        total_forks,
        SUM(stars)          AS stars_this_week,
        SUM(pushes)         AS pushes_this_week,
        SUM(forks)          AS forks_this_week,
        SUM(pull_requests)  AS prs_this_week,
        SUM(activity_score) AS score_this_week,
        -- Best (lowest) daily rank achieved this week
        MIN(daily_rank)     AS best_daily_rank
    FROM ai_repos
    WHERE activity_date >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

last_week AS (
    SELECT
        repo_id,
        SUM(stars)          AS stars_last_week,
        SUM(activity_score) AS score_last_week
    FROM ai_repos
    WHERE activity_date BETWEEN DATEADD('day', -14, CURRENT_DATE())
                             AND DATEADD('day', -8,  CURRENT_DATE())
    GROUP BY 1
)

SELECT
    t.repo_id,
    t.repo_name,
    t.owner,
    t.primary_language,
    t.topic_tags,
    t.repo_description,
    t.total_stars,
    t.total_forks,
    t.stars_this_week,
    t.pushes_this_week,
    t.forks_this_week,
    t.prs_this_week,
    t.score_this_week,
    t.best_daily_rank,
    COALESCE(l.stars_last_week, 0) AS stars_last_week,
    COALESCE(l.score_last_week, 0) AS score_last_week,
    -- Week-over-week deltas
    t.stars_this_week - COALESCE(l.stars_last_week, 0)         AS star_wow_delta,
    ROUND(t.score_this_week / NULLIF(l.score_last_week, 0), 2) AS score_wow_ratio,
    CASE
        WHEN t.score_this_week / NULLIF(l.score_last_week, 0) >= 3   THEN 'surging'
        WHEN t.score_this_week / NULLIF(l.score_last_week, 0) >= 1.5 THEN 'growing'
        WHEN l.score_last_week IS NULL                                THEN 'new_to_radar'
        ELSE 'stable'
    END AS momentum
FROM this_week t
LEFT JOIN last_week l ON t.repo_id = l.repo_id
ORDER BY t.score_this_week DESC
