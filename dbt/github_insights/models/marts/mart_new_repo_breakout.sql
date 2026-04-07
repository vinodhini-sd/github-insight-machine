-- New repo breakout: recently created repos (< 30 days) with high early traction
-- "Find the next hot thing before it's obvious" — earliest signal of emerging tools
-- Uses stg_create_events: MIN(created_at) per repo as proxy for creation date
-- breakout_tier: instant_hit / fast_mover / rising / early_stage
WITH new_repos AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(MIN(created_at)) AS first_seen_date
    FROM {{ ref('stg_create_events') }}
    GROUP BY 1, 2
    HAVING TO_DATE(MIN(created_at)) >= DATEADD('day', -30, CURRENT_DATE())
),

repo_activity AS (
    SELECT
        repo_id,
        SUM(stars)          AS total_stars,
        SUM(pushes)         AS total_pushes,
        SUM(forks)          AS total_forks,
        SUM(pull_requests)  AS total_prs,
        SUM(issues)         AS total_issues,
        SUM(activity_score) AS total_activity_score,
        MAX(activity_score) AS peak_daily_score,
        COUNT(*)            AS active_days
    FROM {{ ref('int_repo_daily_activity') }}
    GROUP BY 1
)

SELECT
    n.repo_id,
    n.repo_name,
    SPLIT_PART(n.repo_name, '/', 1)                                                            AS owner,
    n.first_seen_date,
    DATEDIFF('day', n.first_seen_date, CURRENT_DATE())                                         AS age_days,
    a.total_stars,
    a.total_pushes,
    a.total_forks,
    a.total_prs,
    a.total_issues,
    a.total_activity_score,
    a.peak_daily_score,
    a.active_days,
    ROUND(a.total_stars / NULLIF(DATEDIFF('day', n.first_seen_date, CURRENT_DATE()), 0), 2)    AS stars_per_day,
    ROUND(a.total_activity_score / NULLIF(DATEDIFF('day', n.first_seen_date, CURRENT_DATE()), 0), 2) AS activity_per_day,
    CASE
        WHEN a.total_stars >= 500 AND DATEDIFF('day', n.first_seen_date, CURRENT_DATE()) <= 7  THEN 'instant_hit'
        WHEN a.total_stars >= 200 AND DATEDIFF('day', n.first_seen_date, CURRENT_DATE()) <= 14 THEN 'fast_mover'
        WHEN a.total_stars >= 50  AND DATEDIFF('day', n.first_seen_date, CURRENT_DATE()) <= 30 THEN 'rising'
        ELSE 'early_stage'
    END AS breakout_tier
FROM new_repos n
INNER JOIN repo_activity a ON n.repo_id = a.repo_id
WHERE a.total_activity_score > 0
  AND SPLIT_PART(n.repo_name, '/', 1) NOT IN ('github', 'dependabot')
ORDER BY a.total_activity_score DESC
