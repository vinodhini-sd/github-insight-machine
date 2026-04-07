-- Viral velocity: repos with sudden star acceleration vs their own 7-day baseline
-- "What blew up this week" signal — velocity_ratio compares yesterday vs prior 7-day avg
-- velocity_class: explosive (≥10x), viral (≥5x), accelerating (≥3x), steady
-- no_prior_baseline = TRUE means truly new repos spiking with no history
WITH daily_stars AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS star_date,
        COUNT(*)            AS daily_stars
    FROM {{ ref('stg_watch_events') }}
    WHERE created_at >= DATEADD('day', -15, CURRENT_DATE())
    GROUP BY 1, 2, 3
),

repo_baseline AS (
    -- Prior 7 days, excluding the most recent complete day (to isolate the spike)
    SELECT
        repo_id,
        ROUND(AVG(daily_stars), 2) AS avg_daily_stars_7d,
        SUM(daily_stars)           AS total_stars_7d
    FROM daily_stars
    WHERE star_date BETWEEN DATEADD('day', -8, CURRENT_DATE())
                        AND DATEADD('day', -2, CURRENT_DATE())
    GROUP BY 1
),

most_recent_day AS (
    -- Most recent complete day of star data (yesterday)
    SELECT
        repo_id,
        repo_name,
        daily_stars AS stars_recent
    FROM daily_stars
    WHERE star_date = DATEADD('day', -1, CURRENT_DATE())
)

SELECT
    r.repo_id,
    r.repo_name,
    SPLIT_PART(r.repo_name, '/', 1)                               AS owner,
    r.stars_recent,
    COALESCE(b.avg_daily_stars_7d, 0)                             AS avg_daily_stars_7d,
    COALESCE(b.total_stars_7d, 0)                                 AS total_stars_7d,
    ROUND(r.stars_recent / NULLIF(b.avg_daily_stars_7d, 0), 2)   AS velocity_ratio,
    ROUND(r.stars_recent - COALESCE(b.avg_daily_stars_7d, 0), 0) AS stars_above_baseline,
    CASE
        WHEN r.stars_recent / NULLIF(b.avg_daily_stars_7d, 0) >= 10 THEN 'explosive'
        WHEN r.stars_recent / NULLIF(b.avg_daily_stars_7d, 0) >= 5  THEN 'viral'
        WHEN r.stars_recent / NULLIF(b.avg_daily_stars_7d, 0) >= 3  THEN 'accelerating'
        ELSE 'steady'
    END AS velocity_class,
    -- Flag repos with no prior star history — truly new + spiking
    CASE WHEN b.avg_daily_stars_7d IS NULL THEN TRUE ELSE FALSE END AS no_prior_baseline
FROM most_recent_day r
LEFT JOIN repo_baseline b ON r.repo_id = b.repo_id
WHERE r.stars_recent >= 5  -- minimum threshold to filter noise
ORDER BY velocity_ratio DESC NULLS LAST
