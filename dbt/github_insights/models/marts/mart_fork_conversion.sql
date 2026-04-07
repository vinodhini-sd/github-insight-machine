-- Fork-to-PR conversion: repos where forks actually lead to PRs
-- High conversion = healthy community; low conversion = "star and forget"
WITH repo_forks AS (
    SELECT
        repo_id,
        repo_name,
        COUNT(*) AS fork_count,
        COUNT(DISTINCT actor_id) AS unique_forkers
    FROM {{ ref('stg_fork_events') }}
    -- 90-day window: community health should reflect recent activity
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),

repo_prs AS (
    SELECT
        repo_id,
        COUNT(*) AS pr_count,
        COUNT(DISTINCT actor_id) AS unique_pr_authors
    FROM {{ ref('stg_pull_request_events') }}
    -- Match the same 90-day window as forks
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),

repo_stars AS (
    SELECT
        repo_id,
        COUNT(*) AS star_count
    FROM {{ ref('stg_watch_events') }}
    GROUP BY 1
)

SELECT
    f.repo_id,
    f.repo_name,
    SPLIT_PART(f.repo_name, '/', 1) AS owner,
    f.fork_count,
    f.unique_forkers,
    COALESCE(p.pr_count, 0) AS pr_count,
    COALESCE(p.unique_pr_authors, 0) AS unique_pr_authors,
    COALESCE(s.star_count, 0) AS star_count,
    -- Fork-to-PR conversion rate
    CASE
        WHEN f.fork_count > 0
        THEN ROUND(COALESCE(p.pr_count, 0)::FLOAT / f.fork_count, 3)
        ELSE 0
    END AS fork_to_pr_conversion,
    -- Community health classification
    CASE
        WHEN COALESCE(p.pr_count, 0) > 0 AND f.fork_count > 0
            AND (COALESCE(p.pr_count, 0)::FLOAT / f.fork_count) >= 0.5
            THEN 'thriving'
        WHEN COALESCE(p.pr_count, 0) > 0
            THEN 'growing'
        WHEN f.fork_count >= 3 AND COALESCE(p.pr_count, 0) = 0
            THEN 'fork_graveyard'
        ELSE 'early_stage'
    END AS community_health
FROM repo_forks f
LEFT JOIN repo_prs p ON f.repo_id = p.repo_id
LEFT JOIN repo_stars s ON f.repo_id = s.repo_id
WHERE f.fork_count >= 2  -- at least 2 forks to be meaningful
ORDER BY fork_to_pr_conversion DESC, f.fork_count DESC
