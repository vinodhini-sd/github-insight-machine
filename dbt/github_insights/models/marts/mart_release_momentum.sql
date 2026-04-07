-- Release momentum: repos cutting releases with sustained PR/push activity
-- "Going from experimental to real" signal — uses release + PR + push co-occurrence
-- project_stage: active_oss / growing / early_release / solo_project
-- prs_per_release = external contribution rate (high = healthy community)
-- 90-day window on all event types
WITH repo_releases AS (
    SELECT
        repo_id,
        repo_name,
        COUNT(*)                 AS release_count,
        MIN(created_at)          AS first_release_at,
        MAX(created_at)          AS latest_release_at,
        COUNT(DISTINCT actor_id) AS unique_releasers
    FROM {{ ref('stg_release_events') }}
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),

repo_prs AS (
    SELECT
        repo_id,
        COUNT(*)                 AS pr_count,
        COUNT(DISTINCT actor_id) AS unique_pr_authors
    FROM {{ ref('stg_pull_request_events') }}
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),

repo_pushes AS (
    SELECT
        repo_id,
        COUNT(*)                 AS push_count,
        COUNT(DISTINCT actor_id) AS unique_pushers
    FROM {{ ref('stg_push_events') }}
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),

repo_stars AS (
    SELECT
        repo_id,
        COUNT(*) AS star_count
    FROM {{ ref('stg_watch_events') }}
    WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
)

SELECT
    r.repo_id,
    r.repo_name,
    SPLIT_PART(r.repo_name, '/', 1)                                                     AS owner,
    r.release_count,
    r.first_release_at,
    r.latest_release_at,
    r.unique_releasers,
    DATEDIFF('day', r.first_release_at, r.latest_release_at)                            AS release_span_days,
    COALESCE(pr.pr_count, 0)          AS pr_count,
    COALESCE(pr.unique_pr_authors, 0) AS unique_pr_authors,
    COALESCE(p.push_count, 0)         AS push_count,
    COALESCE(p.unique_pushers, 0)     AS unique_pushers,
    COALESCE(s.star_count, 0)         AS star_count,
    -- Release cadence: extrapolated to per-30-days
    ROUND(r.release_count * 30.0 / 90, 2)                                               AS releases_per_30_days,
    -- External contribution rate: PRs per release
    CASE WHEN r.release_count > 0
         THEN ROUND(COALESCE(pr.pr_count, 0)::FLOAT / r.release_count, 2)
         ELSE 0
    END AS prs_per_release,
    CASE
        WHEN r.release_count >= 5 AND COALESCE(pr.unique_pr_authors, 0) >= 3 THEN 'active_oss'
        WHEN r.release_count >= 3 AND COALESCE(pr.pr_count, 0) > 0           THEN 'growing'
        WHEN r.release_count >= 1 AND COALESCE(p.push_count, 0) > 0          THEN 'early_release'
        ELSE 'solo_project'
    END AS project_stage
FROM repo_releases r
LEFT JOIN repo_prs    pr ON r.repo_id = pr.repo_id
LEFT JOIN repo_pushes p  ON r.repo_id = p.repo_id
LEFT JOIN repo_stars  s  ON r.repo_id = s.repo_id
WHERE COALESCE(p.push_count, 0) > 0  -- must have code activity, not just a release tag
ORDER BY r.release_count DESC, prs_per_release DESC
