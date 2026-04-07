-- Contributor profiles: how active is each developer across the ecosystem
SELECT
    actor_id,
    actor_login,
    COUNT(DISTINCT repo_id) AS repos_contributed_to,
    COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' THEN repo_id END) AS repos_pushed_to,
    COUNT(DISTINCT CASE WHEN event_type = 'PullRequestEvent' THEN repo_id END) AS repos_pr_to,
    COUNT(DISTINCT CASE WHEN event_type = 'IssuesEvent' THEN repo_id END) AS repos_issues_on,
    COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent' THEN repo_id END) AS repos_starred,
    COUNT(*) AS total_events,
    COUNT(DISTINCT TO_DATE(created_at)) AS active_days,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen,
    -- Contributor type classification
    CASE
        WHEN COUNT(DISTINCT CASE WHEN event_type IN ('PushEvent', 'PullRequestEvent') THEN repo_id END) >= 5
            THEN 'polyglot_contributor'
        WHEN COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent' THEN repo_id END) >= 10
            AND COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' THEN repo_id END) = 0
            THEN 'scout'
        WHEN COUNT(DISTINCT CASE WHEN event_type IN ('PushEvent', 'PullRequestEvent') THEN repo_id END) >= 2
            THEN 'active_contributor'
        ELSE 'casual'
    END AS contributor_type
-- NOTE: queries source directly (not staging) for performance —
-- a single-pass multi-CASE scan is more efficient than UNION ALL across 6 staging models.
FROM {{ source('raw', 'github_events') }}
WHERE actor_login NOT LIKE '%[bot]%'
  AND actor_login NOT LIKE '%-bot'
  -- 90-day window: contributor profiles should reflect current contributors
  AND created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY actor_id, actor_login
