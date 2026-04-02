-- Organization contribution patterns: which orgs are most active in OSS
SELECT
    org_id,
    org_login,
    COUNT(DISTINCT repo_id) AS repos_active,
    COUNT(DISTINCT actor_id) AS unique_contributors,
    COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' THEN actor_id END) AS unique_pushers,
    COUNT(DISTINCT CASE WHEN event_type = 'PullRequestEvent' THEN actor_id END) AS unique_pr_authors,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN event_type = 'PushEvent' THEN 1 END) AS push_events,
    COUNT(CASE WHEN event_type = 'PullRequestEvent' THEN 1 END) AS pr_events,
    COUNT(CASE WHEN event_type = 'ReleaseEvent' THEN 1 END) AS release_events,
    COUNT(CASE WHEN event_type = 'WatchEvent' THEN 1 END) AS star_events,
    MIN(created_at) AS first_activity,
    MAX(created_at) AS last_activity
FROM {{ source('raw', 'github_events') }}
WHERE org_id IS NOT NULL
  AND org_login IS NOT NULL
GROUP BY org_id, org_login
