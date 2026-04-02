-- Stars (WatchEvent in GH Archive = someone starred a repo)
SELECT
    event_id,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_id,
    org_login,
    created_at,
    source_file
FROM {{ source('raw', 'github_events') }}
WHERE event_type = 'WatchEvent'
