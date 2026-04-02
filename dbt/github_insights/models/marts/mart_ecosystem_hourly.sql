-- Ecosystem signals: aggregate activity across the whole GitHub ecosystem
-- Useful for spotting macro trends (is OSS accelerating? which event types growing?)
WITH hourly_activity AS (
    SELECT
        DATE_TRUNC('hour', created_at) AS activity_hour,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT actor_id) AS unique_actors,
        COUNT(DISTINCT repo_id) AS unique_repos
    FROM {{ source('raw', 'github_events') }}
    GROUP BY 1, 2
)

SELECT
    activity_hour,
    event_type,
    event_count,
    unique_actors,
    unique_repos,
    -- Events per actor (intensity metric)
    ROUND(event_count::FLOAT / NULLIF(unique_actors, 0), 2) AS events_per_actor,
    -- Repos per actor (breadth metric)
    ROUND(unique_repos::FLOAT / NULLIF(unique_actors, 0), 2) AS repos_per_actor
FROM hourly_activity
ORDER BY activity_hour DESC, event_count DESC
