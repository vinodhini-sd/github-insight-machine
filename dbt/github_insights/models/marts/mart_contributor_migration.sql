-- Contributor migration: tracks developers who contribute to multiple orgs
-- Signals cross-pollination and talent flow between ecosystems
WITH contributor_orgs AS (
    SELECT
        actor_id,
        actor_login,
        org_login,
        COUNT(*) AS events_in_org,
        COUNT(DISTINCT repo_id) AS repos_in_org,
        MIN(created_at) AS first_activity_in_org,
        MAX(created_at) AS last_activity_in_org
    FROM {{ source('raw', 'github_events') }}
    WHERE org_id IS NOT NULL
      AND event_type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent')
      AND actor_login NOT LIKE '%[bot]%'
    GROUP BY 1, 2, 3
),

multi_org_contributors AS (
    SELECT
        actor_id,
        actor_login,
        COUNT(DISTINCT org_login) AS org_count,
        ARRAY_AGG(DISTINCT org_login) AS orgs,
        SUM(events_in_org) AS total_cross_org_events
    FROM contributor_orgs
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT org_login) >= 2
)

SELECT
    m.actor_id,
    m.actor_login,
    m.org_count,
    m.orgs,
    m.total_cross_org_events,
    p.repos_contributed_to,
    p.contributor_type,
    p.active_days
FROM multi_org_contributors m
LEFT JOIN {{ ref('int_contributor_profiles') }} p
    ON m.actor_id = p.actor_id
ORDER BY m.org_count DESC, m.total_cross_org_events DESC
