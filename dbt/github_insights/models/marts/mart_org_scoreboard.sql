-- Organization OSS scoreboard: which orgs are investing most in open source
-- Useful for tracking corporate OSS bets and competitive intelligence
SELECT
    org_id,
    org_login,
    repos_active,
    unique_contributors,
    unique_pushers,
    unique_pr_authors,
    total_events,
    push_events,
    pr_events,
    release_events,
    star_events,
    first_activity,
    last_activity,
    -- Derived metrics
    ROUND(push_events::FLOAT / NULLIF(total_events, 0), 3) AS push_ratio,
    ROUND(pr_events::FLOAT / NULLIF(total_events, 0), 3) AS pr_ratio,
    ROUND(unique_contributors::FLOAT / NULLIF(repos_active, 0), 2) AS contributors_per_repo,
    -- Org size tier
    CASE
        WHEN unique_contributors >= 50 THEN 'enterprise'
        WHEN unique_contributors >= 10 THEN 'mid_size'
        WHEN unique_contributors >= 3 THEN 'startup'
        ELSE 'indie'
    END AS org_tier,
    -- Rank by total activity
    RANK() OVER (ORDER BY total_events DESC) AS activity_rank
FROM {{ ref('int_org_activity') }}
WHERE repos_active >= 2  -- filter out single-repo orgs
ORDER BY total_events DESC
