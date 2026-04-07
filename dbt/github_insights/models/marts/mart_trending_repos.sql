-- Trending repos: repos with highest composite activity scores
-- Filters out bots and low-signal repos
SELECT
    repo_id,
    repo_name,
    owner,
    activity_date,
    stars,
    pushes,
    unique_pushers,
    forks,
    pull_requests,
    issues,
    activity_score,
    -- Rank by activity score per day
    RANK() OVER (PARTITION BY activity_date ORDER BY activity_score DESC) AS daily_rank,
    -- Star-to-fork ratio (higher = more "look but don't build" — lower is better signal)
    CASE WHEN stars > 0 THEN ROUND(forks::FLOAT / stars, 3) ELSE 0 END AS fork_to_star_ratio,
    -- PR engagement: are people submitting PRs, not just starring?
    CASE WHEN stars > 0 THEN ROUND(pull_requests::FLOAT / stars, 3) ELSE 0 END AS pr_to_star_ratio,
    -- Multi-contributor signal: more than 1 pusher = real community
    CASE WHEN unique_pushers > 1 THEN TRUE ELSE FALSE END AS has_community_contributors
FROM {{ ref('int_repo_daily_activity') }}
WHERE activity_score > 0
  -- Only last 30 days — prevents full table scans as history grows
  AND activity_date >= DATEADD('day', -30, CURRENT_DATE())
  -- Filter out GitHub internal repos and known bot-heavy repos
  AND owner NOT IN ('github', 'dependabot')
  AND repo_name NOT LIKE '%/.github%'
ORDER BY activity_date DESC, activity_score DESC
