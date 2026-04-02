-- Daily activity metrics per repo: stars, pushes, forks, PRs, issues
-- This is the workhorse table for all downstream trending/signal models
WITH daily_stars AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS activity_date,
        COUNT(*) AS stars
    FROM {{ ref('stg_watch_events') }}
    GROUP BY 1, 2, 3
),

daily_pushes AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS activity_date,
        COUNT(*) AS pushes,
        COUNT(DISTINCT actor_id) AS unique_pushers
    FROM {{ ref('stg_push_events') }}
    GROUP BY 1, 2, 3
),

daily_forks AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS activity_date,
        COUNT(*) AS forks
    FROM {{ ref('stg_fork_events') }}
    GROUP BY 1, 2, 3
),

daily_prs AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS activity_date,
        COUNT(*) AS pull_requests
    FROM {{ ref('stg_pull_request_events') }}
    GROUP BY 1, 2, 3
),

daily_issues AS (
    SELECT
        repo_id,
        repo_name,
        TO_DATE(created_at) AS activity_date,
        COUNT(*) AS issues
    FROM {{ ref('stg_issue_events') }}
    GROUP BY 1, 2, 3
),

-- Get all unique repo+date combos from any event type
all_repo_dates AS (
    SELECT DISTINCT repo_id, repo_name, TO_DATE(created_at) AS activity_date
    FROM {{ source('raw', 'github_events') }}
)

SELECT
    d.repo_id,
    d.repo_name,
    d.activity_date,
    SPLIT_PART(d.repo_name, '/', 1) AS owner,
    COALESCE(s.stars, 0) AS stars,
    COALESCE(p.pushes, 0) AS pushes,
    COALESCE(p.unique_pushers, 0) AS unique_pushers,
    COALESCE(f.forks, 0) AS forks,
    COALESCE(pr.pull_requests, 0) AS pull_requests,
    COALESCE(i.issues, 0) AS issues,
    -- Composite activity score: weighted sum of different signals
    COALESCE(s.stars, 0) * 1.0
        + COALESCE(f.forks, 0) * 2.0
        + COALESCE(pr.pull_requests, 0) * 3.0
        + COALESCE(p.unique_pushers, 0) * 2.5
        + COALESCE(i.issues, 0) * 1.5 AS activity_score
FROM all_repo_dates d
LEFT JOIN daily_stars s ON d.repo_id = s.repo_id AND d.activity_date = s.activity_date
LEFT JOIN daily_pushes p ON d.repo_id = p.repo_id AND d.activity_date = p.activity_date
LEFT JOIN daily_forks f ON d.repo_id = f.repo_id AND d.activity_date = f.activity_date
LEFT JOIN daily_prs pr ON d.repo_id = pr.repo_id AND d.activity_date = pr.activity_date
LEFT JOIN daily_issues i ON d.repo_id = i.repo_id AND d.activity_date = i.activity_date
