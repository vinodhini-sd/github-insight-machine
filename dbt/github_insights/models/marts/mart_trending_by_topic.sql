-- Trending repos enriched with topics, language, and description
-- Enables filtering by topic (e.g. 'mcp', 'ai-agents', 'llm')
-- and by language (e.g. 'Rust', 'Python', 'TypeScript')
WITH enriched_trending AS (
    SELECT
        t.repo_id,
        t.repo_name,
        t.owner,
        t.activity_date,
        t.stars,
        t.pushes,
        t.unique_pushers,
        t.forks,
        t.pull_requests,
        t.issues,
        t.activity_score,
        t.daily_rank,
        t.fork_to_star_ratio,
        t.pr_to_star_ratio,
        t.has_community_contributors,
        -- Enrichment fields
        m.description AS repo_description,
        m.language AS primary_language,
        m.topics AS topic_tags,
        m.license,
        m.stargazers_count AS total_stars,
        m.forks_count AS total_forks,
        m.owner_type,
        m.is_fork,
        m.created_at AS repo_created_at
    FROM {{ ref('mart_trending_repos') }} t
    LEFT JOIN {{ source('raw', 'repo_metadata') }} m
        ON t.repo_id = m.repo_id
)

SELECT
    *,
    -- Flatten topics into a searchable string for ILIKE filtering
    ARRAY_TO_STRING(COALESCE(topic_tags, ARRAY_CONSTRUCT()), ',') AS topics_flat,
    -- Combined text search field: topics + description + language
    LOWER(
        COALESCE(ARRAY_TO_STRING(topic_tags, ' '), '') || ' ' ||
        COALESCE(repo_description, '') || ' ' ||
        COALESCE(primary_language, '')
    ) AS search_text
FROM enriched_trending
WHERE activity_score > 0
