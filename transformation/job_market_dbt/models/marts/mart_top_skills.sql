SELECT
    js.skill_name,
    js.skill_type,
    COUNT(*) AS demand_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT job_id) FROM {{ ref('stg_job_postings') }}), 1) AS pct_of_jobs
FROM {{ source('raw', 'job_skills') }} js
GROUP BY js.skill_name, js.skill_type
ORDER BY demand_count DESC
