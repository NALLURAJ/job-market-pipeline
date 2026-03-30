WITH avg_by_level AS (
    SELECT 
        experience_level,
        ROUND(AVG(salary_avg), 2) AS level_avg_salary
    FROM {{ ref('stg_job_postings') }}
    WHERE salary_avg IS NOT NULL
    GROUP BY experience_level
)

SELECT
    jp.job_id,
    jp.title,
    jp.location,
    jp.experience_level,
    jp.salary_min,
    jp.salary_max,
    jp.salary_avg,
    a.level_avg_salary,
    CASE 
        WHEN jp.salary_avg IS NOT NULL THEN jp.salary_avg
        ELSE a.level_avg_salary
    END AS estimated_salary,
    CASE 
        WHEN jp.salary_avg IS NOT NULL THEN 'actual'
        ELSE 'estimated'
    END AS salary_source
FROM {{ ref('stg_job_postings') }} jp
LEFT JOIN avg_by_level a ON jp.experience_level = a.experience_level
