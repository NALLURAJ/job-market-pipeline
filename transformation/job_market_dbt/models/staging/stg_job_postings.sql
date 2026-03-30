SELECT
    job_id,
    company_id,
    title,
    description,
    location,
    work_model,
    job_type,
    experience_level,
    CASE 
        WHEN salary_min IS NOT NULL AND salary_min < 500 
        THEN ROUND(salary_min * 2080, 2)
        ELSE salary_min 
    END AS salary_min,
    CASE 
        WHEN salary_max IS NOT NULL AND salary_max < 500 
        THEN ROUND(salary_max * 2080, 2)
        ELSE salary_max 
    END AS salary_max,
    salary_currency,
    source_platform,
    source_url,
    posted_date,
    scraped_date,
    is_active,
    created_at,
    CASE 
        WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL THEN
            ROUND((
                CASE WHEN salary_min < 500 THEN salary_min * 2080 ELSE salary_min END +
                CASE WHEN salary_max < 500 THEN salary_max * 2080 ELSE salary_max END
            ) / 2, 2)
        ELSE NULL 
    END AS salary_avg
FROM {{ source('raw', 'job_postings') }}
