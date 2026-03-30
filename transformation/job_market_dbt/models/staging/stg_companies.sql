select
company_id,
company_name,
industry,
company_size,
headquarters_location,
created_at
from {{ source('raw', 'companies') }}