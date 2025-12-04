select
    filename,
    filecontent,
    created_date
from
    {{ source('outlook','calendar_events') }}
