select
    filename,
    filecontent,
    created_date,
    row_number() over (
        order by filename desc
    ) as file_row_num

from
    {{ source('outlook','calendar_events') }}
