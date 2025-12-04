select
    row_number() over (
        order by filename desc
    ) as file_row_num,
    filename,
    filecontent,
    created_date

from
    {{ source('outlook','calendar_events') }}
