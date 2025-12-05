with valid_files as (
    select
        file_row_num,
        filename,
        created_date,
        json_extract_string(filecontent, '$."@odata.context"') as context,
        json_extract(filecontent, '$.value') as json_file_value,
        json_extract_string(filecontent,'$."@odata.nextLink"') as nextLink,
        json_extract_string(filecontent,'$."@odata.deltaLink"') as deltaLink
    from
        {{ ref('stg_outlook_calendar_events_raw') }}
    where json_file_value <> '[]'
)

select *
from
    valid_files
