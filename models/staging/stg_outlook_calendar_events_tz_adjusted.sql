with base as (
    select *
    from {{ ref('stg_outlook_calendar_events_deduped') }}
)
select
    *,
    {{ convert_iso_string_to_timezone('start_datetime', 'America/Bogota') }} as start_colombia,
    {{ convert_iso_string_to_timezone('end_datetime',   'America/Bogota') }} as end_colombia
from base
