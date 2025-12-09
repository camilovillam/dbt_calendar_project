select
    *
from
    {{ ref('stg_outlook_calendar_events_tz_adjusted') }}
where
    event_type <> 'seriesMaster'
