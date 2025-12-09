select 
    *
from 
    {{ ref('stg_outlook_calendar_events_tz_adjusted') }}
WHERE event_type = 'seriesMaster'

