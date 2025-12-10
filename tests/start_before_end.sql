select *
from {{ ref('fct_calendar_events') }}
where start_colombia > end_colombia
