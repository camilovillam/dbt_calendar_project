select *
from {{ ref('fct_calendar_events') }}
where datediff('minute', start_colombia, end_colombia) < 0
