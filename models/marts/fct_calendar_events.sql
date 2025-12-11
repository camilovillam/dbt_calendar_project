with occ as (
    select *
    from {{ ref('stg_outlook_calendar_events_tz_adjusted') }}
    where event_type in ('occurrence','exception')
),
single as (
    select *
    from {{ ref('stg_outlook_calendar_events_tz_adjusted') }}
    where event_type = 'singleInstance'
),
master as (
    select *
    from {{ ref('dim_meeting_series') }}
)

-- Single-instance meetings: already complete
select
    md5(concat(s.event_id, '-', s.event_type)) as event_sk,
    s.event_id,
    s.series_master_id,
    s.subject,
    s.body_preview,
    s.organizer_name,
    s.organizer_email,
    s.importance,
    s.sensitivity,
    s.show_as,
    s.start_datetime,
    s.end_datetime,
    s.start_colombia,
    s.end_colombia,
    datediff('minute', s.start_colombia, s.end_colombia) as duration_minutes,
    s.is_online_meeting,
    s.is_all_day,
    s.is_cancelled,
    s.is_draft,
    s.event_response,
    s.event_response_time,
    s.event_type
from single s

union all

-- Occurrences and exceptions: enriched with master metadata
select
    md5(concat(o.event_id, '-', o.event_type)) as event_sk,
    o.event_id,
    o.series_master_id,
    coalesce(o.subject, m.subject) as subject,
    coalesce(o.body_preview, m.body_preview) as body_preview,
    coalesce(o.organizer_name, m.organizer_name) as organizer_name,
    coalesce(o.organizer_email, m.organizer_email) as organizer_email,
    coalesce(o.importance, m.importance) as importance,
    coalesce(o.sensitivity, m.sensitivity) as sensitivity,
    coalesce(o.show_as, m.show_as) as show_as,
    o.start_datetime,
    o.end_datetime,
    o.start_colombia,
    o.end_colombia,
    datediff('minute', o.start_colombia, o.end_colombia) as duration_minutes,
    o.is_online_meeting,
    o.is_all_day,
    o.is_cancelled,
    o.is_draft,
    o.event_response,
    o.event_response_time,
    o.event_type
from occ o
left join master m
    on o.series_master_id = m.event_id

