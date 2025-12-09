with base as (
    select
        event_sk,
        attendees_json
    from 
        {{ ref('stg_outlook_calendar_events_flat') }}
    where attendees_json is not null
)
select
    event_sk,
    att.key as attendee_index,
    -- surrogate key: event + attendee index
    event_sk || '-' || att.key as attendee_sk,

    -- Attendee details
    json_extract_string(att.value, '$.emailAddress.name')    as attendee_name,
    json_extract_string(att.value, '$.emailAddress.address') as attendee_email,
    split_part(json_extract_string(att.value, '$.emailAddress.address'), '@', 2) as attendee_org,
    json_extract_string(att.value, '$.type')                 as attendee_type,   -- required, optional, resource
    json_extract_string(att.value, '$.status.response')      as response_status, -- accepted, declined, tentative
    json_extract_string(att.value, '$.status.time')          as response_time

from base,
     json_each(base.attendees_json) as att
order by event_sk, attendee_index
