with base as (
    select
        event_sk,
        attendees_json
    from
        {{ ref('stg_outlook_calendar_events_deduped') }}
    where attendees_json is not null
)

select
    base.event_sk,
    att.key as attendee_index,
    -- surrogate key: event + attendee index
    base.event_sk || '-' || att.key as attendee_sk,

    -- Attendee details
    json_extract_string(att.value, '$.emailAddress.name') as attendee_name,
    json_extract_string(att.value, '$.emailAddress.address') as attendee_email,
    split_part(
        json_extract_string(att.value, '$.emailAddress.address'), '@', 2
    ) as attendee_org,
    -- required, optional, resource
    json_extract_string(att.value, '$.type') as attendee_type,
    -- accepted, declined, tentative
    json_extract_string(att.value, '$.status.response') as response_status,
    json_extract_string(att.value, '$.status.time') as response_time

from base,
    json_each(base.attendees_json) as att
order by base.event_sk, attendee_index
