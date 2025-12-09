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
select
    vf.file_row_num,
    vf.filename,
    je.key as event_index,
    vf.file_row_num || '-' || je.key as event_sk,  -- surrogate key
    -- Core identifiers
    json_extract_string(je.value, '$.id')          as event_id,
    json_extract_string(je.value, '$.uid')         as uid,
    json_extract_string(je.value, '$.iCalUId')     as ical_uid,
    json_extract_string(je.value, '$.@odata.etag') as etag,
    -- Timestamps
    json_extract_string(je.value, '$.createdDateTime')     as created_datetime,
    json_extract_string(je.value, '$.lastModifiedDateTime') as last_modified_datetime,
    -- Event details
    json_extract_string(je.value, '$.subject')     as subject,
    json_extract_string(je.value, '$.bodyPreview') as body_preview,
    json_extract(je.value, '$.body')               as body_json,        -- full object
    json_extract_string(je.value, '$.importance')  as importance,
    json_extract_string(je.value, '$.sensitivity') as sensitivity,
    json_extract_string(je.value, '$.showAs')      as show_as,
    json_extract_string(je.value, '$.type')        as event_type,
    -- Organizer
    json_extract(je.value, '$.organizer')          as organizer_json,   -- full object
    -- Timing
    json_extract(je.value, '$.start')              as start_json,       -- full object
    json_extract(je.value, '$.end')                as end_json,         -- full object
    -- Location
    json_extract(je.value, '$.location')           as location_json,    -- full object
    json_extract(je.value, '$.locations')          as locations_json,   -- array
    -- Attendees
    json_extract(je.value, '$.attendees')          as attendees_json,   -- array
    -- Other metadata
    json_extract_string(je.value, '$.webLink')     as web_link,
    json_extract_string(je.value, '$.onlineMeetingUrl') as online_meeting_url,
    json_extract_string(je.value, '$.onlineMeetingProvider') as online_meeting_provider,
    json_extract_string(je.value, '$.isOnlineMeeting') as is_online_meeting,
    json_extract_string(je.value, '$.isAllDay')    as is_all_day,
    json_extract_string(je.value, '$.isCancelled') as is_cancelled,
    json_extract_string(je.value, '$.isDraft')     as is_draft,
    json_extract_string(je.value, '$.isReminderOn') as is_reminder_on,
    json_extract_string(je.value, '$.reminderMinutesBeforeStart') as reminder_minutes_before_start,
    json_extract(je.value, '$.responseStatus')     as response_status_json,
    json_extract(je.value, '$.categories')         as categories_json   -- array
from valid_files vf,
     json_each(vf.json_file_value) as je
order by 4 asc
