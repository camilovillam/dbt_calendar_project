with flat as (
    select *
    from {{ ref('stg_outlook_calendar_events_flat') }}
),

ranked as (
    select
        flat.*,
        row_number() over (
            partition by flat.event_id
            order by flat.last_modified_datetime desc
        ) as rn
    from flat
)

select *
-- keep all columns from flat
from ranked
where rn = 1
