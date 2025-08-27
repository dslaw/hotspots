create view warehouse.incidents as
select
    bucket_timestamp,
    bucket_geohash,
    loaded_at,
    '311_case' as incident_type,
    toString(service_request_id) as unique_id
from warehouse.base_311_cases
union all
select
    bucket_timestamp,
    bucket_geohash,
    loaded_at,
    'fire_ems_call' as incident_type,
    toString(rowid) as unique_id
from warehouse.base_fire_ems_calls
union all
select
    bucket_timestamp,
    bucket_geohash,
    loaded_at,
    'fire_incident' as incident_type,
    toString(incident_number) as unique_id
from warehouse.base_fire_incidents
union all
select
    bucket_timestamp,
    bucket_geohash,
    loaded_at,
    'police_incident' as incident_type,
    toString(incident_number) as unique_id
from warehouse.base_police_incidents
union all
select
    bucket_timestamp,
    bucket_geohash,
    loaded_at,
    'traffic_crash' as incident_type,
    toString(unique_id) as unique_id
from warehouse.base_traffic_crashes;
