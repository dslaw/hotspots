create view warehouse.base_311_cases as
select st.*
from warehouse.stg_311_cases as st
order by service_request_id, loaded_at asc
limit 1 by service_request_id;

create view warehouse.base_fire_ems_calls as
select st.*
from warehouse.stg_fire_ems_calls as st
order by rowid, loaded_at asc
limit 1 by rowid;

create view warehouse.base_fire_incidents as
select st.*
from warehouse.stg_fire_incidents as st
order by incident_number, loaded_at asc
limit 1 by incident_number;

create view warehouse.base_police_incidents as
select st.*
from warehouse.stg_police_incidents as st
order by incident_number, loaded_at asc
limit 1 by incident_number;

create view warehouse.base_traffic_crashes as
select st.*
from warehouse.stg_traffic_crashes as st
order by unique_id, loaded_at asc
limit 1 by unique_id;
