create table aggregate_buckets (
    id int generated always as identity,
    -- Seconds since epoch.
    occurred_at bigint not null,
    -- Geohash of precision 12 has 12 characters.
    geo_id varchar(12) not null, 
    incident_count int not null,

    primary key (id)
);

create index on aggregate_buckets (occurred_at, geo_id) include (incident_count);

create view aggregated as (
    select
        occurred_at,
        geo_id,
        sum(incident_count) as incident_count
    from aggregate_buckets
    group by occurred_at, geo_id
);
