-- migrate:up
create table aggregate_buckets (
    id int generated always as identity,
    occurred_at timestamp without time zone not null,
    geo_id varchar(12) not null,
    incident_count int not null,

    primary key (id)
);

create index on aggregate_buckets (occurred_at, geo_id) include (incident_count);


-- migrate:down
drop table aggregate_buckets cascade;
