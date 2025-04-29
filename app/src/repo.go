package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const aggregatesQuery = `
select
    occurred_at,
    geo_id,
    sum(incident_count) as incident_count
from aggregate_buckets
where occurred_at >= $1 and occurred_at <= $2
group by occurred_at, geo_id
order by occurred_at, geo_id
`

type AggregateRow struct {
	OccurredAt time.Time
	Geohash    string
	Count      int32
}

type Repo struct {
	conn *pgxpool.Pool
}

func (r *Repo) GetAggregateRows(ctx context.Context, startTime, endTime time.Time) ([]AggregateRow, error) {
	rows, err := r.conn.Query(ctx, aggregatesQuery, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggregateRows []AggregateRow
	for rows.Next() {
		var r AggregateRow
		if err := rows.Scan(&r.OccurredAt, &r.Geohash, &r.Count); err != nil {
			return nil, err
		}
		aggregateRows = append(aggregateRows, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return aggregateRows, nil
}
