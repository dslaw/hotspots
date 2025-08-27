package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AggregateRow struct {
	OccurredAt time.Time `db:"occurred_at"`
	Geohash    string    `db:"geo_id"`
	Count      int32     `db:"incident_count"`
}

type Repo struct {
	conn *pgxpool.Pool
}

const getAggregatesQuery = `
select
    occurred_at,
    geo_id,
    sum(incident_count) as incident_count
from aggregate_buckets
where occurred_at >= $1 and occurred_at <= $2
group by occurred_at, geo_id
order by occurred_at, geo_id
`

func (r *Repo) GetAggregateRows(ctx context.Context, startTime, endTime time.Time) ([]AggregateRow, error) {
	rows, err := r.conn.Query(ctx, getAggregatesQuery, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[AggregateRow])
}

func (r *Repo) InsertAggregateRows(ctx context.Context, records []AggregateRow) error {
	rows := make([][]any, len(records))
	for idx, record := range records {
		row := make([]any, 3)
		row[0] = record.OccurredAt
		row[1] = record.Geohash
		row[2] = record.Count
		rows[idx] = row
	}

	_, err := r.conn.CopyFrom(
		ctx,
		pgx.Identifier([]string{"aggregate_buckets"}),
		[]string{"occurred_at", "geo_id", "incident_count"},
		pgx.CopyFromRows(rows),
	)
	return err
}

const upsertAggregateStmt = `
with delete_existing as (
    delete from aggregate_buckets
    where occurred_at = $1 and geo_id = $2
)
insert into aggregate_buckets (occurred_at, geo_id, incident_count)
values ($1, $2, $3)
`

func (r *Repo) UpsertAggregateRows(ctx context.Context, records []AggregateRow) error {
	tx, err := r.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, record := range records {
		batch.Queue(upsertAggregateStmt, record.OccurredAt, record.Geohash, record.Count)
	}

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
