package main

import (
	"time"
)

func BucketTime(t time.Time, precision time.Duration) time.Time {
	truncated := t.Truncate(precision)
	if truncated == t {
		return truncated
	}
	return truncated.Add(precision)
}

func BucketGeo(geohash string, precision int) string {
	return geohash[:precision]
}

func Rollup(rows []AggregateRow, timePrecision time.Duration, geoPrecision int) []Aggregate {
	type Bucket struct {
		OccurredAt time.Time
		Geohash    string
	}

	// Maintain ordering of `records` while rolling up.
	rollups := make([]Aggregate, len(rows))
	rollupIndexes := make(map[Bucket]int)
	rollupIndex := -1
	for _, row := range rows {
		bucket := Bucket{
			OccurredAt: BucketTime(row.OccurredAt, timePrecision),
			Geohash:    BucketGeo(row.Geohash, geoPrecision),
		}
		rollup := Aggregate{
			OccurredAt: bucket.OccurredAt,
			Geohash:    bucket.Geohash,
			Count:      row.Count,
		}

		if rollupIndex == -1 {
			rollupIndex++
			rollupIndexes[bucket] = rollupIndex
			rollups[rollupIndex] = rollup
			continue
		}

		idx, ok := rollupIndexes[bucket]
		if !ok {
			rollupIndex++
			rollupIndexes[bucket] = rollupIndex
			rollups[rollupIndex] = rollup
		} else {
			rollups[idx].Count += rollup.Count
		}
	}

	return rollups[:rollupIndex+1]
}
