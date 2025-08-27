// Methods for known record types to get aggregation/bucketing information.
package main

import "time"

func (r *A311Case) Coordinates() (float64, float64) {
	return r.Long, r.Lat
}

func (r *A311Case) Timestamp() time.Time {
	return r.RequestedDatetime
}

func (r *FireIncident) Coordinates() (float64, float64) {
	return r.Long, r.Lat
}

func (r *FireIncident) Timestamp() time.Time {
	return r.IncidentDate
}
