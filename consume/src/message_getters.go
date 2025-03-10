package main

import "time"

type Coordinates struct {
	Longitude float32
	Latitude  float32
}

func (r *A311Case) SchemaName() string {
	return SchemaName311Case
}

func (r *A311Case) Coordinates() *Coordinates {
	return &Coordinates{Longitude: r.Long, Latitude: r.Lat}
}

func (r *A311Case) Timestamp() time.Time {
	return r.RequestedDatetime
}

func (r *FireEmsCall) SchemaName() string {
	return SchemaNameFireEMSCall
}

func (r *FireEmsCall) Coordinates() *Coordinates {
	return &Coordinates{Longitude: r.Long, Latitude: r.Lat}
}

func (r *FireEmsCall) Timestamp() time.Time {
	return r.ReceivedDttm
}

func (r *FireIncident) SchemaName() string {
	return SchemaNameFireIncident
}

func (r *FireIncident) Coordinates() *Coordinates {
	return &Coordinates{Longitude: r.Long, Latitude: r.Lat}
}

func (r *FireIncident) Timestamp() time.Time {
	return r.IncidentDate
}

func (r *PoliceIncident) SchemaName() string {
	return SchemaNamePoliceIncident
}

func (r *PoliceIncident) Coordinates() *Coordinates {
	if r.Longitude == nil || r.Latitude == nil {
		return nil
	}
	return &Coordinates{Longitude: *r.Longitude, Latitude: *r.Latitude}
}

func (r *PoliceIncident) Timestamp() time.Time {
	return r.IncidentDatetime
}

func (r *TrafficCrash) SchemaName() string {
	return SchemaNameTrafficCrash
}

func (r *TrafficCrash) Coordinates() *Coordinates {
	if r.Long == nil || r.Lat == nil {
		return nil
	}
	return &Coordinates{Longitude: *r.Long, Latitude: *r.Lat}
}

func (r *TrafficCrash) Timestamp() time.Time {
	return r.CollisionDatetime
}
