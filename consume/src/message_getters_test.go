package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	timestamp = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	longitude = float32(100.123)
	latitude  = float32(99.123)
)

func TestA311Case(t *testing.T) {
	record := &A311Case{
		RequestedDatetime: timestamp,
		Long:              longitude,
		Lat:               latitude,
	}

	assert.Equal(t, "a311_case", record.SchemaName())

	coords := record.Coordinates()
	assert.NotNil(t, coords)
	assert.Equal(t, longitude, coords.Longitude)
	assert.Equal(t, latitude, coords.Latitude)

	assert.Equal(t, timestamp, record.Timestamp())
}

func TestFireEmsCall(t *testing.T) {
	timestamp := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	record := &FireEmsCall{
		ReceivedDttm: timestamp,
		Long:         longitude,
		Lat:          latitude,
	}

	assert.Equal(t, "fire_ems_call", record.SchemaName())

	coords := record.Coordinates()
	assert.NotNil(t, coords)
	assert.Equal(t, longitude, coords.Longitude)
	assert.Equal(t, latitude, coords.Latitude)

	assert.Equal(t, timestamp, record.Timestamp())
}

func TestFireIncident(t *testing.T) {
	timestamp := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	record := &FireIncident{
		IncidentDate: timestamp,
		Long:         longitude,
		Lat:          latitude,
	}

	assert.Equal(t, "fire_incident", record.SchemaName())

	coords := record.Coordinates()
	assert.NotNil(t, coords)
	assert.Equal(t, longitude, coords.Longitude)
	assert.Equal(t, latitude, coords.Latitude)

	assert.Equal(t, timestamp, record.Timestamp())
}

func TestPoliceIncident(t *testing.T) {
	record := &PoliceIncident{
		IncidentDatetime: timestamp,
		Longitude:        &longitude,
		Latitude:         &latitude,
	}

	assert.Equal(t, "police_incident", record.SchemaName())

	coords := record.Coordinates()
	assert.NotNil(t, coords)
	assert.Equal(t, longitude, coords.Longitude)
	assert.Equal(t, latitude, coords.Latitude)

	assert.Equal(t, timestamp, record.Timestamp())
}

func TestTrafficCrash(t *testing.T) {
	record := &TrafficCrash{
		CollisionDatetime: timestamp,
		Long:              &longitude,
		Lat:               &latitude,
	}

	assert.Equal(t, "traffic_crash", record.SchemaName())

	coords := record.Coordinates()
	assert.NotNil(t, coords)
	assert.Equal(t, longitude, coords.Longitude)
	assert.Equal(t, latitude, coords.Latitude)

	assert.Equal(t, timestamp, record.Timestamp())
}
