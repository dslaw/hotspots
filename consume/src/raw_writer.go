package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"
)

func mapNullableUInt8(v *int) *uint8 {
	if v == nil {
		return nil
	}
	value := uint8(*v)
	return &value
}

func mapA311Case(record *A311Case) []interface{} {
	return []interface{}{
		record.ServiceRequestID,
		record.RequestedDatetime,
		record.ClosedDate,
		record.UpdatedDatetime,
		record.StatusDescription,
		record.StatusNotes,
		record.AgencyResponsible,
		record.ServiceName,
		record.ServiceSubtype,
		record.ServiceDetails,
		record.Address,
		record.Street,
		record.SupervisorDistrict,
		record.NeighborhoodsSffindBoundaries,
		record.AnalysisNeighborhood,
		record.PoliceDistrict,
		record.Lat,
		record.Long,
		record.Bos2012,
		record.Source,
		record.DataAsOf,
		record.DataLoadedAt,
	}
}

func mapFireEmsCall(record *FireEmsCall) []interface{} {
	return []interface{}{
		record.CallNumber,
		record.UnitID,
		record.IncidentNumber,
		record.CallType,
		record.CallDate,
		record.WatchDate,
		record.ReceivedDttm,
		record.EntryDttm,
		record.DispatchDttm,
		record.ResponseDttm,
		record.OnSceneDttm,
		record.TransportDttm,
		record.HospitalDttm,
		record.CallFinalDisposition,
		record.AvailableDttm,
		record.Address,
		record.City,
		record.ZipcodeOfIncident,
		record.Battalion,
		record.StationArea,
		record.Box,
		record.OriginalPriority,
		record.Priority,
		record.FinalPriority,
		record.AlsUnit,
		record.CallTypeGroup,
		uint8(record.NumberOfAlarms),
		record.UnitType,
		record.UnitSequenceInCallDispatch,
		record.FirePreventionDistrict,
		record.SupervisorDistrict,
		record.NeighborhoodAnalysisBoundaries,
		record.Rowid,
		record.Lat,
		record.Long,
		record.DataAsOf,
		record.DataLoadedAt,
	}
}

func mapFireIncident(record *FireIncident) []interface{} {
	return []interface{}{
		record.IncidentNumber,
		record.ExposureNumber,
		record.ID,
		record.Address,
		record.IncidentDate,
		record.CallNumber,
		record.AlarmDttm,
		record.ArrivalDttm,
		record.CloseDttm,
		record.City,
		record.Zipcode,
		record.Battalion,
		record.StationArea,
		record.Box,
		record.SuppressionUnits,
		record.SuppressionPersonnel,
		record.EmsUnits,
		record.EmsPersonnel,
		record.OtherUnits,
		record.OtherPersonnel,
		record.FirstUnitOnScene,
		record.EstimatedPropertyLoss,
		record.EstimatedContentsLoss,
		record.FireFatalities,
		record.FireInjuries,
		record.CivilianFatalities,
		record.CivilianInjuries,
		record.NumberOfAlarms,
		record.PrimarySituation,
		record.MutualAid,
		record.ActionTakenPrimary,
		record.ActionTakenSecondary,
		record.ActionTakenOther,
		record.DetectorAlertedOccupants,
		record.PropertyUse,
		record.AreaOfFireOrigin,
		record.IgnitionCause,
		record.IgnitionFactorPrimary,
		record.IgnitionFactorSecondary,
		record.HeatSource,
		record.ItemFirstIgnited,
		record.HumanFactorsAssociatedWithIgnition,
		record.StructureType,
		record.StructureStatus,
		mapNullableUInt8(record.FloorOfFireOrigin),
		record.FireSpread,
		record.NoFlameSpread,
		record.NumberOfFloorsWithMinimumDamage,
		record.NumberOfFloorsWithSignificantDamage,
		record.NumberOfFloorsWithHeavyDamage,
		record.NumberOfFloorsWithExtremeDamage,
		record.DetectorsPresent,
		record.DetectorOperation,
		record.DetectorEffectiveness,
		record.DetectorFailureReason,
		record.AutomaticExtinguishingSystemPresent,
		record.AutomaticExtinguishingSystemType,
		record.AutomaticExtinguishingSystemPerformance,
		record.AutomaticExtinguishingSystemFailureReason,
		record.NumberOfSprinklerHeadsOperating,
		record.SupervisorDistrict,
		record.NeighborhoodDistrict,
		record.Lat,
		record.Long,
		record.DataAsOf,
		record.DataLoadedAt,
	}
}

func mapPoliceIncident(record *PoliceIncident) []interface{} {
	return []interface{}{
		record.IncidentDatetime,
		record.IncidentDate,
		record.IncidentTime,
		record.IncidentYear,
		record.IncidentDayOfWeek,
		record.ReportDatetime,
		record.RowID,
		record.IncidentID,
		record.IncidentNumber,
		record.CadNumber,
		record.ReportTypeCode,
		record.ReportTypeDescription,
		record.FileOnline,
		record.IncidentCode,
		record.IncidentCategory,
		record.IncidentSubcategory,
		record.IncidentDescription,
		record.Resolution,
		record.Intersection,
		record.Cnn,
		record.PoliceDistrict,
		record.AnalysisNeighborhood,
		record.SupervisorDistrict,
		record.SupervisorDistrict2012,
		record.Latitude,
		record.Longitude,
	}
}

func mapTrafficCrash(record *TrafficCrash) []interface{} {
	return []interface{}{
		record.UniqueID,
		record.CnnIntrsctnFkey,
		record.CnnSgmtFkey,
		record.CaseIDPkey,
		record.TbLatitude,
		record.TbLongitude,
		record.GeocodeSource,
		record.GeocodeLocation,
		record.CollisionDatetime,
		record.CollisionDate,
		record.CollisionTime,
		record.AccidentYear,
		record.Month,
		record.DayOfWeek,
		record.TimeCat,
		record.Juris,
		record.OfficerID,
		record.ReportingDistrict,
		record.BeatNumber,
		record.PrimaryRd,
		record.SecondaryRd,
		record.Distance,
		record.Direction,
		record.Weather1,
		record.Weather2,
		record.CollisionSeverity,
		record.TypeOfCollision,
		record.Mviw,
		record.PedAction,
		record.RoadSurface,
		record.RoadCond1,
		record.RoadCond2,
		record.Lighting,
		record.ControlDevice,
		record.Intersection,
		record.VzPcfCode,
		record.VzPcfGroup,
		record.VzPcfDescription,
		record.VzPcfLink,
		uint8(record.NumberKilled),
		uint8(record.NumberInjured),
		record.StreetView,
		record.DphColGrp,
		record.DphColGrpDescription,
		record.PartyAtFault,
		record.Party1Type,
		record.Party1DirOfTravel,
		record.Party1MovePreAcc,
		record.Party2Type,
		record.Party2DirOfTravel,
		record.Party2MovePreAcc,
		record.DataAsOf,
		record.DataUpdatedAt,
		record.DataLoadedAt,
		record.Lat,
		record.Long,
		record.AnalysisNeighborhood,
		record.SupervisorDistrict,
		record.PoliceDistrict,
	}
}

func makeDatabaseTuple[R any](record R, mapper func(R) []interface{}, bucketTimestamp *time.Time, bucketGeohash *string, loadedAt time.Time) []interface{} {
	values := mapper(record)
	values = append(values, bucketTimestamp)
	values = append(values, bucketGeohash)
	values = append(values, loadedAt)
	return values
}

type BatchPreparer interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// RawWriter writes data directly (i.e. as it is received) to a data sink.
type RawWriter struct {
	conn     BatchPreparer
	bucketer *Bucketer
}

func NewRawWriter(conn BatchPreparer, bucketer *Bucketer) *RawWriter {
	return &RawWriter{conn: conn, bucketer: bucketer}
}

// WriteRawRecords decodes the messages and writes them to the data sink,
// sending one batch of records per each record type. There is no guarantee that
// all messages will be written atomically.
func (w *RawWriter) WriteRawRecords(ctx context.Context, messages []kafka.Message) error {
	batches := make(map[string]driver.Batch)
	for _, item := range []struct {
		Stmt       string
		SchemaName string
	}{
		{Stmt: "insert into warehouse.a311_cases", SchemaName: SchemaName311Case},
		{Stmt: "insert into warehouse.fire_ems_calls", SchemaName: SchemaNameFireEMSCall},
		{Stmt: "insert into warehouse.fire_incidents", SchemaName: SchemaNameFireIncident},
		{Stmt: "insert into warehouse.police_incidents", SchemaName: SchemaNamePoliceIncident},
		{Stmt: "insert into warehouse.traffic_crashes", SchemaName: SchemaNameTrafficCrash},
	} {
		batch, err := w.conn.PrepareBatch(ctx, item.Stmt, driver.WithReleaseConnection(), driver.WithCloseOnFlush())
		if err != nil {
			return err
		}
		batches[item.SchemaName] = batch
	}

	loadedAt := time.Now().UTC()

	for record := range DecodeMessages(SchemaNameHeader, messages) {
		var (
			bucketTimestamp *time.Time
			bucketGeohash   *string
		)

		bucket, ok := w.bucketer.MakeBucket(record)
		if ok {
			bucketTimestamp = &bucket.Timestamp
			bucketGeohash = &bucket.Geohash
		}

		var (
			batch  driver.Batch
			values []interface{}
		)

		switch record.(type) {
		case *A311Case:
			values = makeDatabaseTuple(record.(*A311Case), mapA311Case, bucketTimestamp, bucketGeohash, loadedAt)
			batch, _ = batches[SchemaName311Case]
		case *FireEmsCall:
			values = makeDatabaseTuple(record.(*FireEmsCall), mapFireEmsCall, bucketTimestamp, bucketGeohash, loadedAt)
			batch, _ = batches[SchemaNameFireEMSCall]
		case *FireIncident:
			values = makeDatabaseTuple(record.(*FireIncident), mapFireIncident, bucketTimestamp, bucketGeohash, loadedAt)
			batch, _ = batches[SchemaNameFireIncident]
		case *PoliceIncident:
			values = makeDatabaseTuple(record.(*PoliceIncident), mapPoliceIncident, bucketTimestamp, bucketGeohash, loadedAt)
			batch, _ = batches[SchemaNamePoliceIncident]
		case *TrafficCrash:
			values = makeDatabaseTuple(record.(*TrafficCrash), mapTrafficCrash, bucketTimestamp, bucketGeohash, loadedAt)
			batch, _ = batches[SchemaNameTrafficCrash]
		default:
			slog.Error("Message with unrecognized schema name", "schema_name, dropping message", record.SchemaName())
			continue
		}

		if err := batch.Append(values...); err != nil {
			return err
		}
	}

	for _, batch := range batches {
		if err := batch.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Write writes messages to the data sink.
// NB: Malformed messages will be dropped.
func (w *RawWriter) Write(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	return w.WriteRawRecords(ctx, messages)
}
