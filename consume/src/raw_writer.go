package main

import (
	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"
)

type BatchPreparer interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// RawWriter writes data directly (i.e. as it is received) to a data sink.
type RawWriter struct {
	conn BatchPreparer
}

func NewRawWriter(conn BatchPreparer) *RawWriter {
	return &RawWriter{conn: conn}
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

	for record := range DecodeMessages(SchemaNameHeader, messages) {
		switch record.(type) {
		case *A311Case:
			batch, _ := batches[SchemaName311Case]
			if err := batch.Append(
				record.(*A311Case).ServiceRequestID,
				record.(*A311Case).RequestedDatetime,
				record.(*A311Case).ClosedDate,
				record.(*A311Case).UpdatedDatetime,
				record.(*A311Case).StatusDescription,
				record.(*A311Case).StatusNotes,
				record.(*A311Case).AgencyResponsible,
				record.(*A311Case).ServiceName,
				record.(*A311Case).ServiceSubtype,
				record.(*A311Case).ServiceDetails,
				record.(*A311Case).Address,
				record.(*A311Case).Street,
				record.(*A311Case).SupervisorDistrict,
				record.(*A311Case).NeighborhoodsSffindBoundaries,
				record.(*A311Case).AnalysisNeighborhood,
				record.(*A311Case).PoliceDistrict,
				record.(*A311Case).Lat,
				record.(*A311Case).Long,
				record.(*A311Case).Bos2012,
				record.(*A311Case).Source,
				record.(*A311Case).DataAsOf,
				record.(*A311Case).DataLoadedAt,
			); err != nil {
				return err
			}

		case *FireEmsCall:
			batch, _ := batches[SchemaNameFireEMSCall]
			if err := batch.Append(
				record.(*FireEmsCall).CallNumber,
				record.(*FireEmsCall).UnitID,
				record.(*FireEmsCall).IncidentNumber,
				record.(*FireEmsCall).CallType,
				record.(*FireEmsCall).CallDate,
				record.(*FireEmsCall).WatchDate,
				record.(*FireEmsCall).ReceivedDttm,
				record.(*FireEmsCall).EntryDttm,
				record.(*FireEmsCall).DispatchDttm,
				record.(*FireEmsCall).ResponseDttm,
				record.(*FireEmsCall).OnSceneDttm,
				record.(*FireEmsCall).TransportDttm,
				record.(*FireEmsCall).HospitalDttm,
				record.(*FireEmsCall).CallFinalDisposition,
				record.(*FireEmsCall).AvailableDttm,
				record.(*FireEmsCall).Address,
				record.(*FireEmsCall).City,
				record.(*FireEmsCall).ZipcodeOfIncident,
				record.(*FireEmsCall).Battalion,
				record.(*FireEmsCall).StationArea,
				record.(*FireEmsCall).Box,
				record.(*FireEmsCall).OriginalPriority,
				record.(*FireEmsCall).Priority,
				record.(*FireEmsCall).FinalPriority,
				record.(*FireEmsCall).AlsUnit,
				record.(*FireEmsCall).CallTypeGroup,
				record.(*FireEmsCall).NumberOfAlarms,
				record.(*FireEmsCall).UnitType,
				record.(*FireEmsCall).UnitSequenceInCallDispatch,
				record.(*FireEmsCall).FirePreventionDistrict,
				record.(*FireEmsCall).SupervisorDistrict,
				record.(*FireEmsCall).NeighborhoodAnalysisBoundaries,
				record.(*FireEmsCall).Rowid,
				record.(*FireEmsCall).Lat,
				record.(*FireEmsCall).Long,
				record.(*FireEmsCall).DataAsOf,
				record.(*FireEmsCall).DataLoadedAt,
			); err != nil {
				return err
			}

		case *FireIncident:
			batch, _ := batches[SchemaNameFireIncident]
			if err := batch.Append(
				record.(*FireIncident).IncidentNumber,
				record.(*FireIncident).ExposureNumber,
				record.(*FireIncident).ID,
				record.(*FireIncident).Address,
				record.(*FireIncident).IncidentDate,
				record.(*FireIncident).CallNumber,
				record.(*FireIncident).AlarmDttm,
				record.(*FireIncident).ArrivalDttm,
				record.(*FireIncident).CloseDttm,
				record.(*FireIncident).City,
				record.(*FireIncident).Zipcode,
				record.(*FireIncident).Battalion,
				record.(*FireIncident).StationArea,
				record.(*FireIncident).Box,
				record.(*FireIncident).SuppressionUnits,
				record.(*FireIncident).SuppressionPersonnel,
				record.(*FireIncident).EmsUnits,
				record.(*FireIncident).EmsPersonnel,
				record.(*FireIncident).OtherUnits,
				record.(*FireIncident).OtherPersonnel,
				record.(*FireIncident).FirstUnitOnScene,
				record.(*FireIncident).EstimatedPropertyLoss,
				record.(*FireIncident).EstimatedContentsLoss,
				record.(*FireIncident).FireFatalities,
				record.(*FireIncident).FireInjuries,
				record.(*FireIncident).CivilianFatalities,
				record.(*FireIncident).CivilianInjuries,
				record.(*FireIncident).NumberOfAlarms,
				record.(*FireIncident).PrimarySituation,
				record.(*FireIncident).MutualAid,
				record.(*FireIncident).ActionTakenPrimary,
				record.(*FireIncident).ActionTakenSecondary,
				record.(*FireIncident).ActionTakenOther,
				record.(*FireIncident).DetectorAlertedOccupants,
				record.(*FireIncident).PropertyUse,
				record.(*FireIncident).AreaOfFireOrigin,
				record.(*FireIncident).IgnitionCause,
				record.(*FireIncident).IgnitionFactorPrimary,
				record.(*FireIncident).IgnitionFactorSecondary,
				record.(*FireIncident).HeatSource,
				record.(*FireIncident).ItemFirstIgnited,
				record.(*FireIncident).HumanFactorsAssociatedWithIgnition,
				record.(*FireIncident).StructureType,
				record.(*FireIncident).StructureStatus,
				record.(*FireIncident).FloorOfFireOrigin,
				record.(*FireIncident).FireSpread,
				record.(*FireIncident).NoFlameSpread,
				record.(*FireIncident).NumberOfFloorsWithMinimumDamage,
				record.(*FireIncident).NumberOfFloorsWithSignificantDamage,
				record.(*FireIncident).NumberOfFloorsWithHeavyDamage,
				record.(*FireIncident).NumberOfFloorsWithExtremeDamage,
				record.(*FireIncident).DetectorsPresent,
				record.(*FireIncident).DetectorOperation,
				record.(*FireIncident).DetectorEffectiveness,
				record.(*FireIncident).DetectorFailureReason,
				record.(*FireIncident).AutomaticExtinguishingSystemPresent,
				record.(*FireIncident).AutomaticExtinguishingSystemType,
				record.(*FireIncident).AutomaticExtinguishingSystemPerformance,
				record.(*FireIncident).AutomaticExtinguishingSystemFailureReason,
				record.(*FireIncident).NumberOfSprinklerHeadsOperating,
				record.(*FireIncident).SupervisorDistrict,
				record.(*FireIncident).NeighborhoodDistrict,
				record.(*FireIncident).Lat,
				record.(*FireIncident).Long,
				record.(*FireIncident).DataAsOf,
				record.(*FireIncident).DataLoadedAt,
			); err != nil {
				return err
			}

		case *PoliceIncident:
			batch, _ := batches[SchemaNamePoliceIncident]
			if err := batch.Append(
				record.(*PoliceIncident).IncidentDatetime,
				record.(*PoliceIncident).IncidentDate,
				record.(*PoliceIncident).IncidentTime,
				record.(*PoliceIncident).IncidentYear,
				record.(*PoliceIncident).IncidentDayOfWeek,
				record.(*PoliceIncident).ReportDatetime,
				record.(*PoliceIncident).RowID,
				record.(*PoliceIncident).IncidentID,
				record.(*PoliceIncident).IncidentNumber,
				record.(*PoliceIncident).CadNumber,
				record.(*PoliceIncident).ReportTypeCode,
				record.(*PoliceIncident).ReportTypeDescription,
				record.(*PoliceIncident).FileOnline,
				record.(*PoliceIncident).IncidentCode,
				record.(*PoliceIncident).IncidentCategory,
				record.(*PoliceIncident).IncidentSubcategory,
				record.(*PoliceIncident).IncidentDescription,
				record.(*PoliceIncident).Resolution,
				record.(*PoliceIncident).Intersection,
				record.(*PoliceIncident).Cnn,
				record.(*PoliceIncident).PoliceDistrict,
				record.(*PoliceIncident).AnalysisNeighborhood,
				record.(*PoliceIncident).SupervisorDistrict,
				record.(*PoliceIncident).SupervisorDistrict2012,
				record.(*PoliceIncident).Latitude,
				record.(*PoliceIncident).Longitude,
			); err != nil {
				return err
			}

		case *TrafficCrash:
			batch, _ := batches[SchemaNameTrafficCrash]
			if err := batch.Append(
				record.(*TrafficCrash).UniqueID,
				record.(*TrafficCrash).CnnIntrsctnFkey,
				record.(*TrafficCrash).CnnSgmtFkey,
				record.(*TrafficCrash).CaseIDPkey,
				record.(*TrafficCrash).TbLatitude,
				record.(*TrafficCrash).TbLongitude,
				record.(*TrafficCrash).GeocodeSource,
				record.(*TrafficCrash).GeocodeLocation,
				record.(*TrafficCrash).CollisionDatetime,
				record.(*TrafficCrash).CollisionDate,
				record.(*TrafficCrash).CollisionTime,
				record.(*TrafficCrash).AccidentYear,
				record.(*TrafficCrash).Month,
				record.(*TrafficCrash).DayOfWeek,
				record.(*TrafficCrash).TimeCat,
				record.(*TrafficCrash).Juris,
				record.(*TrafficCrash).OfficerID,
				record.(*TrafficCrash).ReportingDistrict,
				record.(*TrafficCrash).BeatNumber,
				record.(*TrafficCrash).PrimaryRd,
				record.(*TrafficCrash).SecondaryRd,
				record.(*TrafficCrash).Distance,
				record.(*TrafficCrash).Direction,
				record.(*TrafficCrash).Weather1,
				record.(*TrafficCrash).Weather2,
				record.(*TrafficCrash).CollisionSeverity,
				record.(*TrafficCrash).TypeOfCollision,
				record.(*TrafficCrash).Mviw,
				record.(*TrafficCrash).PedAction,
				record.(*TrafficCrash).RoadSurface,
				record.(*TrafficCrash).RoadCond1,
				record.(*TrafficCrash).RoadCond2,
				record.(*TrafficCrash).Lighting,
				record.(*TrafficCrash).ControlDevice,
				record.(*TrafficCrash).Intersection,
				record.(*TrafficCrash).VzPcfCode,
				record.(*TrafficCrash).VzPcfGroup,
				record.(*TrafficCrash).VzPcfDescription,
				record.(*TrafficCrash).VzPcfLink,
				record.(*TrafficCrash).NumberKilled,
				record.(*TrafficCrash).NumberInjured,
				record.(*TrafficCrash).StreetView,
				record.(*TrafficCrash).DphColGrp,
				record.(*TrafficCrash).DphColGrpDescription,
				record.(*TrafficCrash).PartyAtFault,
				record.(*TrafficCrash).Party1Type,
				record.(*TrafficCrash).Party1DirOfTravel,
				record.(*TrafficCrash).Party1MovePreAcc,
				record.(*TrafficCrash).Party2Type,
				record.(*TrafficCrash).Party2DirOfTravel,
				record.(*TrafficCrash).Party2MovePreAcc,
				record.(*TrafficCrash).DataAsOf,
				record.(*TrafficCrash).DataUpdatedAt,
				record.(*TrafficCrash).DataLoadedAt,
				record.(*TrafficCrash).Lat,
				record.(*TrafficCrash).Long,
				record.(*TrafficCrash).AnalysisNeighborhood,
				record.(*TrafficCrash).SupervisorDistrict,
				record.(*TrafficCrash).PoliceDistrict,
			); err != nil {
				return err
			}

		default:
			slog.Error("Message with unrecognized schema name", "schema_name, dropping message", record.SchemaName())
			continue
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
