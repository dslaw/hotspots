import logging
from datetime import datetime
from typing import Iterable, Literal

from pydantic import BaseModel, ValidationError, computed_field


class Point(BaseModel):
    type: Literal["Point"]
    coordinates: tuple[float, float]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def lat(self) -> float:
        return self.coordinates[1]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def long(self) -> float:
        return self.coordinates[0]


class A311Case(BaseModel):
    service_request_id: int
    requested_datetime: datetime
    closed_date: datetime | None = None
    updated_datetime: datetime
    status_description: str
    status_notes: str | None = None
    agency_responsible: str
    service_name: str
    service_subtype: str
    service_details: str | None = None
    address: str
    street: str | None = None
    supervisor_district: float | None = None
    neighborhoods_sffind_boundaries: str | None = None
    analysis_neighborhood: str | None = None
    police_district: str | None = None
    lat: float
    long: float
    bos_2012: float | None = None
    source: str
    data_as_of: datetime
    data_loaded_at: datetime


class FireEMSCall(BaseModel):
    call_number: str
    unit_id: str
    incident_number: str
    call_type: str
    call_date: datetime
    watch_date: datetime
    received_dttm: datetime
    entry_dttm: datetime
    dispatch_dttm: datetime
    response_dttm: datetime | None = None
    on_scene_dttm: datetime | None = None
    transport_dttm: datetime | None = None
    hospital_dttm: datetime | None = None
    call_final_disposition: str
    available_dttm: datetime | None = None
    address: str | None = None
    city: str
    zipcode_of_incident: str | None = None
    battalion: str
    station_area: str
    box: str
    original_priority: str
    priority: str
    final_priority: str
    als_unit: bool | None = None
    call_type_group: str | None = None
    number_of_alarms: int
    unit_type: str
    unit_sequence_in_call_dispatch: int
    fire_prevention_district: str
    supervisor_district: str
    neighborhood_analysis_boundaries: str | None = None
    rowid: str
    case_location: Point
    data_as_of: datetime
    data_loaded_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def lat(self) -> float:
        return self.case_location.lat

    @computed_field  # type: ignore[prop-decorator]
    @property
    def long(self) -> float:
        return self.case_location.long


class FireIncident(BaseModel):
    incident_number: str
    exposure_number: str
    id: str
    address: str
    incident_date: datetime
    call_number: str
    alarm_dttm: datetime
    arrival_dttm: datetime
    close_dttm: datetime
    city: str
    zipcode: str | None = None
    battalion: str
    station_area: str
    box: str | None = None
    suppression_units: str
    suppression_personnel: str
    ems_units: str
    ems_personnel: str
    other_units: str
    other_personnel: str
    first_unit_on_scene: str | None = None
    estimated_property_loss: str | None = None
    estimated_contents_loss: str | None = None
    fire_fatalities: str
    fire_injuries: str
    civilian_fatalities: str
    civilian_injuries: str
    number_of_alarms: str
    primary_situation: str
    mutual_aid: str
    action_taken_primary: str | None = None
    action_taken_secondary: str | None = None
    action_taken_other: str | None = None
    detector_alerted_occupants: str | None = None
    property_use: str | None = None
    area_of_fire_origin: str | None = None
    ignition_cause: str | None = None
    ignition_factor_primary: str | None = None
    ignition_factor_secondary: str | None = None
    heat_source: str | None = None
    item_first_ignited: str | None = None
    human_factors_associated_with_ignition: str | None = None
    structure_type: str | None = None
    structure_status: str | None = None
    floor_of_fire_origin: int | None = None
    fire_spread: str | None = None
    no_flame_spread: str | None = None
    number_of_floors_with_minimum_damage: str | None = None
    number_of_floors_with_significant_damage: str | None = None
    number_of_floors_with_heavy_damage: str | None = None
    number_of_floors_with_extreme_damage: str | None = None
    detectors_present: str | None = None
    detector_operation: str | None = None
    detector_effectiveness: str | None = None
    detector_failure_reason: str | None = None
    automatic_extinguishing_system_present: str | None = None
    automatic_extinguishing_system_type: str | None = None
    supervisor_district: str | None = None
    neighborhood_district: str | None = None
    point: Point | None = None
    data_as_of: datetime
    data_loaded_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def lat(self) -> float | None:
        return self.point.lat if self.point is not None else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def long(self) -> float | None:
        return self.point.long if self.point is not None else None


class PoliceIncident(BaseModel):
    incident_datetime: datetime
    incident_date: datetime
    incident_time: str
    incident_year: str
    incident_day_of_week: str
    report_datetime: datetime
    row_id: str
    incident_id: str
    incident_number: str
    cad_number: str | None = None
    report_type_code: str
    report_type_description: str
    file_online: bool | None = None
    incident_code: str
    incident_category: str
    incident_subcategory: str
    incident_description: str
    resolution: str
    intersection: str | None = None
    cnn: str | None = None
    police_district: str
    analysis_neighborhood: str | None = None
    supervisor_district: float | None = None
    supervisor_district_2012: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    point: Point | None = None


class TrafficCrash(BaseModel):
    unique_id: str
    cnn_intrsctn_fkey: str | None = None
    cnn_sgmt_fkey: str | None = None
    case_id_pkey: str
    tb_latitude: float | None = None
    tb_longitude: float | None = None
    geocode_source: str
    geocode_location: str
    collision_datetime: datetime
    collision_date: datetime
    collision_time: str | None = None
    accident_year: str
    month: str
    day_of_week: str | None = None
    time_cat: str | None = None
    juris: str
    officer_id: str | None = None
    reporting_district: str | None = None
    beat_number: str | None = None
    primary_rd: str
    secondary_rd: str | None = None
    distance: float | None = None
    direction: str
    weather_1: str
    weather_2: str | None = None
    collision_severity: str
    type_of_collision: str
    mviw: str
    ped_action: str
    road_surface: str
    road_cond_1: str
    road_cond_2: str
    lighting: str
    control_device: str
    intersection: str
    vz_pcf_code: str | None = None
    vz_pcf_group: str | None = None
    vz_pcf_description: str
    vz_pcf_link: str | None = None
    number_killed: int
    number_injured: int
    street_view: str | None = None
    dph_col_grp: str
    dph_col_grp_description: str
    party_at_fault: str | None = None
    party1_type: str
    party1_dir_of_travel: str | None = None
    party1_move_pre_acc: str | None = None
    party2_type: str | None = None
    party2_dir_of_travel: str | None = None
    party2_move_pre_acc: str | None = None
    point: Point | None = None
    data_as_of: datetime
    data_updated_at: datetime
    data_loaded_at: datetime
    analysis_neighborhood: str | None = None
    supervisor_district: str | None = None
    police_district: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def lat(self) -> float | None:
        return self.point.lat if self.point is not None else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def long(self) -> float | None:
        return self.point.long if self.point is not None else None


_model_cls_mapping = {
    "a311_case": A311Case,
    "fire_ems_call": FireEMSCall,
    "fire_incident": FireIncident,
    "police_incident": PoliceIncident,
    "traffic_crash": TrafficCrash,
}


class Validator:
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self._model_cls = _model_cls_mapping[schema_name]

    def validate(self, records: Iterable[dict]) -> Iterable[dict]:
        for record in records:
            try:
                model = self._model_cls(**record)
            except ValidationError as e:
                logging.error(f"Record failed validation: {e}")
            else:
                yield model.model_dump()

        return
