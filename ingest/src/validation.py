import logging
from datetime import datetime
from typing import Iterable, Literal

from pydantic import BaseModel, Field, ValidationError, computed_field


class Point(BaseModel):
    type: Literal["Point"]
    coordinates: tuple[float, float]


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
    point: Point | None = Field(None, exclude=True)
    data_as_of: datetime
    data_loaded_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def lat(self) -> float | None:
        return self.point.coordinates[1] if self.point is not None else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def long(self) -> float | None:
        return self.point.coordinates[0] if self.point is not None else None


_model_cls_mapping = {
    "a311_case": A311Case,
    "fire_incident": FireIncident,
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
