# hotspots

Query for a heatmap of incidents (fire, police, EMS calls, etc) within San
Francisco.

Public data provided by [San Francisco Open Data](https://datasf.org/opendata/)
is used to create counts of various incidents (e.g. fire incidents, EMS calls),
aggregated to arbitrary spatio-temporal granularity, and return them for
mapping. Currently, data can be ingested from the following incident types:

- [311 Cases](https://data.sfgov.org/City-Infrastructure/311-Cases/vw6y-z8j6/about_data)
- [Fire Department and EMS Dispatched Calls](https://data.sfgov.org/Public-Safety/Fire-Department-and-Emergency-Medical-Services-Dis/nuek-vuh3/about_data)
- [SF Fire Incidents](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data)
- [SFPD Incidents](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783/about_data)
- [SF Traffic Crashes Resulting in Injury](https://data.sfgov.org/Public-Safety/Traffic-Crashes-Resulting-in-Injury/ubvf-ztfx/about_data)


## Overview

Data ingestion, storage, computation and serving spans a number of system
components. Ingested data follows two paths: a fast path which computes the
incident counts at the most granular configured spatio-temporal level and
makes them available for immediate end-user consumption, with the caveat that
the counts may not be accurate, and a slow path which periodically recomputes
incident counts based on all available data and updates them for serving.


## Getting Started

Build the project:
```bash
$ docker compose build
```

Run database migrations:
```bash
$ docker compose run --rm aggregates-db-migrations -e DATABASE_URL --wait up
```

Start services:
```bash
$ docker compose up app aggregates-consumer raw-consumer --wait
```

Begin ingestion for an incident type/resource, e.g. 311-cases:

> [!NOTE]
> Before running ingestion, you may want to obtain and set the `API_TOKEN`
> environment variable in `.env`. It is not necessary, but may prevent/mitigate
> throttling.
>
> See the ["Throttling and Application Tokens"](https://dev.socrata.com/consumers/getting-started.html)
> section in the Socrata developer documentation for further information on how
> to obtain an API token and the implications of not using one.

> [!WARNING]
> The ingest worker will continue to run until all data is consumed or it is
> manually shut down. The `--max-pages` flag can be appended to the following
> command with the number of pages to limit ingestion to (e.g. `--max-pages=5`),
> as desired, or the run container can be manually stopped.

```bash
$ docker compose run --rm ingest-worker "src.main" --resource-name="311-cases"
```

Incident counts from the fast path can be queried using e.g. `curl`:
```bash
$ curl -X GET "localhost:8080/aggregates"
```

Reconciliation can be run to fix incident counts, as necessary, passing start
and end time parameters:
```bash
$ docker compose run --rm reconciliation-worker --start-time="2000-01-01T00:00:00Z" --end-time="2030-01-01T00:00:00Z"
```

For specific instructions relating to development of individual subprojects, see
the subproject's README file, e.g. [`ingest/README.md`](ingest/README.md).
