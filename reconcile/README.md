# Reconciliation Worker

Reconciliation worker to update the aggregates database with up-to-date
aggregated data.


## Getting Started

To run ingestion locally, first build the `docker compose` stack and start
dependent services:
```bash
$ docker compose build
$ docker compose up aggregates-db warehouse --wait
```

then, run reconciliation over a time period:
```bash
$ docker compose run --rm reconciliation-worker --start-time="2020-01-01T00:00:00Z" --end-time="2030-01-01T00:00:00Z"
```

data that was loaded into the warehouse in the time period will have incident
counts computed for its bucket and written to the aggregates database. Existing
records for a bucket that was computed as part of the reconciliation run will be
removed.


## Development

To get started with development, first set up a Python environment using `uv`:
```bash
$ uv sync
```

To run linting/formating:
```bash
uv run ruff check . --fix --extend-select I
uv run ruff format .
```
