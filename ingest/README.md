# Ingestion Worker

Ingestion worker to fetch data from [the external
API](https://datasf.org/opendata/) and write it to Kafka. When run, the
ingestion worker pages through data for a specific resource, given by the
`--resource-name` flag, validates each fetched record and ingests each page to
Kafka as a series of messages. See `src/resource_configs.json` for configured
resources that can be ingested.


## Getting Started

To run ingestion locally, first build the `docker compose` stack from the
project's root directory and start dependent services:
```bash
$ cd ..
$ ln -s ingest/.env .env-ingest
$ docker compose build
$ docker compose up broker cache --wait
```

Then, run an ingestion worker for the e.g. 311-cases resource:
```bash
$ docker compose run --rm ingest-worker --resource-name="311-cases"
```

There are additional flags that can be passed to the ingestion worker which are
handy for local development:
- `--skip-wait`: the ingestion worker uses the last run's checkpoint to
  determine whether it should begin ingesting new data or not, and may wait for
  some time before ingesting, depending on the last run time and resource's
  configured run cadence. Using the `--skip-wait` flag causes the ingestion
  worker to begin ingestion immediately, ignoring the last run time and cadence.
- `--max-pages`: by default, the ingestion worker will continuously ingest data
  until there is no more to ingest. Passing a low number, e.g. `1`, to the
  `--max-pages` flag will limit the amount of data that is ingested.
  Specifically, no more than `--max-pages` will be read, with the page size
  being controlled by the `PAGE_SIZE` environment variable. This allows for
  ingesting a small amount of data to exercise the third-party integration for
  local development and testing.
- `--write-to-file`: instead of writing ingested data to Kafka (default), the
  `--write-to-file` flag will cause the ingestion worker to write the fetched
  data to a local avro file. Useful for inspecting the data that would be
  produced by the worker.

Using the flags for development, a fast, local test run can be done:
```bash
$ docker compose run --rm ingest-worker --resource-name="311-cases" --max-pages=1 --skip-wait
```

and the run's output checked:
```bash
$ redis-cli -u redis://localhost:8379/0 get "ingest-checkpoint:311-cases"
```

if writing the ingested data to the local filesystem:
```bash
$ docker compose run --rm ingest-worker --resource-name="311-cases" --max-pages=1 --skip-wait --write-to-file
$ redis-cli -u redis://localhost:8379/0 get "ingest-checkpoint:311-cases"
$ wc -l ../data/311-cases/*.avro
```


## Development

To get started with development, first set up a Python environment using `uv`:
```bash
$ uv sync
```

To run linting/formating/type checking:
```bash
uv run ruff check . --fix --extend-select I
uv run ruff format .
uv run mypy src/
```

To run tests:
```bash
$ uv run python -m pytest tests/
```
