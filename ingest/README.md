# Ingestion Worker

Ingestion worker to fetch data from [the external
API](https://datasf.org/opendata/) and write it to Kafka. When run, the
ingestion worker pages through data for a specific resource, given by the
`--resource-name` flag, validates each fetched record and ingests each page to
Kafka as a series of messages. See `src/resource_configs.json` for configured
resources that can be ingested.


## Getting Started

To run ingestion locally, first build the `docker compose` stack and start
dependent services:
```bash
$ docker compose build
$ docker compose up broker cache --wait
```

Then, run an ingestion worker for the e.g. 311-cases resource:
```bash
$ docker compose run --rm ingest-worker "src.main" --resource-name="311-cases"
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

Using the flags for development, a fast, local test run can be done:
```bash
$ docker compose run --rm ingest-worker "src.main" --resource-name="311-cases" --max-pages=1 --skip-wait
```

and the run's output checked:
```bash
$ redis-cli -u redis://localhost:8379/0 get "ingest-checkpoint:311-cases"
```

For local development, the [`produce_from_file`
module](src/scripts/produce_from_file.py) is also available. The module can be
used to produce data to Kafka from a local JSON file, rather than consuming data
from the third-party API. To use it, the data to use must be present locally and
the filename must match the resource name. For example, to download Fire and EMS
calls data:

```bash
$ mkdir -p sample-data
$ curl -X GET "https://data.sfgov.org/resource/nuek-vuh3.json" > sample-data/fire-ems-calls.json
```

then run the module to write the sample data to Kafka:
```bash
$ docker compose run --rm \
    --volume=$PWD/sample-data:/data \
    ingest-worker "src.scripts.produce_from_file" \
    --resource-name=fire-ems-calls \
    --input-file=/data/fire-ems-calls.json
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
