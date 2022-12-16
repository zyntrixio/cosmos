# Bink Provided Loyalty 2.0

## Setup

- `CREATE DATABASE cosmos;`
- `poetry install`
- create a `local.env` file in the root directory (below)
- `poetry install`

### Example `local.env`

    REDIS_URL=redis://localhost:6379/0
    POSTGRES_PASSWORD=postgres
    LOG_FORMATTER=brief
    SQL_DEBUG=True

## Data generation (bestla)

`poetry run bestla --password postgres -B --retailer trenette --max-val 100 --add-fetch-types --refund-window 10 -c trenette-campaign -L STAMPS`

## Running APIs

### Accounts API

`poetry run cosmos api --port 8000 --reload accounts`

### Transactions API

`poetry run cosmos api --port 8000 --reload transactions`
