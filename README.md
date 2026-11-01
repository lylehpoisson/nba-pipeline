# NBA Pipeline 🏀

End-to-end data pipeline pulling NBA game logs from the public `nba_api` into Snowflake, transforming them with dbt, and orchestrated with Dagster on a nightly schedule. A RAG layer answers natural language questions about teams and players.

**Stack:** Python · nba_api · Snowflake · dbt-core · Dagster · Gemini embeddings · Claude

## Architecture

```
nba_api (free, no key)
    └─► Dagster (nightly schedule, 9am UTC)
            └─► ingestion/pipeline/load.py
                    └─► NBA_DB.RAW           (3 raw tables, loaded via stage + COPY INTO)
                            └─► dbt run
                                    ├─► DBT_DEV_STAGING   (incremental tables — type-cast + renamed)
                                    └─► DBT_DEV_MARTS     (tables — aggregated, ranked)

DBT_DEV_MARTS (on demand, not part of the nightly job)
    └─► ingestion/pipeline/rag.py
            └─► NBA_DB.RAW.EMBEDDINGS   (Gemini embeddings of team and player summaries)
                    └─► question → Snowflake vector search → answer from Claude
```

## Repo layout

```
nba_pipeline/
├── ingestion/
│   ├── pyproject.toml          # Makes `pipeline` an installable package
│   ├── pipeline/
│   │   ├── cli.py              # Entry point: load, embed, and query commands
│   │   ├── load.py             # Extracts from nba_api, loads RAW layer
│   │   └── rag.py              # Generates embeddings, answers questions
│   └── sql/
│       ├── create_raw_teams.sql
│       ├── create_raw_team_game_logs.sql
│       ├── create_raw_player_game_logs.sql
│       └── create_embeddings.sql
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Reads SNOWFLAKE_PASSWORD from env
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_team_game_logs.sql
│       │   └── stg_player_game_logs.sql
│       └── marts/
│           ├── mart_team_season_stats.sql
│           └── mart_player_season_stats.sql
├── dagster_project/
│   └── nba_pipeline/
│       └── nba_pipeline/
│           ├── assets.py       # Raw ingestion and embeddings assets (Python)
│           ├── dbt_assets.py   # dbt models as Dagster assets
│           └── definitions.py  # Wires assets, resources, schedule
├── start.ps1                   # Windows: loads .env, parses dbt, starts Dagster
├── requirements.in             # Top-level deps (compile with pip-compile)
├── requirements.txt
├── ROADMAP.md
└── LICENSE
```

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
cd nba_pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e ingestion
```

### 2. Configure credentials

Create a `.env` file in the repo root:

```ini
SNOWFLAKE_ACCOUNT=your-account-id
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=NBA_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
DBT_SCHEMA=DBT_DEV
GOOGLE_API_KEY=your-google-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
```

`SNOWFLAKE_SCHEMA` is the raw ingestion target; `DBT_SCHEMA` is the schema dbt writes transformed models into. `dbt_project/profiles.yml` reads all connection fields from the environment. `GOOGLE_API_KEY` and `ANTHROPIC_API_KEY` are only needed for the RAG layer.

### 3. Load raw data manually (optional)

```bash
python -m pipeline.cli load                    # defaults to 2025-26
python -m pipeline.cli load --season 2023-24   # any prior season
```

This creates `NBA_DB.RAW` in Snowflake and loads three tables:

| Table | Description | ~Rows |
|---|---|---|
| `RAW_TEAMS` | Static team reference (id, name, abbreviation, city) | 30 |
| `RAW_TEAM_GAME_LOGS` | One row per team per game — full box score | 2,460 |
| `RAW_PLAYER_GAME_LOGS` | One row per player per game — full box score | 26,000+ |

### 4. Run dbt manually (optional)

```bash
cd dbt_project
dbt debug          # verify Snowflake connection
dbt run            # build all models
dbt test           # run data quality tests
dbt docs generate
dbt docs serve     # browse lineage in browser
```

### 5. Run with Dagster

```bash
# Set DAGSTER_HOME for persistent run history
export DAGSTER_HOME=~/.dagster_home   # Windows: $env:DAGSTER_HOME = "C:\dagster_home"

cd dagster_project/nba_pipeline
dagster dev
```

On Windows, you can run `.\start.ps1` from the repo root instead. It loads `.env`, regenerates the dbt manifest, and starts Dagster.

Open http://127.0.0.1:3000. The pipeline runs on a nightly schedule at 9am UTC (after all NBA games have finished). The nightly job covers the raw and dbt assets but not `embeddings`. To trigger a manual run of everything, go to Catalog → select all assets → Materialize all.

### 6. Query with the RAG layer

Build the marts first, then generate embeddings and ask a question:

```bash
python -m pipeline.cli embed
python -m pipeline.cli query "Which teams have the best home records?"
python -m pipeline.cli query "Who are the most efficient scorers?" --top-k 10
```

`embed` only re-embeds teams and players whose summary text has changed. `query` retrieves the closest summaries from `NBA_DB.RAW.EMBEDDINGS` (5 by default) and has Claude answer from them.

## Data models

### Staging (incremental tables in `DBT_DEV_STAGING`)

Each run only processes games newer than the latest `game_date` already in the table. Use `dbt run --full-refresh` to rebuild from scratch.

| Model | Source table | Unique key | What it does |
|---|---|---|---|
| `stg_team_game_logs` | `RAW_TEAM_GAME_LOGS` | `game_id`, `team_id` | Types all columns, derives `season` from `SEASON_ID`, adds `home_away` flag |
| `stg_player_game_logs` | `RAW_PLAYER_GAME_LOGS` | `game_id`, `player_id` | Types all columns, excludes DNPs |

### Marts (tables in `DBT_DEV_MARTS`)

| Model | Description |
|---|---|
| `mart_team_season_stats` | Season aggregates per team: W/L record, win %, per-game shooting/rebounding/playmaking averages, home/away splits, and win % rank |
| `mart_player_season_stats` | Season aggregates per player: PPG/RPG/APG, true shooting %, possession actions per game, fantasy points average, PPG rank and TS% rank (min 10 games) |

## Updating dependencies

Top-level deps are in `requirements.in`. To recompile the pinned lockfile:

```bash
pip install pip-tools
pip-compile requirements.in
```

## Roadmap

**Recently shipped:**

- **Incremental dbt models** — `stg_team_game_logs` and `stg_player_game_logs` now process new games only instead of rebuilding on every run
- **RAG layer** — natural language queries over team and player season summaries, using Gemini embeddings, Snowflake vector search, and Claude for answers

See [ROADMAP.md](ROADMAP.md) for what's next.