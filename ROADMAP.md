# Roadmap

An unprioritized list of ideas for the NBA pipeline.

## Data sources

- Multi-season backfill command (`cli.py backfill --start 2020-21 --end 2025-26`)
- Playoff and play-in games as a separate `season_type`
- Play-by-play data for lineups, clutch stats, and scoring runs
- Shot chart data with court coordinates
- Injury reports and player availability
- Schedule data for rest days, back-to-backs, and travel distance
- Player bio and roster history, including trades and mid-season team changes

## dbt models

- `mart_rolling_team_form`: last-10-games rolling window using Snowflake window functions
- `mart_player_rolling_form`: hot and cold streaks against season baselines
- Advanced team stats: pace, offensive and defensive rating, net rating, Four Factors
- Advanced player stats: usage rate, assist rate, rebound rate
- Head-to-head team matchup history
- Strength of schedule, overall and remaining
- Player home/away and rest-day splits
- Season-over-season player development
- Standings by conference and division, by date
- Team and player dimension tables, with slowly changing history via dbt snapshots

## Data quality

- Tests on marts, which currently have none (unique grain, not-null keys, accepted ranges)
- Reconciliation tests: player points sum to team points, and each game has two team rows
- `dbt source freshness` checks, with allowances for the off-season
- Row-count anomaly checks after each load
- Dagster asset checks, so that failures block downstream assets
- Unit tests for ingestion and RAG summary generation

## Orchestration and reliability

- Alert on failed runs via Slack or email
- Retries with backoff for `nba_api` timeouts and rate limits
- Season partitions in Dagster, for backfills and selective reruns
- Skip or reduce scheduled runs during the off-season
- Run the pipeline in Docker, so it doesn't depend on a local machine
- Deploy to Dagster Cloud or a small VM

## Engineering

- CI: lint, tests, and `dbt build` against a temporary schema on each PR
- Pre-commit hooks with `ruff` and `sqlfluff`
- Separate dev and prod Snowflake environments
- Least-privilege Snowflake role instead of `ACCOUNTADMIN`
- Key-pair authentication for Snowflake instead of a password
- Warehouse cost monitoring with auto-suspend and resource monitors
- Refresh the README layout, which predates the `ingestion/pipeline` package

## RAG and natural language queries

- Add `embeddings` to the nightly job, so that answers stay current
- Text-to-SQL for precise questions, with embeddings for fuzzy ones
- Query routing between the two approaches
- Evaluation set of questions with known answers, to catch regressions
- Citations that show which rows or summaries informed each answer
- Metadata filters for season, team, and player before similarity search
- Game recap summaries as a new embedded document type
- Replace the pickle embeddings cache with a Snowflake table or Parquet file
- Conversation history for follow-up questions

## Analytics and ML

- Game outcome predictions, with tracked accuracy over time
- Player points projections
- Player similarity search and historical comps
- Player archetype clustering
- Breakout and regression detection
- Fantasy 9-cat: player rankings by category, punt-strategy analysis, and waiver recommendations

## User-facing

- Streamlit dashboard for team and player stats
- Chat interface over the RAG layer
- Read-only API with FastAPI
- MCP server, so that AI assistants can query the data
- Daily digest of standout performances by email or Slack
- Public dbt docs site on GitHub Pages
