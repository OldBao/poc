# S&R&A Metric SQL Agent

LLM-based SQL generation for S&R&A metrics. Takes natural language questions and generates accurate SQL using GPT-4o, guided by a knowledge base of YAML metric definitions and reference SQL snippets.

## Setup

```bash
pip install -r requirements.txt
```

Set environment variables (or add to `~/.zshrc`):

```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_BASE_URL="https://compass.llm.shopee.io/compass-api/v1"
```

## Usage

### Interactive CLI

```bash
python -m src.agent
```

Ask questions in natural language:

```
Q: ID market DAU in November 2025
--- Generated SQL ---
SELECT grass_region, substr(cast(grass_date as varchar), 1, 7) AS period, avg(a1) AS dau
FROM traffic.shopee_traffic_dws_platform_active_churn_nd__reg_s0_live
WHERE grass_date BETWEEN date '2025-11-01' AND date '2025-11-30'
  AND tz_type = 'local' AND grass_region = 'ID'
GROUP BY 1, 2 ORDER BY 2 DESC

Q: What's the revenue?
Ambiguous request. Did you mean:
  1. Gross Ads Revenue (total ads revenue before deductions)
  2. Net Ads Revenue (after SCS credits and rebates)
Please rephrase with a specific metric.
```

Type `quit`, `exit`, or `q` to exit.

### Importer CLI

Import raw SQL files or docs into the metric knowledge base:

```bash
# Analyze a SQL file and generate draft YAML + snippet files
python -m src.importer.cli raw/monthly_core_metrics_tracker.sql --type sql

# Analyze a doc
python -m src.importer.cli path/to/doc.txt --type doc

# Dry run (print analysis without writing files)
python -m src.importer.cli raw/monthly_core_metrics_tracker.sql --dry-run
```

Options:
- `--type sql|doc` — Input type (default: `sql`)
- `--metrics-dir DIR` — Output directory for YAML metrics (default: `metrics/`)
- `--snippets-dir DIR` — Output directory for SQL snippets (default: `snippets/`)
- `--dry-run` — Print JSON analysis without writing files

## Testing

```bash
# Run unit tests (no API key needed)
python -m pytest tests/ --ignore=tests/test_regression.py -v

# Run regression suite (requires OPENAI_API_KEY)
python -m pytest tests/test_regression.py -v -m live
```

## Project Structure

```
sqlpoc/
├── src/
│   ├── agent.py            # CLI entry point
│   ├── prompt_builder.py   # Assembles system prompt from YAML + snippets
│   ├── llm_client.py       # OpenAI API wrapper
│   ├── registry.py         # YAML metric loader
│   ├── models.py           # Metric data models
│   └── importer/
│       ├── analyzer.py     # Parses raw SQL/docs via LLM
│       ├── generator.py    # Produces draft YAML + snippet files
│       └── cli.py          # Importer CLI
├── metrics/                # YAML metric definitions (knowledge base)
├── snippets/               # Per-metric reference SQL snippets
├── raw/                    # Raw input SQL files for importer
├── tests/
│   ├── test_cases.yaml     # Regression test cases
│   ├── test_regression.py  # Live API regression runner
│   └── test_*.py           # Unit tests
└── docs/plans/             # Design and implementation docs
```

## Adding Metrics

1. Create a YAML file in `metrics/` (see existing files for format)
2. Optionally add a reference SQL snippet in `snippets/`
3. The system prompt auto-updates on next agent startup
4. Add regression test cases to `tests/test_cases.yaml`
5. Run the regression suite to verify

Or use the importer to auto-generate drafts from raw SQL/docs, then review and commit.
