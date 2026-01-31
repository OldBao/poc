# CLAUDE.md — S&R&A Metric SQL Agent

## Overview

LLM-based SQL generation for S&R&A metrics. Natural language → SQL via GPT-4o, guided by YAML metric definitions and reference SQL snippets.

## Quick Start

```bash
source ~/.zshrc 2>/dev/null || true   # loads OPENAI_API_KEY, OPENAI_BASE_URL
pip install -r requirements.txt
python -m src.agent                   # interactive CLI
```

## Testing

```bash
# Unit tests (no API key needed)
python -m pytest tests/ --ignore=tests/test_regression.py -v

# Live regression tests (requires OPENAI_API_KEY)
python -m pytest tests/test_regression.py -v -m live

# Auto-testing loop (3-phase: generate, execute, repair)
python -m src.autotest [--id CASE_ID] [--dry-run]
```

## Key Directories

| Path | Purpose |
|------|---------|
| `src/` | Core source code |
| `src/autotest/` | Auto-testing loop (loader, comparator, repairer, runner) |
| `src/importer/` | SQL/doc → YAML metric importer |
| `metrics/` | YAML metric definitions (knowledge base) |
| `snippets/` | Per-metric reference SQL snippets |
| `rules/` | Conditional adjustment rules (e.g. BR SCS credit) |
| `tests/` | Unit tests, regression tests, benchmark cases |
| `docs/plans/` | Design and implementation plans |
| `docs/FEATURELIST.md` | Feature checklist with completion tracking |

## Plan Management

Plans live in `docs/plans/` and track feature lifecycle:

```
docs/plans/
├── *.md                    # Active/pending plans
├── implemented/            # Completed and merged plans
└── superseded/             # Plans replaced by newer approaches
```

**When finishing a feature branch**, update `docs/FEATURELIST.md` (mark done, add date/branch) and move its plan to the appropriate subfolder:
- `implemented/` — feature is fully merged and working
- `superseded/` — approach was replaced by a different design

This prevents confusion when multiple features are in flight.

### Current Status

| Plan | Status | Location |
|------|--------|----------|
| LLM SQL Generation | Implemented | `implemented/` |
| Multi-Turn Clarification | Implemented | `implemented/` |
| Auto-Testing Loop | Implemented | `implemented/` |
| Semantic Layer (deterministic) | Superseded by LLM approach | `superseded/` |
| Semantic Layer Agent Design | Design doc | `docs/plans/` |
| Hybrid Pipeline Design | Design doc | `docs/plans/` |
| LLM SQL Generation Design | Design doc | `docs/plans/` |
| Auto-Testing Loop Design | Design doc | `docs/plans/` |

## Architecture Notes

- **Agent** (`src/agent.py`): Multi-turn conversation agent, maintains message history
- **PromptBuilder** (`src/prompt_builder.py`): Assembles system prompt from YAML metrics + SQL snippets + rules
- **SQLValidator** (`src/validator.py`): Validates LLM-generated SQL (syntax, tables, filter values). Extracts allowed tables from both `sources:` blocks and `snippet_file` SQL.
- **MetricRegistry** (`src/registry.py`): Loads YAML metric definitions into `MetricDefinition` models
- **ValueIndex** (`src/value_index.py`): SQLite index of valid dimension values for filter validation
- **LLMClient** (`src/llm_client.py`): OpenAI API wrapper (supports custom base_url for Ollama/Shopee Compass)

## Adding Metrics

1. Create YAML in `metrics/` (see existing files for format)
2. Optionally add reference SQL in `snippets/`
3. System prompt auto-updates on next agent startup
4. Add regression cases to `tests/test_cases.yaml`
5. Run regression suite to verify
