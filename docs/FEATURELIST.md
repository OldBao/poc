# Feature List

Tracks all features from design → implementation → done. Update when merging feature branches.

## Implementation Features

| # | Feature | Done | Date | Branch | Plan |
|---|---------|------|------|--------|------|
| 1 | Semantic layer (models, registry, YAML metrics) | [x] | 2026-01-30 | `refactor/single-conversation-agent` | [superseded](plans/superseded/2026-01-30-semantic-layer-implementation.md) |
| 2 | LLM-based SQL generation (prompt builder, llm client, agent) | [x] | 2026-01-30 | `refactor/single-conversation-agent` | [implemented](plans/implemented/2026-01-30-llm-sql-generation-impl.md) |
| 3 | SQL importer (analyzer, generator, CLI) | [x] | 2026-01-30 | `refactor/single-conversation-agent` | [implemented](plans/implemented/2026-01-30-llm-sql-generation-impl.md) |
| 4 | Multi-turn clarification (conversation history, ask/reset) | [x] | 2026-01-30 | `refactor/single-conversation-agent` | [implemented](plans/implemented/2026-01-30-multi-turn-clarification-impl.md) |
| 5 | Auto-testing loop (loader, comparator, repairer, runner) | [x] | 2026-01-30 | `refactor/single-conversation-agent` | [implemented](plans/implemented/2026-01-30-auto-testing-loop-impl.md) |
| 6 | SQL validator (syntax, tables, filter values) | [x] | 2026-01-30 | `composite-key` | — |
| 7 | Value index (SQLite dimension value store) | [x] | 2026-01-30 | `composite-key` | — |
| 8 | Rule engine (conditional adjustments, BR SCS credit) | [x] | 2026-01-30 | `composite-key` | — |
| 9 | Ollama / custom LLM endpoint support | [x] | 2026-01-30 | `composite-key` | — |
| 10 | Fix regression tests (snippet table validation, prompt fixes) | [x] | 2026-01-31 | `fix/test-regression` | — |
| 11 | 3-layer snippet architecture (source fragments, aggregation templates, composition templates) | [ ] | | | [design](plans/2026-01-31-snippet-granularity-design.md) |
| 12 | Self-correction + DB execution (2-retry loop with Presto/Hive/SparkSQL validation) | [ ] | | | [design](plans/2026-01-31-self-correction-and-fewshot-design.md) |
| 13 | Dynamic few-shot + auto-capture (ChromaDB retrieval, /save, /unsave, implicit save) | [ ] | | | [design](plans/2026-01-31-self-correction-and-fewshot-design.md) |

## Design Documents (not yet implemented)

| # | Design | Status | Plan |
|---|--------|--------|------|
| D1 | Semantic Layer Agent Design | Reference | [design](plans/2026-01-30-semantic-layer-agent-design.md) |
| D2 | Hybrid Pipeline Design | Reference | [design](plans/2026-01-30-hybrid-pipeline-design.md) |
| D3 | LLM SQL Generation Design | Reference | [design](plans/2026-01-30-llm-sql-generation-design.md) |
| D4 | Auto-Testing Loop Design | Reference | [design](plans/2026-01-30-auto-testing-loop-design.md) |
| D5 | Self-Correction + Dynamic Few-shot Design | Pending (#12, #13) | [design](plans/2026-01-31-self-correction-and-fewshot-design.md) |
| D6 | 3-Layer Snippet Architecture Design | Active (#11) | [design](plans/2026-01-31-snippet-granularity-design.md) |

## How to Update

When completing a feature:
1. Mark the row `[x]` with the completion date and branch name
2. Move the plan file to `docs/plans/implemented/` (or `superseded/`)
3. Add the plan link to the row
