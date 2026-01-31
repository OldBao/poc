import argparse
import sys

from src.agent import Agent
from src.llm_backend import create_backend
from src.autotest.loader import BenchmarkLoader
from src.autotest.comparator import StructuralComparator, ResultComparator
from src.autotest.repairer import Repairer
from src.autotest.runner import Runner, RunSummary
from src.query_service import QueryService


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Auto-testing loop for S&R&A Metric SQL Agent"
    )
    parser.add_argument(
        "--benchmark", default="tests/benchmark.yaml",
        help="Path to benchmark YAML (default: tests/benchmark.yaml)",
    )
    parser.add_argument("--tags", default=None, help="Comma-separated tags to filter")
    parser.add_argument("--id", default=None, help="Run a single case by ID")
    parser.add_argument(
        "--max-retries", type=int, default=3,
        help="Per-case retry cap (default: 3)",
    )
    parser.add_argument(
        "--max-llm-calls", type=int, default=50,
        help="Global LLM call budget (default: 50)",
    )
    parser.add_argument(
        "--no-repair", action="store_true",
        help="Evaluation only — skip repair loop",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show repair plans without applying changes",
    )
    parser.add_argument(
        "--backend", default="openai", choices=["openai", "claude"],
        help="LLM backend (default: openai)",
    )
    return parser.parse_args(argv)


def print_summary(summary: RunSummary):
    print("\n" + "=" * 40)
    print(f"=== Auto-Test Results ===")
    print(f"Total: {summary.total} | Passed: {summary.passed} | Repaired: {summary.repaired} | Failed: {summary.failed}")
    print()

    repaired = [r for r in summary.results if r.passed and r.retries > 0]
    if repaired:
        print("Repaired:")
        for r in repaired:
            print(f"  ✓ {r.case_id} ({r.retries} retries)")
            for plan in r.repair_plans:
                for action in plan.actions:
                    print(f"    - {action.type}: {action.file}")
        print()

    failed = [r for r in summary.results if not r.passed]
    if failed:
        print("Failed (needs human review):")
        for r in failed:
            print(f"  ✗ {r.case_id} ({r.retries} retries exhausted)")
            if r.error:
                print(f"    - {r.error}")
        print()


def main():
    args = parse_args()

    # Load benchmark cases
    loader = BenchmarkLoader(args.benchmark)
    tags = args.tags.split(",") if args.tags else None
    cases = loader.load(tags=tags, case_id=args.id)

    if not cases:
        print("No benchmark cases found.")
        sys.exit(1)

    print(f"Running {len(cases)} benchmark case(s)...")

    # Set up components
    backend = create_backend(args.backend)
    agent = Agent(backend=backend)
    structural = StructuralComparator(backend=backend)
    result_comp = ResultComparator()
    query_service = QueryService()  # TODO: configure base_url and token
    repairer = Repairer(backend=backend)

    runner = Runner(
        agent=agent,
        structural_comparator=structural,
        result_comparator=result_comp,
        query_service=query_service,
        repairer=repairer,
        max_retries=args.max_retries,
        max_llm_calls=args.max_llm_calls,
        no_repair=args.no_repair,
        dry_run=args.dry_run,
    )

    summary = runner.run_all(cases)
    print_summary(summary)

    # Prompt for commit if there were repairs
    if summary.repaired > 0 and not args.dry_run:
        print("KB changes pending confirmation.")
        answer = input("Commit these changes? [y/n] ").strip().lower()
        if answer == "y":
            import subprocess
            subprocess.run(["git", "add", "metrics/", "snippets/"], check=True)
            subprocess.run(
                ["git", "commit", "-m", "fix(kb): auto-repair from autotest loop"],
                check=True,
            )
            print("Changes committed.")
        else:
            print("Changes left unstaged. Review manually.")


if __name__ == "__main__":
    main()
