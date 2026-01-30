from dataclasses import dataclass, field
from typing import Optional

from src.agent import Agent
from src.autotest.loader import BenchmarkCase
from src.autotest.comparator import (
    StructuralComparator,
    ResultComparator,
    CompareResult,
    ResultCompareResult,
)
from src.autotest.repairer import Repairer, RepairPlan
from src.query_service import QueryService


@dataclass
class CaseResult:
    case_id: str
    passed: bool
    retries: int = 0
    structural_result: Optional[CompareResult] = None
    result_result: Optional[ResultCompareResult] = None
    repair_plans: list[RepairPlan] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class RunSummary:
    total: int
    passed: int
    repaired: int
    failed: int
    results: list[CaseResult] = field(default_factory=list)


class Runner:
    def __init__(
        self,
        agent: Agent,
        structural_comparator: StructuralComparator,
        result_comparator: ResultComparator,
        query_service: QueryService,
        repairer: Repairer,
        max_retries: int = 3,
        max_llm_calls: int = 50,
        no_repair: bool = False,
        dry_run: bool = False,
    ):
        self.agent = agent
        self.structural = structural_comparator
        self.result_comp = result_comparator
        self.query_service = query_service
        self.repairer = repairer
        self.max_retries = max_retries
        self.max_llm_calls = max_llm_calls
        self.no_repair = no_repair
        self.dry_run = dry_run
        self._llm_calls = 0

    def run_case(self, case: BenchmarkCase) -> CaseResult:
        # Phase 1: Generate SQL
        response = self.agent.ask(case.question)
        self._llm_calls += 1

        if response.get("type") != "sql":
            msg = response.get("message", "Agent did not return SQL")
            return CaseResult(case_id=case.id, passed=False, error=msg)

        generated_sql = response["sql"]

        # Phase 1b: Structural compare
        struct_result = self.structural.compare(case.expected_sql, generated_sql)
        self._llm_calls += 1

        if struct_result.match:
            # Phase 2: Result compare
            result_outcome = self._run_result_compare(case.expected_sql, generated_sql)
            if result_outcome.match:
                return CaseResult(
                    case_id=case.id,
                    passed=True,
                    structural_result=struct_result,
                    result_result=result_outcome,
                )
            failure_context = f"Result mismatch: {result_outcome.row_mismatches} rows differ. {result_outcome.sample_diffs}"
        else:
            failure_context = f"Structural mismatch: {struct_result.differences}"
            result_outcome = None

        # Phase 3: Repair loop
        if self.no_repair:
            return CaseResult(
                case_id=case.id,
                passed=False,
                structural_result=struct_result,
                result_result=result_outcome,
                error=failure_context,
            )

        repair_plans = []
        for retry in range(self.max_retries):
            if self._llm_calls >= self.max_llm_calls:
                return CaseResult(
                    case_id=case.id,
                    passed=False,
                    retries=retry,
                    repair_plans=repair_plans,
                    error="Global LLM call budget exhausted",
                )

            plan = self.repairer.propose(
                question=case.question,
                expected_sql=case.expected_sql,
                generated_sql=generated_sql,
                failure_context=failure_context,
            )
            self._llm_calls += 1
            repair_plans.append(plan)

            if not self.dry_run:
                self.repairer.apply(plan)

            # Re-run
            response = self.agent.ask(case.question)
            self._llm_calls += 1

            if response.get("type") != "sql":
                if not self.dry_run:
                    self.repairer.revert(plan)
                continue

            generated_sql = response["sql"]

            struct_result = self.structural.compare(case.expected_sql, generated_sql)
            self._llm_calls += 1

            if struct_result.match:
                result_outcome = self._run_result_compare(
                    case.expected_sql, generated_sql
                )
                if result_outcome.match:
                    return CaseResult(
                        case_id=case.id,
                        passed=True,
                        retries=retry + 1,
                        structural_result=struct_result,
                        result_result=result_outcome,
                        repair_plans=repair_plans,
                    )
                failure_context = f"Result mismatch: {result_outcome.row_mismatches} rows differ. {result_outcome.sample_diffs}"
            else:
                failure_context = f"Structural mismatch: {struct_result.differences}"
                if not self.dry_run:
                    self.repairer.revert(plan)

        return CaseResult(
            case_id=case.id,
            passed=False,
            retries=self.max_retries,
            structural_result=struct_result,
            result_result=result_outcome,
            repair_plans=repair_plans,
            error=failure_context,
        )

    def _run_result_compare(
        self, expected_sql: str, generated_sql: str
    ) -> ResultCompareResult:
        exp_result = self.query_service.execute(expected_sql)
        gen_result = self.query_service.execute(generated_sql)
        return self.result_comp.compare(exp_result, gen_result)

    def run_all(self, cases: list[BenchmarkCase]) -> RunSummary:
        results = []
        for case in cases:
            result = self.run_case(case)
            results.append(result)

        passed = sum(1 for r in results if r.passed and r.retries == 0)
        repaired = sum(1 for r in results if r.passed and r.retries > 0)
        failed = sum(1 for r in results if not r.passed)

        return RunSummary(
            total=len(results),
            passed=passed,
            repaired=repaired,
            failed=failed,
            results=results,
        )
