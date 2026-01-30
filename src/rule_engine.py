import os
from typing import Optional

import yaml

from src.models import Rule, AssemblyContext, JoinAdjustment, WrapAdjustment


class RuleEngine:
    def __init__(self, rules_dir: str = "rules"):
        self.rules_dir = rules_dir
        self.rules: list[Rule] = []

    def load(self):
        self.rules = []
        if not os.path.isdir(self.rules_dir):
            return
        for fname in sorted(os.listdir(self.rules_dir)):
            if not fname.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(self.rules_dir, fname)
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "rule" in data:
                self.rules.append(Rule.from_dict(data))

    def match(
        self,
        market: Optional[str] = None,
        metric_tags: Optional[list[str]] = None,
        query_date_start: Optional[str] = None,
    ) -> list[Rule]:
        matched = []
        for rule in self.rules:
            if self._evaluate(rule, market, metric_tags or [], query_date_start):
                matched.append(rule)
        return matched

    def _evaluate(
        self,
        rule: Rule,
        market: Optional[str],
        metric_tags: list[str],
        query_date_start: Optional[str],
    ) -> bool:
        when = rule.when

        # Check market condition
        if "market" in when:
            expected = when["market"]
            if isinstance(expected, list):
                if market not in expected:
                    return False
            else:
                if market != expected:
                    return False

        # Check metric_tags condition (all listed tags must be present)
        if "metric_tags" in when:
            required_tags = set(when["metric_tags"])
            if not required_tags.issubset(set(metric_tags)):
                return False

        # Check temporal condition
        if "date_range_after" in when:
            if query_date_start and query_date_start < when["date_range_after"]:
                return False

        # Check valid_from on the rule itself
        if rule.valid_from:
            if query_date_start and query_date_start < str(rule.valid_from):
                return False

        return True

    def build_context(
        self,
        base_snippet: str,
        matched_rules: list[Rule],
    ) -> AssemblyContext:
        joins = []
        filters = []
        columns = []
        wrappers = []

        for rule in matched_rules:
            snippet_content = ""
            if rule.snippet_file:
                with open(rule.snippet_file) as f:
                    snippet_content = f.read()

            if rule.effect_type == "left_join":
                joins.append(JoinAdjustment(
                    name=rule.name,
                    snippet=snippet_content,
                    join_keys=rule.join_keys,
                ))
            elif rule.effect_type == "filter":
                filters.append(rule.clause or "")
            elif rule.effect_type == "column":
                columns.append(rule.clause or "")
            elif rule.effect_type == "wrap":
                wrappers.append(WrapAdjustment(
                    name=rule.name,
                    snippet=snippet_content,
                    priority=rule.priority,
                ))

        wrappers.sort(key=lambda w: w.priority)

        return AssemblyContext(
            base_snippet=base_snippet,
            joins=joins,
            filters=filters,
            columns=columns,
            wrappers=wrappers,
        )
