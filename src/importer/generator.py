import os
import yaml


class Generator:
    def __init__(self, metrics_dir: str = "metrics", snippets_dir: str = "snippets"):
        self.metrics_dir = metrics_dir
        self.snippets_dir = snippets_dir

    def generate(self, analyzed_metrics: list[dict]) -> list[str]:
        os.makedirs(self.metrics_dir, exist_ok=True)
        os.makedirs(self.snippets_dir, exist_ok=True)

        created_files = []
        for m in analyzed_metrics:
            slug = m["name"].lower().replace(" ", "_")

            yaml_data = self._build_yaml(m)
            yaml_path = os.path.join(self.metrics_dir, f"{slug}.yaml")
            with open(yaml_path, "w") as f:
                yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)
            created_files.append(yaml_path)

            snippet = m.get("snippet")
            if snippet:
                snippet_path = os.path.join(self.snippets_dir, f"{slug}.sql")
                with open(snippet_path, "w") as f:
                    f.write(snippet.strip() + "\n")
                created_files.append(snippet_path)

        return created_files

    def _build_yaml(self, m: dict) -> dict:
        metric = {
            "name": m["name"],
            "aliases": m.get("aliases", []),
            "type": m["type"],
            "dimensions": {"required": ["market", "date_range"], "optional": []},
        }
        if m.get("aggregation"):
            metric["aggregation"] = m["aggregation"]
        if m.get("table"):
            source = {
                "id": m["name"].lower().replace(" ", "_"),
                "layer": "dws",
                "table": m["table"],
                "columns": m.get("columns", {}),
                "filters": m.get("filters", []),
                "use_when": {"granularity": ["platform"]},
            }
            metric["sources"] = [source]
        if m.get("snippet"):
            metric["snippet_file"] = f"snippets/{m['name'].lower().replace(' ', '_')}.sql"
        if m.get("notes"):
            metric["notes"] = m["notes"]
        return {"metric": metric}
