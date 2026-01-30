import argparse
import sys
from src.importer.analyzer import SQLAnalyzer
from src.importer.generator import Generator


def main():
    parser = argparse.ArgumentParser(description="Import raw SQL/docs into metric KB")
    parser.add_argument("input_file", help="Path to SQL file or text doc")
    parser.add_argument("--type", choices=["sql", "doc"], default="sql", help="Input type")
    parser.add_argument("--metrics-dir", default="metrics", help="Output metrics directory")
    parser.add_argument("--snippets-dir", default="snippets", help="Output snippets directory")
    parser.add_argument("--dry-run", action="store_true", help="Print analysis without writing files")
    args = parser.parse_args()

    with open(args.input_file) as f:
        content = f.read()

    analyzer = SQLAnalyzer()
    if args.type == "sql":
        results = analyzer.analyze_sql(content)
    else:
        results = analyzer.analyze_doc(content)

    if args.dry_run:
        import json
        print(json.dumps(results, indent=2))
        return

    generator = Generator(metrics_dir=args.metrics_dir, snippets_dir=args.snippets_dir)
    created = generator.generate(results)
    print(f"Created {len(created)} files:")
    for f in created:
        print(f"  {f}")


if __name__ == "__main__":
    main()
