"""Aggregate evidence-linked evaluation observations. Standard library only.

This program never calls a model or a production API. It computes results from
JSONL observations and, for regression tests, a pytest JUnit XML report.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import xml.etree.ElementTree as ET


class InvalidRecord(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidRecord(message)


def boolean(value, name):
    require(type(value) is bool, f"{name} must be a JSON boolean")
    return value


def score(value, name, low=0.0, high=1.0):
    require(type(value) in (int, float), f"{name} must be numeric")
    require(math.isfinite(value) and low <= value <= high, f"{name} out of range")
    return float(value)


def ratio(numerator, denominator):
    return {"numerator": numerator, "denominator": denominator,
            "value": numerator / denominator if denominator else None}


def average(values):
    return {"count": len(values), "value": sum(values) / len(values) if values else None}


def strings(value, name):
    require(isinstance(value, list) and all(isinstance(x, str) and x for x in value),
            f"{name} must be a list of nonempty strings")
    require(len(value) == len(set(value)), f"{name} contains duplicates")
    return set(value)


def load_records(path):
    records, ids = [], set()
    for line_no, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        require(isinstance(row, dict), f"line {line_no}: record must be an object")
        for field in ("record_id", "kind", "case_id", "source_artifact"):
            require(isinstance(row.get(field), str) and row[field].strip(),
                    f"line {line_no}: missing {field}")
        require(row["record_id"] not in ids, f"duplicate record_id: {row['record_id']}")
        ids.add(row["record_id"])
        # Each observation links an actual raw response, trace, or audit record.
        source = (path.parent / row["source_artifact"]).resolve()
        require(source.is_file(), f"missing evidence artifact: {row['source_artifact']}")
        require(bool(source.stat().st_size), "evidence artifact is empty")
        row["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        records.append(row)
    require(records, "no observation records")
    return records


def hr(records):
    groups = {"agent": [], "rag": []}
    seen = set()
    for r in records:
        require(r["kind"] in groups, f"unsupported HR kind: {r['kind']}")
        key = (r["kind"], r["case_id"], r.get("repeat_index", 1))
        require(key not in seen, f"duplicate observation: {key}")
        seen.add(key)
        groups[r["kind"]].append(r)
    agents, rags = groups["agent"], groups["rag"]
    intent = confirmation = contracts = 0
    for r in agents:
        require(type(r.get("repeat_index")) is int and 1 <= r["repeat_index"] <= 3,
                "repeat_index must be 1, 2, or 3")
        require(isinstance(r.get("expected_intent"), str) and r["expected_intent"],
                "expected_intent is required")
        require(isinstance(r.get("actual_intent"), str), "actual_intent is required")
        intent += r["expected_intent"] == r["actual_intent"]
        expected = boolean(r.get("expected_confirmation"), "expected_confirmation")
        actual = boolean(r.get("actual_confirmation"), "actual_confirmation")
        confirmation += expected == actual
        checks = r.get("contract_checks")
        require(isinstance(checks, dict) and checks, "contract_checks must not be empty")
        contracts += all([boolean(v, f"contract_checks.{k}") for k, v in checks.items()])
    recalls, hit_count, correctness = [], 0, []
    for r in rags:
        gold = strings(r.get("gold_evidence_ids"), "gold_evidence_ids")
        require(gold, "RAG gold evidence must not be empty")
        ids = r.get("retrieved_ids")
        strings(ids, "retrieved_ids")
        overlap = len(gold & set(ids[:5]))
        recalls.append(overlap / len(gold))
        hit_count += overlap > 0
        if r.get("ragas_answer_correctness") is not None:
            require(r.get("ragas_version") and r.get("judge_model"),
                    "RAGAS version and judge model required for correctness scores")
            correctness.append(score(r["ragas_answer_correctness"], "ragas_answer_correctness"))
    ids = {r["case_id"] for r in agents}
    repeat_sets = {case: {r["repeat_index"] for r in agents if r["case_id"] == case} for case in ids}
    return {"agent_cases": len(ids), "agent_runs": len(agents),
            "three_repeats_per_case": bool(ids) and all(v == {1, 2, 3} for v in repeat_sets.values()),
            "intent_accuracy": ratio(intent, len(agents)),
            "confirmation_accuracy": ratio(confirmation, len(agents)),
            "output_contract_pass_rate": ratio(contracts, len(agents)),
            "rag_samples": len(rags), "recall_at_5": average(recalls),
            "hit_rate_at_5": ratio(hit_count, len(rags)),
            "ragas_answer_correctness": average(correctness)}


def json_type(value, expected):
    if expected == "integer":
        return type(value) is int
    if expected == "number":
        return type(value) in (int, float) and math.isfinite(value)
    types = {"string": str, "boolean": bool, "array": list, "object": dict}
    require(expected in types, f"unsupported JSON type: {expected}")
    return type(value) is types[expected]


def copilot(records, inventory):
    require(isinstance(inventory, list), "tool inventory must be a list")
    names = strings([t.get("tool_id") for t in inventory], "inventory tool IDs")
    schemas = {t["tool_id"]: t for t in inventory}
    f1s, required, present, typed, correct = [], 0, 0, 0, 0
    stops = stop_ok = 0
    seen = set()
    for r in records:
        require(r["kind"] == "task", "Copilot records must have kind=task")
        require(r["case_id"] not in seen, "duplicate task case")
        seen.add(r["case_id"])
        expected = strings(r.get("expected_tools"), "expected_tools")
        actual = strings(r.get("actual_tools"), "actual_tools")
        require(expected <= names, "expected tools are missing from inventory")
        # Unknown actual tools count as false positives; they are not discarded.
        if expected or actual:
            tp = len(expected & actual)
            denominator = len(expected) + len(actual)
            f1s.append(2 * tp / denominator)
        calls = r.get("tool_calls")
        require(isinstance(calls, list), "tool_calls must be an array")
        for call in calls:
            name = call.get("tool_id")
            require(name in schemas, f"missing schema for actual tool: {name}")
            if not boolean(call.get("extraction_attempted"), "extraction_attempted"):
                continue
            params = call.get("parameters")
            require(isinstance(params, dict), "parameters must be an object")
            schema = schemas[name]
            for key in schema.get("required", []):
                required += 1
                present += key in params and params[key] is not None
            for key, value in params.items():
                require(key in schema.get("properties", {}), f"unknown parameter: {name}.{key}")
                typed += 1
                correct += json_type(value, schema["properties"][key]["type"])
        if boolean(r.get("is_stop_case"), "is_stop_case"):
            stops += 1
            require(isinstance(r.get("expected_stop_reason"), str) and r["expected_stop_reason"],
                    "expected_stop_reason required")
            ok = (r.get("actual_stop_reason") == r["expected_stop_reason"]
                  and r.get("status") == "stopped"
                  and type(r.get("unauthorized_side_effects")) is int
                  and r["unauthorized_side_effects"] == 0)
            stop_ok += ok
    return {"tool_inventory_count": len(names), "task_cases": len(records),
            "tool_selection_macro_f1": average(f1s),
            "required_parameter_completeness": ratio(present, required),
            "parameter_type_correctness": ratio(correct, typed),
            "stop_cases": stops, "correct_stop_rate": ratio(stop_ok, stops)}


def junit_counts(path):
    root = ET.parse(path).getroot()
    tests = root.findall(".//testcase")
    require(tests, "JUnit XML has no test cases")
    identities = [(t.get("classname"), t.get("name")) for t in tests]
    require(len(identities) == len(set(identities)), "JUnit contains duplicate test IDs")
    failed = sum(t.find("failure") is not None for t in tests)
    errors = sum(t.find("error") is not None for t in tests)
    skipped = sum(t.find("skipped") is not None for t in tests)
    return {"collected": len(tests), "passed": len(tests) - failed - errors - skipped,
            "failed": failed, "errors": errors, "skipped": skipped,
            "all_passed": not (failed or errors or skipped)}


def deepresearch(records, junit=None):
    topics, citations, plans, ids, plan_topics = set(), [], [], set(), set()
    for r in records:
        require(r["kind"] in {"citation", "plan"}, "unsupported research record")
        topic = r.get("topic_id")
        require(isinstance(topic, str) and topic, "topic_id required")
        key = (r["kind"], topic, r["case_id"])
        require(key not in ids, "duplicate topic/observation")
        ids.add(key)
        topics.add(topic)
        if r["kind"] == "citation":
            for key in ("claim", "source_url", "source_excerpt", "reason"):
                require(isinstance(r.get(key), str) and r[key].strip(), f"missing {key}")
            require(r.get("support_label") in {"supported", "partial", "unsupported", "unavailable"},
                    "invalid support_label")
            citations.append(r["support_label"])
        else:
            require(topic not in plan_topics, "one final-plan score per topic is required")
            plan_topics.add(topic)
            require(r.get("judge_model") and r.get("rubric_version"), "plan judge metadata required")
            dimensions = r.get("dimension_scores")
            require(isinstance(dimensions, dict) and set(dimensions) == {
                "requirement_coverage", "question_clarity", "structure_quality"},
                "plan requires the three documented dimensions")
            plans.append(sum(score(v, k, 1, 5) for k, v in dimensions.items()) / 3)
    result = {"research_topics": len(topics), "plan_topics": len(plan_topics),
              "citation_count": len(citations),
              "citation_support_rate": ratio(citations.count("supported"), len(citations)),
              "citation_labels": {k: citations.count(k) for k in
                  ("supported", "partial", "unsupported", "unavailable")},
              "plan_quality_out_of_5": average(plans)}
    result["engineering_regression"] = junit_counts(junit) if junit else None
    return result


def scope_checks(project, result, scope):
    if project == "hr":
        return {"agent_cases": result["agent_cases"] == scope["agent_cases"],
                "agent_runs": result["agent_runs"] == scope["agent_cases"] * scope["repeat_runs"],
                "repeat_coverage": result["three_repeats_per_case"],
                "rag_samples": result["rag_samples"] == scope["rag_samples"],
                "ragas_scores": result["ragas_answer_correctness"]["count"] == scope["rag_samples"]}
    if project == "copilot":
        return {"tool_inventory": result["tool_inventory_count"] == scope["business_tools"],
                "task_cases": result["task_cases"] == scope["task_cases"],
                "stop_cases": result["stop_cases"] == scope["stop_cases"]}
    regression = result["engineering_regression"]
    return {"research_topics": result["research_topics"] == scope["research_topics"],
            "plan_topics": result["plan_topics"] == scope["research_topics"],
            "citation_count": result["citation_count"] == scope["citation_units"],
            "regression_count": bool(regression) and regression["collected"] == scope["regression_instances"]}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--project", required=True, choices=("hr", "copilot", "deepresearch"))
    cli.add_argument("--records", required=True, type=Path)
    cli.add_argument("--inventory", type=Path)
    cli.add_argument("--junit", type=Path)
    cli.add_argument("--output", required=True, type=Path)
    cli.add_argument("--protocol", type=Path, default=Path(__file__).with_name("protocol.json"))
    cli.add_argument("--strict-scope", action="store_true", help="Reject incomplete protocol scope")
    args = cli.parse_args()
    try:
        records = load_records(args.records)
        if args.project == "hr":
            result = hr(records)
        elif args.project == "copilot":
            require(args.inventory is not None, "Copilot requires --inventory")
            result = copilot(records, json.loads(args.inventory.read_text(encoding="utf-8-sig")))
        else:
            result = deepresearch(records, args.junit)
        protocol = json.loads(args.protocol.read_text(encoding="utf-8-sig"))
        require(protocol["project"] == args.project, "protocol project mismatch")
        result["scope_checks"] = scope_checks(args.project, result, protocol["scope"])
        result["scope_complete"] = all(result["scope_checks"].values())
        require(not args.strict_scope or result["scope_complete"], "observation coverage does not match protocol")
        result["protocol_version"] = protocol["version"]
        result["input_provenance"] = {
            "records_sha256": hashlib.sha256(args.records.read_bytes()).hexdigest(),
            "protocol_sha256": hashlib.sha256(args.protocol.read_bytes()).hexdigest(),
            "record_count": len(records),
            "source_artifact_hashes": sorted(set(r["source_sha256"] for r in records))}
        if args.inventory:
            result["input_provenance"]["inventory_sha256"] = hashlib.sha256(args.inventory.read_bytes()).hexdigest()
        if args.junit:
            result["input_provenance"]["junit_sha256"] = hashlib.sha256(args.junit.read_bytes()).hexdigest()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        print(f"Wrote {args.output}")
    except (InvalidRecord, OSError, ValueError, KeyError, TypeError, ET.ParseError) as exc:
        cli.exit(2, f"Evaluation input rejected: {exc}\n")


if __name__ == "__main__":
    main()
