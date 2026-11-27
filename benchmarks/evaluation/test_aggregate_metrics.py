"""Checks for denominator, evidence, and error handling; no external API calls."""
import json
from pathlib import Path
import tempfile
import unittest

import aggregate_metrics as m


def task(**changes):
    value = {"kind": "task", "case_id": "case-a", "expected_tools": ["lookup"],
             "actual_tools": ["lookup"], "is_stop_case": False,
             "tool_calls": [{"tool_id": "lookup", "extraction_attempted": True,
                             "parameters": {"id": 3}}]}
    value.update(changes)
    return value


INVENTORY = [{"tool_id": "lookup", "required": ["id"],
              "properties": {"id": {"type": "integer"}}}]


class AggregationTests(unittest.TestCase):
    def test_zero_denominator_is_not_success(self):
        self.assertIsNone(m.ratio(0, 0)["value"])

    def test_boolean_is_not_integer(self):
        self.assertFalse(m.json_type(True, "integer"))
        self.assertTrue(m.json_type(1, "integer"))

    def test_f1_counts_extra_tool(self):
        row = task(actual_tools=["lookup", "unexpected"])
        self.assertAlmostEqual(m.copilot([row], INVENTORY)["tool_selection_macro_f1"]["value"], 2 / 3)

    def test_empty_extraction_counts_missing_required_field(self):
        row = task(tool_calls=[{"tool_id": "lookup", "extraction_attempted": True, "parameters": {}}])
        result = m.copilot([row], INVENTORY)
        self.assertEqual(result["required_parameter_completeness"]["value"], 0)
        self.assertIsNone(result["parameter_type_correctness"]["value"])

    def test_stop_requires_matching_reason_and_no_side_effect(self):
        row = task(is_stop_case=True, expected_stop_reason="cancelled", actual_stop_reason="cancelled",
                   status="stopped", unauthorized_side_effects=1)
        self.assertEqual(m.copilot([row], INVENTORY)["correct_stop_rate"]["value"], 0)

    def test_duplicate_task_rejected(self):
        with self.assertRaises(m.InvalidRecord):
            m.copilot([task(), task()], INVENTORY)

    def test_multigold_recall_differs_from_hit_rate(self):
        row = {"kind": "rag", "case_id": "q1", "gold_evidence_ids": ["a", "b"],
               "retrieved_ids": ["a", "c"]}
        result = m.hr([row])
        self.assertEqual(result["recall_at_5"]["value"], 0.5)
        self.assertEqual(result["hit_rate_at_5"]["value"], 1)
        self.assertEqual(result["ragas_answer_correctness"]["count"], 0)

    def test_invalid_boolean_rejected(self):
        with self.assertRaises(m.InvalidRecord):
            m.boolean("false", "flag")

    def test_nonfinite_score_rejected(self):
        with self.assertRaises(m.InvalidRecord):
            m.score(float("nan"), "score")

    def test_missing_evidence_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "records.jsonl"
            path.write_text(json.dumps({"record_id": "r", "kind": "rag", "case_id": "q",
                                        "source_artifact": "missing.txt"}), encoding="utf-8")
            with self.assertRaises(m.InvalidRecord):
                m.load_records(path)

    def test_junit_skips_do_not_count_as_passed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            path.write_text('<testsuite><testcase classname="A" name="one"/>'
                            '<testcase classname="A" name="two"><skipped/></testcase>'
                            '</testsuite>', encoding="utf-8")
            result = m.junit_counts(path)
            self.assertEqual(result["passed"], 1)
            self.assertEqual(result["skipped"], 1)
            self.assertFalse(result["all_passed"])

    def test_plan_score_and_citation_labels_remain_separate(self):
        plan = {"kind": "plan", "case_id": "plan1", "topic_id": "t1", "judge_model": "judge",
                "rubric_version": "v1", "dimension_scores": {"requirement_coverage": 5,
                "question_clarity": 4, "structure_quality": 3}}
        citation = {"kind": "citation", "case_id": "ref1", "topic_id": "t1", "claim": "claim",
                    "source_url": "https://example.com/source", "source_excerpt": "evidence",
                    "reason": "partial support", "support_label": "partial"}
        result = m.deepresearch([plan, citation])
        self.assertEqual(result["plan_quality_out_of_5"]["value"], 4)
        self.assertEqual(result["citation_support_rate"]["value"], 0)


if __name__ == "__main__":
    unittest.main()
