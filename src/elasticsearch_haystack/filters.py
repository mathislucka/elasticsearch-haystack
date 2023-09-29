from typing import Any, Dict, List, Union

import numpy as np
from pandas import DataFrame


def _normalize_filters(filters: Union[List[Dict], Dict], logical_condition="") -> Dict[str, Any]:
    conditions = []
    if isinstance(filters, dict):
        filters = [filters]
    for filter in filters:
        for operator, value in filter.items():
            if operator in ["$not", "$and", "$or"]:
                # Logical operators
                conditions.append(_normalize_filters(value, operator))
            else:
                # Comparison operators
                conditions.extend(_parse_comparison(operator, value))

    if len(conditions) == 1:
        return conditions[0]

    conditions = _normalize_ranges(conditions)

    if logical_condition == "$not":
        return {"bool": {"must_not": conditions}}
    elif logical_condition == "$and":
        return {"bool": {"must": conditions}}
    elif logical_condition == "$or":
        return {"bool": {"should": conditions}}
    return {"bool": {"must": conditions}}


def _parse_comparison(field: str, comparison: Union[Dict, List, str, float]) -> List:
    result = []
    if isinstance(comparison, dict):
        for comparator, val in comparison.items():
            if comparator == "$eq":
                if isinstance(val, list):
                    result.append(
                        {
                            "terms_set": {
                                field: {
                                    "terms": val,
                                    "minimum_should_match_script": {
                                        "source": f"Math.max(params.num_terms, doc['{field}'].size())"
                                    },
                                }
                            }
                        }
                    )
                result.append({"term": {field: val}})
            elif comparator == "$ne":
                if isinstance(val, list):
                    raise Exception("MUST NOT be a list")
                result.append({"bool": {"must_not": {"term": {field: val}}}})
            elif comparator == "$in":
                if not isinstance(val, list):
                    raise Exception("MUST be a list")
                result.append({"terms": {field: val}})
            elif comparator == "$nin":
                if not isinstance(val, list):
                    raise Exception("MUST be a list")
                result.append({"bool": {"must_not": {"terms": {field: val}}}})
            elif comparator == "$gt":
                if isinstance(val, list):
                    raise Exception("MUST NOT be a list")
                result.append({"range": {field: {"gt": val}}})
            elif comparator == "$gte":
                if isinstance(val, list):
                    raise Exception("MUST NOT be a list")
                result.append({"range": {field: {"gte": val}}})
            elif comparator == "$lt":
                if isinstance(val, list):
                    raise Exception("MUST NOT be a list")
                result.append({"range": {field: {"lt": val}}})
            elif comparator == "$lte":
                if isinstance(val, list):
                    raise Exception("MUST NOT be a list")
                result.append({"range": {field: {"lte": val}}})
    elif isinstance(comparison, list):
        result.append({"terms": {field: comparison}})
    elif isinstance(comparison, np.ndarray):
        result.append({"terms": {field: comparison.tolist()}})
    elif isinstance(comparison, DataFrame):
        result.append({"match": {field: comparison.to_json()}})
    elif isinstance(comparison, str):
        result.append({"match": {field: comparison}})
    else:
        result.append({"term": {field: comparison}})
    return result


def _normalize_ranges(conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    range_conditions = [cond["range"] for cond in conditions if "range" in cond]
    if range_conditions:
        conditions = [condition for condition in conditions if "range" not in condition]
        range_conditions_dict = {}
        for condition in range_conditions:
            field_name = list(condition.keys())[0]
            operation = list(condition[field_name].keys())[0]
            comparison_value = condition[field_name][operation]
            if field_name not in range_conditions_dict:
                range_conditions_dict[field_name] = {}
            range_conditions_dict[field_name][operation] = comparison_value

        for field_name, comparison_operations in range_conditions_dict.items():
            conditions.append({"range": {field_name: comparison_operations}})
    return conditions
