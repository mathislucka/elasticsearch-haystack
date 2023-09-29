from typing import Any, Dict, List, Union

import numpy as np
from pandas import DataFrame
from haystack.preview.errors import FilterError


def _normalize_filters(filters: Union[List[Dict], Dict], logical_condition="") -> Dict[str, Any]:
    """
    Converts Haystack filters in ElasticSearch compatible filters.
    """
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
    elif logical_condition == "$or":
        return {"bool": {"should": conditions}}

    # If no logical condition is specified we default to "$and"
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
                    raise FilterError(f"{field}'s value can't be a list when using '{comparator}' comparator")
                result.append({"bool": {"must_not": {"term": {field: val}}}})
            elif comparator == "$in":
                if not isinstance(val, list):
                    raise FilterError(f"{field}'s value must be a list when using '{comparator}' comparator")
                result.append({"terms": {field: val}})
            elif comparator == "$nin":
                if not isinstance(val, list):
                    raise FilterError(f"{field}'s value must be a list when using '{comparator}' comparator")
                result.append({"bool": {"must_not": {"terms": {field: val}}}})
            elif comparator == "$gt":
                if isinstance(val, list):
                    raise FilterError(f"{field}'s value can't be a list when using '{comparator}' comparator")
                result.append({"range": {field: {"gt": val}}})
            elif comparator == "$gte":
                if isinstance(val, list):
                    raise FilterError(f"{field}'s value can't be a list when using '{comparator}' comparator")
                result.append({"range": {field: {"gte": val}}})
            elif comparator == "$lt":
                if isinstance(val, list):
                    raise FilterError(f"{field}'s value can't be a list when using '{comparator}' comparator")
                result.append({"range": {field: {"lt": val}}})
            elif comparator == "$lte":
                if isinstance(val, list):
                    raise FilterError(f"{field}'s value can't be a list when using '{comparator}' comparator")
                result.append({"range": {field: {"lte": val}}})
    elif isinstance(comparison, list):
        result.append({"terms": {field: comparison}})
    elif isinstance(comparison, np.ndarray):
        result.append({"terms": {field: comparison.tolist()}})
    elif isinstance(comparison, DataFrame):
        # We're saving dataframes as json strings so we compare them as such
        result.append({"match": {field: comparison.to_json()}})
    elif isinstance(comparison, str):
        # We can't use "term" for text fields as ElasticSearch changes the value of text.
        # More info here:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html#query-dsl-term-query
        result.append({"match": {field: comparison}})
    else:
        result.append({"term": {field: comparison}})
    return result


def _normalize_ranges(conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merges range conditions acting on a same field.

    Example usage:

    ```python
    conditions = [
        {"range": {"date": {"lt": "2021-01-01"}}},
        {"range": {"date": {"gte": "2015-01-01"}}},
    ]
    conditions = _normalize_ranges(conditions)
    assert conditions == [
        {"range": {"date": {"lt": "2021-01-01", "gte": "2015-01-01"}}},
    ]
    ```
    """
    range_conditions = [list(c["range"].items())[0] for c in conditions if "range" in c]
    if range_conditions:
        conditions = [c for c in conditions if "range" not in c]
        range_conditions_dict = {}
        for field_name, comparison in range_conditions:
            if field_name not in range_conditions_dict:
                range_conditions_dict[field_name] = {}
            range_conditions_dict[field_name].update(comparison)

        for field_name, comparisons in range_conditions_dict.items():
            conditions.append({"range": {field_name: comparisons}})
    return conditions
