# SPDX-FileCopyrightText: 2023-present Silvano Cerza <silvanocerza@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Dict, List, Optional, Union, Mapping
import json

from elasticsearch import Elasticsearch, helpers
from elastic_transport import NodeConfig
import numpy as np
from pandas import DataFrame

from haystack.preview.dataclasses import Document
from haystack.preview.document_stores.decorator import document_store
from haystack.preview.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.preview.document_stores.protocols import DuplicatePolicy

logger = logging.getLogger(__name__)

Hosts = Union[str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]]]


@document_store
class ElasticsearchDocumentStore:
    def __init__(self, *, hosts: Optional[Hosts] = None, index: str = "default", **kwargs):
        """
        Creates a new ElasticsearchDocumentStore instance.

        For more information on connection parameters, see the official Elasticsearch documentation:
        https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html

        For the full list of supported kwargs, see the official Elasticsearch reference:
        https://elasticsearch-py.readthedocs.io/en/stable/api.html#module-elasticsearch

        :param hosts: List of hosts running the Elasticsearch client. Defaults to None
        :param index: Name of index in Elasticsearch, if it doesn't exist it will be created. Defaults to "default"
        :param \*\*kwargs: Optional arguments that ``Elasticsearch`` takes.
        """
        self._client = Elasticsearch(hosts, **kwargs)
        self._index = index

        # Check client connection, this will raise if not connected
        self._client.info()

        # Create the index if it doesn't exist
        if not self._client.indices.exists(index=index):
            self._client.indices.create(index=index)

    def count_documents(self) -> int:
        """
        Returns how many documents are present in the document store.
        """
        return self._client.count(index=self._index)["count"]

    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        """
        Returns the documents that match the filters provided.

        Filters are defined as nested dictionaries. The keys of the dictionaries can be a logical operator (`"$and"`,
        `"$or"`, `"$not"`), a comparison operator (`"$eq"`, `$ne`, `"$in"`, `$nin`, `"$gt"`, `"$gte"`, `"$lt"`,
        `"$lte"`) or a metadata field name.

        Logical operator keys take a dictionary of metadata field names and/or logical operators as value. Metadata
        field names take a dictionary of comparison operators as value. Comparison operator keys take a single value or
        (in case of `"$in"`) a list of values as value. If no logical operator is provided, `"$and"` is used as default
        operation. If no comparison operator is provided, `"$eq"` (or `"$in"` if the comparison value is a list) is used
        as default operation.

        Example:

        ```python
        filters = {
            "$and": {
                "type": {"$eq": "article"},
                "date": {"$gte": "2015-01-01", "$lt": "2021-01-01"},
                "rating": {"$gte": 3},
                "$or": {
                    "genre": {"$in": ["economy", "politics"]},
                    "publisher": {"$eq": "nytimes"}
                }
            }
        }
        # or simpler using default operators
        filters = {
            "type": "article",
            "date": {"$gte": "2015-01-01", "$lt": "2021-01-01"},
            "rating": {"$gte": 3},
            "$or": {
                "genre": ["economy", "politics"],
                "publisher": "nytimes"
            }
        }
        ```

        To use the same logical operator multiple times on the same level, logical operators can take a list of
        dictionaries as value.

        Example:

        ```python
        filters = {
            "$or": [
                {
                    "$and": {
                        "Type": "News Paper",
                        "Date": {
                            "$lt": "2019-01-01"
                        }
                    }
                },
                {
                    "$and": {
                        "Type": "Blog Post",
                        "Date": {
                            "$gte": "2019-01-01"
                        }
                    }
                }
            ]
        }
        ```

        :param filters: the filters to apply to the document list.
        :return: a list of Documents that match the given filters.
        """
        query = {"bool": {"filter": self._normalize_filters(filters)}} if filters else None

        res = self._client.search(
            index=self._index,
            query=query,
        )

        return [self._deserialize_document(hit) for hit in res["hits"]["hits"]]

    def _normalize_ranges(self, conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    def _parse_comparison(self, field: str, comparison: Union[Dict, List, str, float]) -> List:
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

    def _normalize_filters(self, filters: Union[List[Dict], Dict], logical_condition="") -> Dict[str, Any]:
        conditions = []
        if isinstance(filters, dict):
            filters = [filters]
        for filter in filters:
            for operator, value in filter.items():
                if operator in ["$not", "$and", "$or"]:
                    # Logical operators
                    conditions.append(self._normalize_filters(value, operator))
                else:
                    # Comparison operators
                    conditions.extend(self._parse_comparison(operator, value))

        if len(conditions) == 1:
            return conditions[0]

        conditions = self._normalize_ranges(conditions)

        if logical_condition == "$not":
            return {"bool": {"must_not": conditions}}
        elif logical_condition == "$and":
            return {"bool": {"must": conditions}}
        elif logical_condition == "$or":
            return {"bool": {"should": conditions}}
        return {"bool": {"must": conditions}}

    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.FAIL) -> None:
        """
        Writes (or overwrites) documents into the store.

        :param documents: a list of documents.
        :param policy: documents with the same ID count as duplicates. When duplicates are met,
            the store can:
             - skip: keep the existing document and ignore the new one.
             - overwrite: remove the old document and write the new one.
             - fail: an error is raised
        :raises DuplicateDocumentError: Exception trigger on duplicate document if `policy=DuplicatePolicy.FAIL`
        :return: None
        """
        if len(documents) > 0:
            if not isinstance(documents[0], Document):
                raise ValueError("param 'documents' must contain a list of objects of type Document")

        action = "index" if policy == DuplicatePolicy.OVERWRITE else "create"
        _, errors = helpers.bulk(
            client=self._client,
            actions=(
                {"_op_type": action, "_id": doc.id, "_source": self._serialize_document(doc)} for doc in documents
            ),
            refresh="wait_for",
            index=self._index,
            raise_on_error=False,
        )

        if errors and policy == DuplicatePolicy.FAIL:
            # TODO: Handle errors in a better way, we're assuming that all errors
            # are related to duplicate documents but that could be very well be wrong.
            ids = ', '.join((e["create"]["_id"] for e in errors))
            raise DuplicateDocumentError(f"IDs '{ids}' already exist in the document store.")

    def _deserialize_document(self, hit: Dict[str, Any]) -> Document:
        """
        Creates a Document from the search hit provided.
        This is mostly useful in self.filter_documents().
        """
        data = hit["_source"]

        if "highlight" in hit:
            data["metadata"]["highlighted"] = hit["highlight"]
        data["score"] = hit["_score"]

        if array := data["array"]:
            data["array"] = np.asarray(array, dtype=np.float32)
        if dataframe := data["dataframe"]:
            data["dataframe"] = DataFrame.from_dict(json.loads(dataframe))
        if embedding := data["embedding"]:
            data["embedding"] = np.asarray(embedding, dtype=np.float32)

        # We can't use Document.from_dict() as the data dictionary contains
        # all the metadata fields
        return Document(
            id=data["id"],
            text=data["text"],
            array=data["array"],
            dataframe=data["dataframe"],
            blob=data["blob"],
            mime_type=data["mime_type"],
            metadata=data["metadata"],
            id_hash_keys=data["id_hash_keys"],
            score=data["score"],
            embedding=data["embedding"],
        )

    def _serialize_document(self, doc: Document) -> Dict[str, Any]:
        """
        Serializes Document to a dictionary handling conversion of Pandas' dataframe
        and NumPy arrays if present.
        """
        # We don't use doc.flatten() cause we want to keep the metadata field
        # as it makes it easier to recreate the Document object when calling
        # self.filter_document().
        # Otherwise we'd have to filter out the fields that are not part of the
        # Document dataclass and keep them as metadata. This is faster and easier.
        res = {**doc.to_dict(), **doc.metadata}
        if res["array"] is not None:
            res["array"] = res["array"].tolist()
        if res["dataframe"] is not None:
            # Convert dataframe to a json string
            res["dataframe"] = res["dataframe"].to_json()
        if res["embedding"] is not None:
            res["embedding"] = res["embedding"].tolist()
        return res

    def delete_documents(self, document_ids: List[str]) -> None:
        """
        Deletes all documents with a matching document_ids from the document store.

        :param object_ids: the object_ids to delete
        """

        #
        helpers.bulk(
            client=self._client,
            actions=({"_op_type": "delete", "_id": id} for id in document_ids),
            refresh="wait_for",
            index=self._index,
            raise_on_error=False,
        )
