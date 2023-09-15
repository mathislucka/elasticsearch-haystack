# SPDX-FileCopyrightText: 2023-present Silvano Cerza <silvanocerza@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Dict, List, Optional, Union, Mapping

from elasticsearch import Elasticsearch, ConflictError
from elastic_transport import NodeConfig

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

    def filter_documents(self, _: Optional[Dict[str, Any]] = None) -> List[Document]:
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
        return []  # FIXME

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

        if policy == DuplicatePolicy.OVERWRITE:
            for doc in documents:
                # Create a document or overwrite it if it already exists.
                self._client.index(index=self._index, id=doc.id, document=doc.to_dict())
            return

        for doc in documents:
            # Create a document only if it doesn't exist yet.
            # If it does and the policy is set to FAIL we raise an error, otherwise we ignore it.
            res = self._client.options(ignore_status=409).create(index=self._index, id=doc.id, document=doc.to_dict())
            if res.meta.status == 409 and policy == DuplicatePolicy.FAIL:
                raise DuplicateDocumentError(f"Document with id '{doc.id}' already exists in the document store.")

    def delete_documents(self, document_ids: List[str]) -> None:
        """
        Deletes all documents with a matching document_ids from the document store.
        Fails with `MissingDocumentError` if no document with this id is present in the store.

        :param object_ids: the object_ids to delete
        """
        for doc_id in document_ids:  # FIXME
            msg = f"ID '{doc_id}' not found, cannot delete it."
            raise MissingDocumentError(msg)
