# SPDX-FileCopyrightText: 2023-present Silvano Cerza <silvanocerza@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import pytest
from haystack.preview.testing.document_store import DocumentStoreBaseTests

from elasticsearch_haystack.document_store import ElasticsearchDocumentStore


@pytest.mark.skip("This is an example Document Store")
class TestDocumentStore(DocumentStoreBaseTests):
    """
    Common test cases will be provided by `DocumentStoreBaseTests` but
    you can add more to this class.
    """

    @pytest.fixture
    def docstore(self) -> ElasticsearchDocumentStore:
        """
        This is the most basic requirement for the child class: provide
        an instance of this document store so the base class can use it.
        """
        return ElasticsearchDocumentStore()
