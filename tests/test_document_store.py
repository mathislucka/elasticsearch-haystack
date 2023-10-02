# SPDX-FileCopyrightText: 2023-present Silvano Cerza <silvanocerza@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import pytest
from haystack.preview.dataclasses.document import Document
from haystack.preview.document_stores.errors import DuplicateDocumentError
from haystack.preview.document_stores.protocols import DuplicatePolicy
from haystack.preview.testing.document_store import DocumentStoreBaseTests

from elasticsearch_haystack.document_store import ElasticsearchDocumentStore


class TestDocumentStore(DocumentStoreBaseTests):
    """
    Common test cases will be provided by `DocumentStoreBaseTests` but
    you can add more to this class.
    """

    @pytest.fixture
    def docstore(self, request) -> ElasticsearchDocumentStore:
        """
        This is the most basic requirement for the child class: provide
        an instance of this document store so the base class can use it.
        """
        hosts = ["http://localhost:9200"]
        # Use a different index for each test so we can run them in parallel
        index = f"{request.node.name}"

        store = ElasticsearchDocumentStore(hosts=hosts, index=index)
        yield store
        store._client.options(ignore_status=[400, 404]).indices.delete(index=index)

    def test_write_duplicate_fail(self, docstore: ElasticsearchDocumentStore):
        """
        Verify `DuplicateDocumentError` is raised when trying to write duplicate files.

        `DocumentStoreBaseTests` declares this test but we override it since we return
        a different error message that it expects.
        """
        doc = Document(text="test doc")
        docstore.write_documents([doc])
        with pytest.raises(DuplicateDocumentError):
            docstore.write_documents(documents=[doc], policy=DuplicatePolicy.FAIL)
        assert docstore.filter_documents(filters={"id": doc.id}) == [doc]

    def test_delete_not_empty(self, docstore: ElasticsearchDocumentStore):
        """
        Verifies delete properly deletes specified document.

        `DocumentStoreBaseTests` declares this test but we override it since we
        want `delete_documents` to be idempotent.
        """
        doc = Document(text="test doc")
        docstore.write_documents([doc])

        docstore.delete_documents([doc.id])

        res = docstore.filter_documents(filters={"id": doc.id})
        assert res == []

    def test_delete_empty(self, docstore: ElasticsearchDocumentStore):
        """
        Verifies delete doesn't raises when trying to delete a non-existing document.

        `DocumentStoreBaseTests` declares this test but we override it since we
        want `delete_documents` to be idempotent.
        """
        docstore.delete_documents(["test"])

    def test_delete_not_empty_nonexisting(self, docstore: ElasticsearchDocumentStore):
        """
        Verifies delete properly deletes specified document in DocumentStore containing
        multiple documents.

        `DocumentStoreBaseTests` declares this test but we override it since we
        want `delete_documents` to be idempotent.
        """
        doc = Document(text="test doc")
        docstore.write_documents([doc])

        docstore.delete_documents(["non_existing"])

        assert docstore.filter_documents(filters={"id": doc.id}) == [doc]

    # The tests below are filters not supported by ElasticsearchDocumentStore
    def test_in_filter_table(self):
        pass

    def test_in_filter_embedding(self):
        pass

    def test_ne_filter_table(self):
        pass

    def test_ne_filter_embedding(self):
        pass

    def test_nin_filter_table(self):
        pass

    def test_nin_filter_embedding(self):
        pass

    def test_gt_filter_non_numeric(self):
        pass

    def test_gt_filter_table(self):
        pass

    def test_gt_filter_embedding(self):
        pass

    def test_gte_filter_non_numeric(self):
        pass

    def test_gte_filter_table(self):
        pass

    def test_gte_filter_embedding(self):
        pass

    def test_lt_filter_non_numeric(self):
        pass

    def test_lt_filter_table(self):
        pass

    def test_lt_filter_embedding(self):
        pass

    def test_lte_filter_non_numeric(self):
        pass

    def test_lte_filter_table(self):
        pass

    def test_lte_filter_embedding(self):
        pass
