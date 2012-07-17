# -*- coding: utf-8 -*-
import unittest2
from time import sleep

from pyelastic import PyElastic


class TestPyElastic(unittest2.TestCase):

    def setUp(cls):
        cls.client = PyElastic(host="localhost", port="1980")
        cls.client.index_document("example", '{"example": "Lorem Ipsum"}', "example1", "example1")

    def tearDown(cls):
        cls.client.delete_index("example")
        cls.client.delete_index("example-index")

    def test_should_get_documents(self):
        response = self.client.get_index("example")['hits']['hits'][0]['_source']
        self.assertTrue("example" in response)
        self.assertEqual("Lorem Ipsum", response["example"])

    def test_should_update_documents_with_json(self):
        json = '{"script" : "ctx._source.example = tag", "params" : { "tag" : "Updated field"}}'
        self.client.uptade_documents_with_json(json, "example")
        sleep(2)  # without sleep, field content is Lorem Ipsum
        response_esearch = self.client.get_index("example")["hits"]["hits"][0]["_source"]["example"]
        self.assertEqual("Updated field", response_esearch)

    def test_should_create_index(self):
        self.client.create_index("example-index")
        self.assertTrue(self.client.get_index("example-index"))

    def test_should_create_index_with_shards_and_replicas(self):
        self.client.create_index("example-index", 3, 2)
        response = self.client.get_settings("example-index")
        self.assertEqual('3', response['example-index']['settings']['index.number_of_shards'])
        self.assertEqual('2', response['example-index']['settings']['index.number_of_replicas'])

    def test_should_index_one_document_in_one_index(self):
        self.client.create_index("es_example")
        json = '{"es_example" : "Simple Test"}'
        self.client.index_document("es_example", json, type_es="simple_test", identifier="test1")
        response_es = self.client.get_index("es_example")["hits"]["hits"][0]["_source"]["es_example"]

        self.assertEqual("Simple Test", response_es)
        self.client.delete_index("es_example")
