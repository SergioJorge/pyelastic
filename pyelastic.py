# -*- coding: utf-8 -*-
import requests
import json
import os


class PyElastic(object):

    def __init__(self, host="localhost", port=9200):
        self.uri = "%s:%s" % (host, port)
        self.host = host
        self.port = port

    def _get_amount_documents(self, index):
        amount_documents = requests.get(self.generate_url(index) + "/_count")
        if amount_documents.ok:
            result = json.loads(amount_documents.content)['count']
            return result
        else:
            print "No documents found in index %s/%s" % (self.uri, index)
            return 0

    def _get_uri_scroll(self, index, amount_documents):
        """
        URL of the documentation using scroll to return so eficient 
        large amount of documents.

        http://www.elasticsearch.org/guide/reference/api/search/search-type.html section SCAN
        """
        uri_scroll_id = self.generate_url(index) + "/_search?search_type=scan&scroll=10m&size=%s" % (amount_documents)
        scroll_documents = requests.get(uri_scroll_id)
        if scroll_documents.ok:
            return json.loads(scroll_documents.content)['_scroll_id']
        else:
            raise Exception("There was an error retrieving the ID of the scroll")

    def _get_documents_with_id_scroll(self, index, id_scroll):
        uri_scroll = self.generate_url() + "_search/scroll?scroll=10m"
        documents = requests.get(uri_scroll, data=id_scroll)
        if documents.ok:
            return json.loads(documents.content)['hits']['hits']
        else:
            raise Exception("An error occurred when retrieving documents.")

    def _update(self, index, field_id, field_type, json):
        uri_update = self.generate_url(index) + "/%s/%s/_update" % (field_type, field_id)
        response = requests.post(uri_update, data=json)
        return response.ok

    def generate_url(self, path=""):
        return "http://%s/%s" % (self.uri, path)

    def get_index(self, path):
        search = self.generate_url(path) + "/_search"
        response = requests.get(search)
        if response.ok:
            return json.loads(response.text)
        else:
            raise Exception(response.content)

    def get_settings(self, path):
        search = self.generate_url(path) + "/_settings"
        response = requests.get(search)
        if response.ok:
            return json.loads(response.text)
        else:
            raise Exception(response.content)
            

    def delete_index(self, path):
        response = requests.delete(self.generate_url(path))
        return json.loads(response.text)

    def uptade_documents_with_json(self, json, index):
        amount_documents = self._get_amount_documents(index)
        id_scroll = self._get_uri_scroll(index, amount_documents)

        documents = self._get_documents_with_id_scroll(index, id_scroll)
        log_dir = os.path.exists("logs")
        if log_dir == False:
            os.mkdir("logs")
        archive_log = open('logs/errors_update_%s.log' % index, 'w')
        for document in documents:
            field_id = document['_id']
            field_type = document['_type']

            if not self._update(index, field_id, field_type, json):
                archive_log.write("Error update the documents: %s/%s/%s" % (index, field_type, field_id))
        archive_log.close()

    def create_index(self, index, number_shards=None, number_replicas=None):
        if number_shards and number_replicas:
            settings = '{"settings": {"index":{"number_of_shards": %d, "number_of_replicas": %d }}}' % (number_shards, number_replicas)
            requests.put(self.generate_url(index), data=settings)
        else:
            requests.put(self.generate_url(index))

    def index_document(self, index, data, type_es, identifier):
        response = requests.post(self.generate_url(index) + '/%s/%s?refresh=True' % (type_es, identifier), data)
        return json.loads(response.text)
