"""
@project DBpediaDataset2ElasticSearch
@since 21/09/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

import datetime as dt
import json
from elasticsearch import helpers as es_helper
from elasticsearch import Elasticsearch

TRIPLE_FILE= "../files/_#homepages_en.txt"
INDEX_NAME = "triples"
INDEX_TYPE = "triple"

INDEX_SETTINGS = {
    "settings": {
        "analysis": {
            "analyzer": {
                "lowercase": {
                    "type": "standard",
                    "filter": ["lowercase"]
                },
            }
        }
    },
    "mappings": {
        INDEX_TYPE: {
            "properties": {
                "subject": {
                    "type": "text",
                    "analyzer": "lowercase"
                },
                "predicate": {
                    "type": "text",
                    "analyzer": "lowercase"
                },
                "object": {
                    "type": "text",
                    "analyzer": "lowercase"
                }
            }
        }
    }
}

class ElasticSearchTests(object):

    def __init__(self, query_file, words_file, result_file):
        self.query_file = query_file
        self.words_file = words_file
        self.result_file = result_file

    def __createElement(self, element: str):
        element = element[1:-1]  # remove '<' and '>' from string
        return element

    def __createInsertAction(self, triple):
        return {
            "_op_type": 'index',
            "_index": INDEX_NAME,
            "_type": INDEX_TYPE,
            "_source": triple
        }

    def __createTriple(self, triple):
        return {
            "subject": self.__createElement(triple[0]),
            "predicate": self.__createElement(triple[1]),
            "object": self.__createElement(triple[2])
        }

    def __loadTriples(self):
        actions = []

        # recreate index
        self.es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
        self.es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)

        with open(TRIPLE_FILE, "r", encoding="utf-8") as triples:
            triple_number = 0
            number_triples_to_bulk = 10000
            max_triples = 1000000

            for triple in triples:
                triple_number += 1
                triple_ = self.__createTriple(triple.split(" "))
                actions.append(self.__createInsertAction(triple_))

                size = len(actions)
                if (size >= number_triples_to_bulk):
                    es_helper.bulk(self.es, actions)
                    actions = []

                if (triple_number > max_triples):
                    break

                print(str(triple_number) + "-" + str(triple_))

        if (len(actions) > 0):
            es_helper.bulk(self.es, actions)


    def createRegex(self, words):
        regex = ".*" + words[0]
        for word in words[1:]:
            regex += ".*|.*" + word

        return regex+".*"


    def createQuery(self):
        with open(self.words_file, "r", encoding="utf-8") as regex_file:
            words = regex_file.read().lower().split("\n")

        with open(self.query_file, "r", encoding="utf-8") as query_file:
            query = query_file.read().replace("\n", " ")

        regex = self.createRegex(words)
        query = query.format(regex)
        return query

    def executeQuery(self, query):
        start = dt.datetime.now()
        results = self.es.search(index=INDEX_NAME, doc_type=INDEX_TYPE, body=query)
        end = dt.datetime.now()
        execution_time = end - start

        triples = []
        for doc in results["hits"]["hits"]:
            triple = doc["_source"]
            triples.append(triple)

        return {
            "triples": triples,
            "time": execution_time
        }


    def formatResults(self, result):
        results = {}
        results["time"] = {}
        results["triples_size"] = len(result["triples"])
        results["triples"] = result["triples"]
        results["time"]["seconds"] = result["time"].total_seconds()
        results["time"]["miliseconds"] = int(result["time"].total_seconds() * 1000)

        return results


    def executeTest(self):
        query = self.createQuery()
        result = self.executeQuery(query)
        formatedResult = self.formatResults(result)

        with open(self.result_file, "w+", encoding="utf-8") as result_file:
            result_file.write(json.dumps(formatedResult, ensure_ascii=False, indent=4, sort_keys=False))

    def execute(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        # self.__loadTriples()
        self.executeTest()
        print("elasticsearch done.")