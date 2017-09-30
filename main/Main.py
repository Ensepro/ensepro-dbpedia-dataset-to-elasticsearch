"""
@project DBpediaDataset2ElasticSearch
@since 21/09/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

DBpedia datasets download at http://wiki.dbpedia.org/downloads-2016-10#datasets
ElasticSearch version: 5.6.1
"""
import json
from elasticsearch import helpers as es_helper
from elasticsearch import Elasticsearch

# TRIPLE_FILE= "../files/_#triples.txt"
TRIPLE_FILE= "../files/100_triples.txt"

INDEX_NAME = "triples"
INDEX_TYPE = "triple"

INDEX_SETTINGS = {
    "settings":{
        "analysis":{
            "analyzer":{
                "ignore_accentuation": {
                    "tokenizer": "keyword",
                    "filter" : [
                        "lowercase",
                        "asciifolding"
                    ]
                }
            }
        }
    },
    "mappings": {
        INDEX_TYPE: {
            "properties": {
                "subject": {
                    "type": "nested",
                    "include_in_parent": True,
                    "properties": {
                        "concept": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        },
                        "uri": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        }
                    }
                },
                "predicate": {
                    "type": "nested",
                    "include_in_parent": True,
                    "properties": {
                        "concept": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        },
                        "uri": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        }
                    }
                },
                "object": {
                    "type": "nested",
                    "include_in_parent": True,
                    "properties": {
                        "concept": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        },
                        "uri": {
                            "type": "string",
                            "analyzer": "ignore_accentuation"
                        }
                    }
                }
            }
        }
    }
}


def createInsertAction(triple):
    return {
        "_op_type": 'index',
        "_index": INDEX_NAME,
        "_type": INDEX_TYPE ,
        "_source": triple
    }


def createTriple(triple):
    return {
        "subject": createElement(triple[0]),
        "predicate": createElement(triple[1]),
        "object": createElement(triple[2])
    }


def createElement(element: str):
    element = element[1:-1]  # remove '<' and '>' from string
    split_point = element.rfind("/")
    uri = element[:split_point]
    concept = element[split_point + 1:]

    return {
        "concept": concept,
        "uri": uri
    }


def executeScan(index, client, scroll, query):
    return es_helper.scan(
        index=index,
        client=client,
        scroll=scroll,
        query=query
    )


def loadTriples():
    actions = []

    # recreate index
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)

    with open(TRIPLE_FILE, "r", encoding="utf-8") as triples:
        triple_number = 0
        number_triples_to_bulk = 10000
        max_triples = 50000

        for triple in triples:
            triple_number += 1
            triple_ = createTriple(triple.split(" "))
            actions.append(createInsertAction(triple_))

            size = len(actions)
            if (size >= number_triples_to_bulk):
                es_helper.bulk(es, actions)
                actions = []

            if (triple_number > max_triples):
                break

            print(str(triple_number) + "-" + str(triple_))

    if (len(actions) > 0):
        es_helper.bulk(es, actions)


def listIndexes():
    for index in es.indices.get('*'):
        print(index)


def queriesTests():
    query = {
        "query": {
            "match_all": {},
        }
    }
    query = {
        "query":{
            "query_string": {
                "fields": ["object.concept"],
                "query": "'Edifício' OR 'Gaúcho' OR 'Universo' OR 'Sistema_Solar' OR 'Exoplaneta'"
            }
        }
    }
    query = {
        "query":{
            "query_string":{
                "query" : "subject.concept :('Astronomi', 'Aquiles') AND object.concept: ('Apolo', 'Sistema_Solar')",
            }
        }
    }
    query = {
        "query":{
            "query_string":{
                "default_field": "object.concept",
                "query" : "gaucho OR edificio",
            }
        }
    }



    results = executeScan(INDEX_NAME, es, '2m', query)

    for result in results:
        print(json.dumps(result, ensure_ascii=False, indent=4, sort_keys=False))




es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

loadTriples()
queriesTests()

# count = es.count(index=INDEX_NAME, doc_type=DOC_TYPE, body={ "query": {"match_all" : { }}})
# print(count)

