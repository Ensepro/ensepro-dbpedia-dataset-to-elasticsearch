"""
@project DBpediaDataset2ElasticSearch
@since 21/09/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""
import json
import datetime as dt
from elasticsearch import helpers as es_helper
from elasticsearch import Elasticsearch

TRIPLE_FILE= "../../files/_homepages_en.txt"
QUERY_FILE = "elasticsearch_query.json"

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
    return element


def loadTriples():
    actions = []

    # recreate index
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)

    with open(TRIPLE_FILE, "r", encoding="utf-8") as triples:
        triple_number = 0
        number_triples_to_bulk = 20000
        max_triples = 10000000

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


def queriesTests():
    with open(QUERY_FILE, "r", encoding="utf-8") as query_file:
        query = query_file.read().replace("\n", " ")

    start = dt.datetime.now()
    results = es.search(index=INDEX_NAME, doc_type=INDEX_TYPE, body=query)
    end = dt.datetime.now()

    execution_time = end - start

    seconds = execution_time.total_seconds()
    miliseconds = int(execution_time.total_seconds() * 1000)

    triples = []
    for doc in results["hits"]["hits"]:
        triple = doc["_source"]
        triples.append(triple)

    to_file = {}
    to_file["triples_size"] = len(triples)
    to_file["triples"] = triples
    to_file["time"] = {}
    to_file["time"]["seconds"] = seconds
    to_file["time"]["miliseconds"] = miliseconds

    with open("_result_elasticsearch.json", "w+", encoding="utf-8") as result_file:
        result_file.write(json.dumps(to_file, ensure_ascii=False, indent=4, sort_keys=False))

    print("done.")


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# loadTriples()
queriesTests()

# count = es.count(index=INDEX_NAME, doc_type=INDEX_TYPE, body={ "query": {"match_all" : { }}})
# print(count)
