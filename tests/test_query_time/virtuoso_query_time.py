"""
@project DBpediaDataset2ElasticSearch
@since 27/09/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""
import json
import datetime as dt
from SPARQLWrapper import SPARQLWrapper, JSON

IP = "127.0.0.1"
PORT = "8890"
PATH = "sparql"

ENDPOINT = "http://" + IP + ":" + PORT + "/" + PATH

KEY_WORD = "WORD"
QUERY_FILE = "virtuoso_query.txt"
WORDS_FILE = "virtuoso_words.txt"

QUERIES = []


def executeQuery(query):
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    start = dt.datetime.now()
    results = sparql.query().convert()
    end = dt.datetime.now()

    execution_time = end - start

    triples = []

    for result in results["results"]["bindings"]:
        triple = {}
        triple["subject"] = result["s"]["value"]
        triple["predicate"] = result["p"]["value"]
        triple["object"] = result["o"]["value"]
        triples.append(triple)

    return {
        "triples": triples,
        "time": execution_time
    }


def loadWords():
    with open(WORDS_FILE, "r", encoding="utf-8") as file_words:
        words = file_words.read().split("\n")

    return words


def createQueries():
    words = loadWords()

    with open(QUERY_FILE, "r", encoding="utf-8") as file_query:
        generic_query = file_query.read().replace("\n", " ")

    for word in words:
        QUERIES.append(generic_query.replace(KEY_WORD, word))


def queriesTests():
    results = {}
    results["triples"] = []
    results["time"] = {}
    results["time"]["total_seconds"] = 0
    results["time"]["total_miliseconds"] = 0

    for query in QUERIES:
        result = executeQuery(query)

        results["time"]["total_seconds"] += result["time"].total_seconds()
        results["time"]["total_miliseconds"] += int(result["time"].total_seconds() * 1000)

        results["triples"] += result["triples"]

    with open("_result_virtuoso.json", "w+", encoding="utf-8") as result_file:
        result_file.write(json.dumps(results, ensure_ascii=False, indent=4, sort_keys=False))

    print("done.")


createQueries()
queriesTests()
