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

class VirtuosoTests(object):

    def __init__(self, query_file, words_file, result_file):
        self.query_file = query_file
        self.words_file = words_file
        self.result_file = result_file

    def createRegex(self, words):
        regex = words[0]
        for word in words[1:]:
            regex = "|" + word

        return regex

    def createQuery(self):
        with open(self.words_file, "r", encoding="utf-8") as words_file:
            words = words_file.read().split("\n")

        with open(self.query_file, "r", encoding="utf-8") as file_query:
            query = file_query.read().replace("\n", " ")

        regex = self.createRegex(words)
        query = query.format(regex)
        return query

    def executeQuery(self, query):
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
        self.executeTest()
        print("virtuoso done.")