"""
@project DBpediaDataset2ElasticSearch
@since 18/10/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

from elasticsearch import helpers as es_helper
from main.Constants import *

from main.Log import Log

logger = Log("info")

class ElasticSearchHelper(object):

    def __init__(self, es, settings):
        self.es = es
        self.settings = settings

    def __createElementIRI(self, element):
        element = element[1:-1]  # remove '<' and '>' from string
        split_point = element.rfind("/")
        uri = element[:split_point]
        concept = element[split_point + 1:]

        return {
            "original_text": element,
            "concept": concept,
            "uri": uri
        }

    def __createElement(self, element: str):
        if (element.startswith("<")):
            return self.__createElementIRI(element)

            # return the value between the first \" and the last \".
        return {
            "original_text": element,
            "concept": element[element.find("\"") + 1:element.rfind("\"")]
        }

    def __createInsertAction(self, triple):
        return {
            "_op_type": 'index',
            "_index": self.settings[INDEX_NAME],
            "_type": self.settings[INDEX_TYPE],
            "_source": triple
        }

    def __createTriple(self, triple):
        return {
            "subject": self.__createElement(triple[0]),
            "predicate": self.__createElement(triple[1]),
            "object": self.__createElement(triple[2])
        }

    def loadTriples(self):
        actions = []

        index_settings = open(self.settings[INDEX_SETTINGS]).read()

        # recreate index
        logger.info("removendo indice \"{}\"".format(self.settings[INDEX_NAME]))
        self.es.indices.delete(index=self.settings[INDEX_NAME], ignore=[400, 404])

        logger.info("criando indice \"{}\"".format(self.settings[INDEX_NAME]))
        self.es.indices.create(index=self.settings[INDEX_NAME], body=index_settings)

        logger.info("iniciando a carega do dataset[{}]".format(self.settings[DATASET]))
        with open(self.settings[DATASET], "r", encoding="utf-8") as triples:
            triple_number = 1
            for triple in triples:
                if (triple.startswith("#")):
                    continue
                triple_ = self.__createTriple(triple.split(" "))
                actions.append(self.__createInsertAction(triple_))

                size = len(actions)
                if (size >= self.settings[TRIPLES_TO_BULK]):
                    logger.debug("Executando bulk - {} triplas".format(self.settings[TRIPLES_TO_BULK]))
                    es_helper.bulk(self.es, actions)
                    actions = []

                if (triple_number > self.settings[MAX_TRIPLES]):
                    break

                triple_number += 1

                logger.debug("Tripla: id={triple_number} - {triple}".format(triple_number=triple_number, triple=str(triple_)))


        logger.info("Carregadas {} triplas!".format(triple_number-1))
        if (len(actions) > 0):
            es_helper.bulk(self.es, actions)