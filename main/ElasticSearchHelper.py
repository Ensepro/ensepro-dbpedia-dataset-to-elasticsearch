"""
@project DBpediaDataset2ElasticSearch
@since 18/10/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

from elasticsearch import helpers as es_helper
# from services import PalavrasService
from Constants import *
from Log import Log

logger = Log("info")


class ElasticSearchHelper(object):
    def __init__(self, es, settings, should_use_canonical_word=False, recreate_index=False):
        self.es = es
        self.settings = settings
        self.should_use_canonical_word = should_use_canonical_word
        self.recreate_index = recreate_index

    def _get_canonical_word(self, word: str):
        word = word.replace("-", "_")
        if self.should_use_canonical_word:
            logger.debug("Buscando palavra canônica para palavra '{}'".format(word))
            # canonical_word = PalavrasService.get_canonical_word(word)
            # logger.debug("Palavra canônica buscada. {0} <-> {1}".format(word, canonical_word))
            # return canonical_word
            raise Exception("should not pass here")
        return word

    def _create_element_uri(self, element):
        element = element[1:-1]  # remove '<' and '>' from string
        element = element.replace("> .", "")
        split_point = element.rfind("/")
        uri = element[:split_point]
        concept = element[split_point + 1:]

        # get canonical word if self.should_use_canonical_word == True
        concept = self._get_canonical_word(concept)

        return {
            "texto_original": element,
            "conceito": concept,
            "ngram_conceito": concept,
            "uri": uri
        }

    def _create_element(self, element: str):
        if "<http" == element[0:5]:
            return self._create_element_uri(element)

        # return the value between the first \" and the last \".
        concept = element[element.find("\"") + 1:element.rfind("\"")]

        return {
            "texto_original": element,
            "conceito": concept,
            "ngram_conceito": concept
        }

    def _create_insert_action(self, triple):
        return {
            "_op_type": 'index',
            "_index": self.settings[INDEX_NAME],
            # "_type": self.settings[INDEX_TYPE],
            "_source": triple
        }

    def _create_triple(self, triple):
        return {
            "sujeito": self._create_element(triple[0]),
            "predicado": self._create_element(triple[1]),
            "objeto": self._create_element(triple[2])
        }

    def _split_terms(self, tripleOriginal):
        first_space = tripleOriginal.index(" ")
        second_space = tripleOriginal[first_space + 1:].index(" ")

        subject = tripleOriginal[:first_space]
        predicate = tripleOriginal[first_space + 1:][:second_space]
        object = tripleOriginal[first_space + 1:][second_space + 1:]

        return [subject, predicate, object]

    def load_triples(self):
        actions = []

        if self.recreate_index:
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
                triple_ = self._create_triple(self._split_terms(triple))
                actions.append(self._create_insert_action(triple_))

                size = len(actions)
                if (size >= self.settings[TRIPLES_TO_BULK]):
                    logger.info("Executando bulk - {} triplas. total: {}".format(self.settings[TRIPLES_TO_BULK], triple_number))
                    es_helper.bulk(self.es, actions, request_timeout=60)
                    actions = []

                if (triple_number > self.settings[MAX_TRIPLES]):
                    break

                logger.debug("Tripla: id={triple_number} - {triple}".format(triple_number=triple_number, triple=str(triple_)))

                triple_number += 1

        logger.info("Carregadas {} triplas!".format(triple_number - 1))
        if (len(actions) > 0):
            es_helper.bulk(self.es, actions, request_timeout=60)
        return triple_number
