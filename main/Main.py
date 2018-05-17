"""
@project DBpediaDataset2ElasticSearch
@since 19/10/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

from main.Constants import *
from elasticsearch import Elasticsearch
from main.ElasticSearchHelper import ElasticSearchHelper

ES_HOST = "127.0.0.1"
ES_PORT = 9200

SETTINGS = {}
SETTINGS[INDEX_NAME] = "ensepro_triplas"
SETTINGS[INDEX_TYPE] = "ensepro_tripla"
SETTINGS[INDEX_SETTINGS] = "index_settings.json"
SETTINGS[DATASET] = "../files/persondata_pt_sample.ttl"
SETTINGS[TRIPLES_TO_BULK] = 3000
SETTINGS[MAX_TRIPLES] = 1000000


ES = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])


es_helper = ElasticSearchHelper(ES, SETTINGS, False, True)

es_helper.load_triples()