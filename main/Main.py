"""
@project DBpediaDataset2ElasticSearch
@since 19/10/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

from Constants import *
from elasticsearch import Elasticsearch
from ElasticSearchHelper import ElasticSearchHelper
import os

ES_HOST = "host.docker.internal"
ES_PORT = 9200

SETTINGS = {}
SETTINGS[INDEX_NAME] = "ensepro_triplas"
SETTINGS[INDEX_TYPE] = "ensepro_tripla"
SETTINGS[INDEX_SETTINGS] = "index_settings.json"
# SETTINGS[DATASET] = "C:\\_ensepro\\datasets\\infobox_property_definitions_pt.ttl"
SETTINGS[TRIPLES_TO_BULK] = 5000
SETTINGS[MAX_TRIPLES] = 999999999

path = "/datasets/"
ES = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])

datasets = [dataset for dataset in os.listdir(path) if dataset.endswith(".ttl")]

# total = 0
# for dataset in datasets:
#     with open(path + dataset, "r", encoding="utf-8") as triples:
#         size = 0
#         for triple in triples:
#             if triple.startswith("#"):
#                 continue
#             size += 1
#
#         # size = len(triples)
#         total += size
#         print(dataset.replace("_", "\\_"), "&", size, "\\\\")
#
# print("Total", "&", total)
is_first = True
for dataset in datasets:
    print(dataset)
    # continue
    SETTINGS[DATASET] = path + dataset
    es_helper = ElasticSearchHelper(ES, SETTINGS, False, is_first)
    es_helper.load_triples()
    is_first = False
