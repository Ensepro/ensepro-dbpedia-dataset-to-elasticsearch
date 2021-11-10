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

datasets = [dataset for dataset in os.listdir(path) if dataset.endswith(".bz2")]
datasets.sort()

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
triple_number = 0
for dataset in datasets:
    ttlfile = path + dataset.replace(".bz2", "")
    print(ttlfile)
    # print("bzip2 -d " + path + "dataset")
    os.system("bzip2 -d " + path + dataset)

    # continue
    SETTINGS[DATASET] = ttlfile
    es_helper = ElasticSearchHelper(ES, SETTINGS, False, is_first)
    triple_number += es_helper.load_triples()
    is_first = False

    os.system("rm " + ttlfile)

print("total triples:", triple_number)
