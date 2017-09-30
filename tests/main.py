"""
@project DBpediaDataset2ElasticSearch
@since 30/09/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

from tests.elasticsearch_query_time import ElasticSearchTests
from tests.virtuoso_query_time import VirtuosoTests

VI_QUERY_FILE = "virtuoso_query.txt"
ES_QUERY_FILE = "elasticsearch_query.txt"
WORDS_FILE = "words.txt"

es_test = ElasticSearchTests(ES_QUERY_FILE, WORDS_FILE, "_result_elasticsearch.json")
vi_test = VirtuosoTests(VI_QUERY_FILE, WORDS_FILE, "_result_virtuoso.json")


es_test.execute()
vi_test.execute()
