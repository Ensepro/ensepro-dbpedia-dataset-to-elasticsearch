"""
@project ensepro
@since 03/08/2017
@author Alencar Rodrigo Hentges <alencarhentges@gmail.com>

"""

import requests

palavras_endpoint = "http://127.0.0.1"
palavras_port = "8099"
palavras_service = "/palavras/analisar/?frase=\"{}\""


def _analyze(word: str):
    response = requests.get(
        ''.join([palavras_endpoint, ':', palavras_port, palavras_service]).format(word)
    )
    return response


def get_canonical_word(word: str):
    json_response = _analyze(word).json()
    analyzed_word = json_response[-1]

    return analyzed_word["palavraCanonica"]
