import csv
import uuid
from typing import Dict, List, Any, Union
from bs4 import BeautifulSoup
import html
from elasticsearch import Elasticsearch


def getCSV(filename, INDEX_NAME, TYPE_NAME):
    f = open(filename, newline='')
    reader = csv.reader(f)
    header = next(reader)
    bulk_data = []  # type: List[Union[Dict[str, Dict[str, Union[str, Any]]], Dict[Any, Any]]]

    for row in reader:
        data_dict = {}
        for i in range(len(row)):
            data_dict[header[i]] = row[i]
        op_dict = {
            "index": {
                "_index": INDEX_NAME,
                "_type": TYPE_NAME,
                "_id": str(uuid.uuid4())
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)
    return bulk_data

def extract_text(html_string):
    unesacped_html = html.unescape(html_string)
    soup = BeautifulSoup(unesacped_html)
    text = soup.get_text(strip=True)
    return text


def load_elastic(ES_HOST, INDEX_NAME, bulk_data):
    # Connect Elastic Server
    es = Elasticsearch(hosts=[ES_HOST], http_auth=('elastic', '!admin1234'), )

    # Create Index
    if not es.indices.exists(INDEX_NAME):
        print("creating '%s' index ..." % (INDEX_NAME))
        res = es.indices.create(index=INDEX_NAME)
        print(" responses: '%s'" % (res))

    # Load Data
    print("bulk indexing...")
    res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)

    # Testing Load Data
    res = es.search(index=INDEX_NAME, size=500, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))

INDEX_NAME = "dailynewsbriefing"
TYPE_NAME = "dailynewsbriefing"
ES_HOST = {"host": "52.78.1.254", "port": 9200}

# Feeding Data from RSS feeds
file = 'dailynewsbriefing.csv'
bulk_data = getCSV(file, INDEX_NAME, TYPE_NAME)

# Load data into Elasticsearch
load_elastic(ES_HOST, INDEX_NAME, bulk_data)
