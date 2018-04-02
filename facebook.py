import uuid
import json
import requests
import pandas as pd
import csv
import time
from elasticsearch import Elasticsearch

def getURL(GROUP_ID):
    BASE = "https://graph.facebook.com"
    NODE = "/" + GROUP_ID + "/feed"
    PARAMETERS = "/?fields=id,message,link,created_time,name,from,attachments{title,url,subattachments},comments&limit=25,shares&limit=%s&access_token=%s" % (NUM_STATUSES, ACCESS_TOKEN) # changed
    URL = BASE + NODE + PARAMETERS
    return URL

def getFacebook(url, filename):
    data = pd.DataFrame()
    i = 0
    while url != 'last':
        try:
            df = json.loads(requests.get(url).text)['data']
            url = json.loads(requests.get(url).text)['paging']['next']
            data_page = pd.DataFrame.from_dict(df)
            data = data.append(data_page)
            i = i + 1
            print("Getting Facebook posts ..." + url)
        except:
            url = 'last'

    data = data.fillna(0).set_index('id')

    attachments_text = []

    for i in range(data.shape[0]):
        attachments = ""
        if data['attachments'][i] != 0:
            for j in range(len(data['attachments'][i].get('data'))):
                title = str(data['attachments'][i].get('data')[j].get('title'))
            url = str(data['attachments'][i].get('data')[j].get('url'))
            attachments += " " + title + ":" + url
            attachments_text.append(attachments)
        else:
            attachments_text.append(0)
    data['attachments_text'] = attachments_text

    comments_text = []
    from_name = []
    for i in range(data.shape[0]):
        com = ""
        if data['comments'][i] != 0:
            for j in range(len(data['comments'][i].get('data'))):
                name = data['comments'][i].get('data')[j].get('from').get('name')
                msg = data['comments'][i].get('data')[j].get('message')
                com += " " + name + ":" + msg
            comments_text.append(com)
        else:
            comments_text.append(0)
        from_name.append(data['from'][i].get('name'))

    data['comments_text'] = comments_text
    data['from_name'] = from_name
    data_selected = data[['name', 'message', 'comments_text', 'from_name', 'link', 'attachments_text', 'created_time']]
    data_selected.to_csv(filename, encoding='utf-8')
    return data


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


def load_elastic(ES_HOST, INDEX_NAME, bulk_data):
    # Connect Elastic Server
    es = Elasticsearch(hosts=[ES_HOST], http_auth=('elastic', '!admin1234'), )

    # Create Index
    if not es.indices.exists(INDEX_NAME):
        print("creating '%s' index ..." % (INDEX_NAME))
        res = es.indices.create(index=INDEX_NAME)
        print(" responses: '%s'" % (res))

    len(bulk_data)

    # Load Data
    print("bulk indexing...")
    res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)

    # Testing Load Data
    res = es.search(index=INDEX_NAME, size=500, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))

#initialize
ACCESS_TOKEN = "EAAc0UNZAJpqYBAHnhuYRzZAtA2tlsx6oVQ5GKY6X3XXNuqRXDuE5ZAjj9y492DgmLmkWL17C1eA4u25AnKErTxqabuOcCHOtozZCgQGogAN4UQcDUEww9MWJZA4UGZBZCokbToHUYHn8WMr9ExL1Xrk3KZCLZCw9vZCBVTEkE5jS4A6QZDZD"
NUM_STATUSES = 100

GROUP_ID1 = '1801883896772814' #jcvc
GROUP_ID2 = '1398154227152195'  #global
FILE_NAME1 = 'jcvc.csv'
FILE_NAME2 = 'global.csv'



ES_HOST = {"host" : "52.78.1.254", "port" : 9200}

INDEX_NAME1 = 'fb_jcvc'
TYPE_NAME1 = 'fb_jcvc'
INDEX_NAME2 = 'fb_global'
TYPE_NAME2 = 'fb_global'

ID_FIELD = 'id'

URL1 = getURL(GROUP_ID1)
URL2 = getURL(GROUP_ID2)

#df1 = getFacebook(URL1, FILE_NAME1)
#df2 = getFacebook(URL2, FILE_NAME2)

#print(df1.info())
#print(df2.info())

#data1 = getCSV(FILE_NAME1, INDEX_NAME1, TYPE_NAME1)
#load_elastic(ES_HOST, INDEX_NAME1, data1)

data2 = getCSV(FILE_NAME2, INDEX_NAME2, TYPE_NAME1)

data2_list = [data2[i:i+50] for i in range(0,len(data2),50)]

for i in range(len(data2_list)):
    load_elastic(ES_HOST, INDEX_NAME2, data2_list[i])
    time.sleep(2)




