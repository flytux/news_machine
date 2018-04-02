import uuid
from pprint import pprint
from bs4 import BeautifulSoup
from BeautifulSoup import BeautifulStoneSoup
import pandas
import feedparser
from dateutil import parser as dp


def getNews(feeds: object, INDEX_NAME: object, TYPE_NAME: object) -> object:
    bulk_data = []

    for feed_url in feeds:
        feed = feedparser.parse(feed_url)
        for item in range(len(feed.entries)):
            id = feed.entries[item].id
            title = feed.entries[item].title
            description = feed.entries[item].description
            published = dp.parse(feed.entries[item].published)
            summary = feed.entries[item].summary
            link = feed.entries[item].link
            html = feed.entries[item].content[0].value
            soup = BeautifulSoup(html, "lxml")
            decoded = BeautifulStoneSoup(soup, convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
            content = decoded.get_text()
            data = {
                "feed_url": feed_url,
                "feed_id": id,
                "title": title,
                "description": description,
                "published": published,
                "summary": summary,
                "link": link,
                "content": content
            }
            index = {
                "index": {
                    "_index": INDEX_NAME,
                    "_type": TYPE_NAME,
                    "_id": str(uuid.uuid4())
                }
            }
            bulk_data.append(index)
            bulk_data.append(data)

    return bulk_data

TECHCRUNCH = "http://techcrunch.com/feed/"
TECHINASIA = "https://www.techinasia.com/feed"

feeds = [TECHCRUNCH, TECHINASIA]
INDEX_NAME = "news"
TYPE_NAME = "news"
ES_HOST = {"host" : "52.78.1.254", "port" : 9200}

bulk_data = getNews(feeds, INDEX_NAME, TYPE_NAME)

from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=[ES_HOST], http_auth=('elastic', '!admin1234'), )

if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index ..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))

print("creating '%s' index ..." % (INDEX_NAME))
res = es.indices.create(index=INDEX_NAME)
print(" responses: '%s'" % (res))

print("bulk indexing...")
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

res = es.search(index = INDEX_NAME, size=500, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))