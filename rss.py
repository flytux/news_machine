from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dp
import html
from elasticsearch import Elasticsearch


def extract_text(html_string):
    unesacped_html = html.unescape(html_string)
    soup = BeautifulSoup(unesacped_html)
    text = soup.get_text(strip=True)
    return text

def getNews(feeds: object, INDEX_NAME: object, TYPE_NAME: object) -> object:
    bulk_data = []

    for feed_url in feeds:
        feed = feedparser.parse(feed_url)
        for item in range(len(feed.entries)):
            title = feed.entries[item].title
            description = extract_text(feed.entries[item].description)
            published = dp.parse(feed.entries[item].published)
            summary = extract_text(feed.entries[item].summary)
            link = feed.entries[item].link
            data = {
                "feed_url": feed_url,
                "title": title,
                "description": description,
                "published": published,
                "summary": summary,
                "link": link
            #   "content": content
            }
            index = {
                "index": {
                    "_index": INDEX_NAME,
                    "_type": TYPE_NAME,
                    "_id": title + str(published)
                }
            }
            bulk_data.append(index)
            bulk_data.append(data)
            print(data)
    return bulk_data


def load_elastic(ES_HOST, INDEX_NAME, bulk_data):
    # Connect Elastic Server
    es = Elasticsearch(hosts=[ES_HOST], http_auth=('elastic', '!admin1234'), )

    # Delete Existing Index
    #if es.indices.exists(INDEX_NAME):
    #    print("deleting '%s' index ..." % (INDEX_NAME))
    #    res = es.indices.delete(index=INDEX_NAME)
    #    print(" response: '%s'" % (res))

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

# Setting Data
TECHCRUNCH = "http://techcrunch.com/feed/"
TECHCRUNCH_STARTUP = "http://techcrunch.com/startups/feed/"
TECHINASIA = "https://www.techinasia.com/feed"
A_VC = "http://feeds.feedburner.com/avc"
GOOG_NEWS = "https://news.google.com/news/rss/search/section/q/startups/startups?hl=en&gl=US&ned=us"
Y_COMBI = "https://news.ycombinator.com/rss"

feeds = [TECHCRUNCH, TECHCRUNCH_STARTUP, TECHINASIA, A_VC, GOOG_NEWS, Y_COMBI]
INDEX_NAME = "news"
TYPE_NAME = "news"
ES_HOST = {"host" : "52.78.1.254", "port" : 9200}

# Feeding Data from RSS feeds
bulk_data = getNews(feeds, INDEX_NAME, TYPE_NAME)

# Load data into Elasticsearch
load_elastic(ES_HOST, INDEX_NAME, bulk_data)
