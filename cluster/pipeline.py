from KINCluster import Pipeline 

import memento_settings as MS

from utility import get_similar
from item import myItem
import connection

import numpy as np

from itertools import chain
import json

class PipelineServer(Pipeline):
    def __init__(self, kid, keyword, frame):
        self.kid = kid
        self.keyword = keyword
        self.frame = frame
        self.eid = []

    def __del__(self):
        for eid in self.eid:
            put_data(SERVER_API + 'entities/' + str(self.kid) + '/events/' + str(eid))

    def capture_item(self):
        items = []
        for idx, row in self.frame.iterrows():
            items.append(myItem(keyword=row.keyword, title=row.title, content=row.content,\
                                published_time=row.published_time, reply_count=row.reply_count,\
                                href_naver=row.href_naver,comments=list(chain(*row.comments)),quotes=list(chain(*row.quotes))))
        return items

    def dress_item(self, ext):

        def push_event(title, date, keywords, rate, items):

            def push_article(eid, title, href, cc):
                payload = {
                  "comment_count": cc,
                  "crawl_target": "NAVERNEWS",
                  "source_url": href,
                  "summary": "",
                  "title": title
                }
                connection.put_data(SERVER_API + 'events/' + str(eid) + '/articles', payload)

            payload = {
                "date" : date + " 00:00:00",
                "title" : title,
                "type" : '연예',
                "status" : 0,
                "issue_score" : rate,
                "emotions" : [],
                "keywords" : [ {"keyword": k, "weight": v} for k, v in keywords],
                "summaries" : []
            }
            res = connection.put_data(SERVER_API + 'events', payload)
            res = json.loads(res)
            # print ('response-find-id', res)
            eid = res['id']

            for item in items:
                connection.push_article(eid, item.title, item.href_naver, item.reply_count)

            return eid

        def get_memento_rate(items):
            return sum([item.reply_count + 1000 for item in items])
        
        filter_quote = lambda x: " ".join(["".join(y) for y in x])
        def get_property(item):
            return " ".join([item.title, item.content, filter_quote(item.quotes)])

        if len(ext.items) < 10: return

        rate = get_memento_rate(ext.items)
        simi = get_similar(self.keyword, map(lambda x: str(x), ext.items))
        topic = ext.items[np.argmax(simi)]
        date = str(topic.published_time)[:10]

        self.eid.append(push_event(topic.title, date, ext.keywords, rate, ext.items))