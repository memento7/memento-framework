from KINCluster.core.pipeline import Pipeline 

import memento_settings as MS

from item import myItem
import connection

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

    def dress_item(self, ext, items):

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
        
        if len(items) < 12: return

        rate = get_memento_rate(items)
        topic = ext.topic
        date = str(topic.published_time)[:10]

        print (topic.content)
        print (topic.comments)
        print (topic.quotes)
        print (ext.keywords)

        self.eid.append(push_event(topic.title, date, ext.keywords, rate, items))