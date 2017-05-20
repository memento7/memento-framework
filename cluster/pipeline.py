from itertools import chain
import json

from KINCluster import Pipeline
import numpy as np

from utility import get_similar, get_emotion, filter_quote, Logging
from item import myItem as Item
import connection
import memento_settings as MS

class PipelineServer(Pipeline):
    def __init__(self, kid, keyword, frame):
        self.kid = kid
        self.keyword = keyword
        self.frame = frame
        self.eid = []
        self.cluster = []

    def __del__(self):
        for eid in self.eid:
            connection.put_data(MS.SERVER_API + 'entities/' + str(self.kid) + '/events/' + str(eid))

    def capture_item(self):
        items = []
        for _, row in self.frame.iterrows():
            items.append(Item(keyword=row.keyword,
                              title=row.title,
                              content=row.content,
                              published_time=row.published_time,
                              reply_count=row.reply_count,
                              href_naver=row.href_naver,
                              cate=row.cate,
                              imgs=list(chain(*row.imgs)),
                              comments=list(chain(*row.comments)),
                              quotes=list(chain(*row.quotes))))
        return items

    def dress_item(self, ext):

        def push_event(topic, keywords, rate, items, emotions):

            def push_article(eid, title, href, comment_count, imgs):
                payload = {
                    "comment_count": comment_count,
                    "crawl_target": "네이버 뉴스",
                    "source_url": href,
                    "summary": "",
                    "title": title
                }
                if imgs:
                    payload['image'] = {
                        "url": imgs[0],
                        "source_link": href,
                        "weight": 5
                    }
                connection.put_data(MS.SERVER_API + 'events/' + str(eid) + '/articles', payload)

            payload = {
                "date" : str(topic.published_time)[:10] + " 00:00:00",
                "title" : topic.title,
                "type" : topic.cate,
                "status" : 0,
                "issue_score" : rate,
                "emotions" : [{
                    "emotion": emotion[0],
                    "weight": emotion[1]
                } for emotion in emotions],
                "images": [{
                    "url": img,
                    "source_url": topic.href_naver,
                    "weight": 10
                } for img in topic.imgs],
                "keywords" : [{
                    "keyword": k,
                    "weight": v * 100
                } for k, v in keywords],
                "summaries" : []
            }
            res = connection.put_data(MS.SERVER_API + 'events', payload)
            try:
                res = json.loads(res)
            except:
                Logging.log('<<put event error>>:', res)
                raise 'Error here why?'
            # print ('response-find-id', res)
            eid = res['id']

            for item in items:
                push_article(eid, item.title, item.href_naver, item.reply_count, item.imgs)

            return eid

        def get_memento_rate(items):
            return sum([item.reply_count + 1000 for item in items])
        
        def get_property(item):
            return " ".join([item.title, item.content, filter_quote(item.quotes)])

        if len(ext.items) < MS.MINIMUM_CLUSTER: 
            return

        rate = get_memento_rate(ext.items)
        emot = get_emotion("\n".join(chain(*list(map(lambda x: x.comments, ext.items)))), [self.keyword])
        simi = get_similar(self.keyword, list(map(get_property, ext.items)))
        simi = [(self.keyword in item.title and v + 0.03 or v) for v, item in zip(simi, ext.items)]
        topic = ext.items[np.argmax(simi)]
        
        self.eid.append(push_event(topic, ext.keywords, rate, ext.items, emot))
