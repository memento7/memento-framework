from datetime import datetime, timedelta

import numpy as np
from KINCluster import KINCluster

from pipeline import PipelineServer
from connection import get_navernews, get_keyword
from utility import get_similar, filter_quote
import memento_settings as MS

DATE_START = datetime(2016, 1, 1)
DATE_END = datetime(2016, 12, 31)
DATE_RANGE = timedelta(days=30)
DATE_JUMP = timedelta(days=15)

#NEED_CONCAT = DATE_JUMP < DATE_RANGE
def process():
    print('cluster started!:', DATE_START, 'to', DATE_END)
    for day in range(int((DATE_END - DATE_START) / DATE_JUMP)):
        date_s = DATE_START + DATE_JUMP * day
        date_e = date_s + DATE_RANGE
        print('date range', date_s, date_e)

        news_frame = get_navernews(date_s.strftime("%Y/%m/%d"), date_e.strftime("%Y/%m/%d"))

        for keyword in np.unique(news_frame.keyword.values):
            key = get_keyword(keyword)
            kid = key['id']

            print('start with keyword', keyword)

            key_frame = news_frame.loc[news_frame.keyword == keyword]
            simi_list = [" ".join([a, b, filter_quote(c)]) for a, b, c in zip(key_frame['title'], key_frame['content'], key_frame['quotes'])]
            key_frame.loc[:, 'similar'] = get_similar(keyword, simi_list)
            rel_condition = (key_frame['similar'] > MS.MINIMUM_SIMILAR) & (key_frame['title'].str.contains(keyword))
            rel_frame = key_frame.loc[rel_condition]

            rel_size, _ = rel_frame.shape

            if rel_size < MS.MINIMUM_ITEMS:
                continue
            print('there are', rel_size, 'news')

            kin = KINCluster(PipelineServer(kid, keyword, rel_frame), settings={
                'EPOCH': 256,
            })
            kin.run()
            del kin

if __name__ == '__main__':
    process()
