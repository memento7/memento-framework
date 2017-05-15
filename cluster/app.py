from KINCluster import KINCluster 
from pipeline import PipelineServer
from connection import get_navernews, get_keyword
from utility import get_similar, filter_quote

import numpy as np

from datetime import datetime, timedelta

date_start = datetime(2015,1,1)
date_end = datetime(2015,1,31)
date_range = timedelta(days=30)
date_jump = timedelta(days=15)

minimum_items = 10
def process():
    for day in range(int((date_end - date_start) / date_jump)):  
        date_s = date_start + date_jump * day
        date_e = date_s + date_range
        print ('date range', date_s, date_e)

        df = get_navernews(date_s.strftime("%Y/%m/%d"), date_e.strftime("%Y/%m/%d"))

        for keyword in np.unique(df.keyword.values):
            key = get_keyword(keyword)
            kid = key['id']

            print ('start with keyword', keyword)

            kf = df.loc[df.keyword == keyword]
            kf['similar'] = get_similar(keyword, [" ".join([a,b,filter_quote(c)]) for a,b,c in zip(kf['title'], kf['content'], kf['quotes'])])
            rf = kf.loc[(kf['similar'] > 0.03) & (kf['title'].str.contains(keyword))]

            kh, kw = kf.shape
            rh, rw = rf.shape

            if rh < minimum_items: continue
            print ('there are', rh, 'news')

            kin = KINCluster(PipelineServer(kid, keyword, kf))
            kin.run()
            del kin

if __name__ == '__main__':
    process()
