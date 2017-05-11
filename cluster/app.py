from KINCluster.KINCluster import KINCluster
from pipeline import PipelineServer
from connection import get_data, get_keyword

import numpy as np

from datetime import datetime, timedelta

date_start = datetime(2015,1,1)
date_end = datetime(2015,12,31)
date_range = timedelta(days=30)
date_jump = timedelta(days=15)

minimum_items = 10
def process():
    for day in range(int((date_end - date_start) / date_jump)):  
        date_s = date_start + date_jump * day
        date_e = date_s + date_range
        print ('date range', date_s, date_e)

        df = get_data(date_s.strftime("%Y/%m/%d"), date_e.strftime("%Y/%m/%d"))

        for keyword in np.unique(df.keyword.values):
            key = get_keyword(keyword)
            kid = key['id']

            print ('start with keyword', keyword)

            kf = df.loc[df.keyword == keyword]
            h, w = kf.shape
            if h < minimum_items: continue
            print ('there are', h, 'news')

            kin = KINCluster(PipelineServer(kid, keyword, kf))
            kin.run()
            del kin

if __name__ == '__main__':
    process()
