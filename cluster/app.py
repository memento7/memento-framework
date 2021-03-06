#!/usr/local/bin/python3
from datetime import datetime, timedelta

import numpy as np
from KINCluster import KINCluster

from pipeline import PipelineServer
from connection import get_navernews, get_entities
from utility import get_similar, filter_quote, Logging
import memento_settings as MS

DATE_START = datetime(2000, 1, 1)
DATE_END = datetime(2017, 5, 30)
DATE_RANGE = timedelta(days=30)
DATE_JUMP = timedelta(days=15)
KEYWORD = '문재인'

#NEED_CONCAT = DATE_JUMP < DATE_RANGE
def process(keyword: str,
            date_start: str,
            date_end: str):
    Logging.register('kin', '<<KINCluster>>: {}')
    frame = get_navernews(keyword, date_start, date_end)

    Logging.logf('kin', '{} has {} news'.format(keyword, frame.shape[0]))
    if frame.empty:
        return;

    simi_list = [" ".join([a, b, filter_quote(c), filter_quote(d)]) for a, b, c, d in zip(frame['title'],
                                                                                          frame['content'],
                                                                                          frame['title_quote'],
                                                                                          frame['content_quote'])]
    frame.loc[:, 'similar'] = get_similar(keyword, simi_list)

    rel_condition = (frame['similar'] > MS.MINIMUM_SIMILAR) & (frame['title'].str.contains(keyword))
    rel_frame = frame.loc[rel_condition]
    rel_size, _ = rel_frame.shape

    Logging.logf('kin', '{} has related {} news'.format(keyword, rel_size))

    if rel_size < MS.MINIMUM_ITEMS:
        return

    kin = KINCluster(PipelineServer(**{
        'keyword': keyword,
        'date_start': date_start,
        'date_end': date_end,
        'frame': rel_frame
    }), settings={
        'EPOCH': 256,
    })
    kin.run()
    del kin

if __name__ == '__main__':
    Logging.register('kin', '<<KINCluster>>: {}')

    print('cluster started!:', DATE_START, 'to', DATE_END)
    Logging.logf('kin', 'Start')

    # entities = get_entities()
    for day in range(int((DATE_END - DATE_START) / DATE_JUMP)):
        date_s = DATE_START + DATE_JUMP * day
        date_e = date_s + DATE_RANGE
        print('date range', date_s, date_e)
        Logging.logf('kin', 'date range {} to {}'.format(date_s, date_e))

        process(**{
            'keyword': KEYWORD,
            'date_start': date_s.strftime("%Y.%m.%d"),
            'date_end': date_e.strftime("%Y.%m.%d"),
        })

        # for entity in entities:
        #     process(**{
        #                 'keyword': entity,
        #                 'date_start': date_s.strftime("%Y.%M.%d"),
        #                 'date_end': date_e.strftime("%Y.%M.%d"),
        #             })
