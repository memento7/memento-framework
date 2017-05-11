from os import environ

from time import time, sleep
import requests
import pymysql
import json
import pandas as pd

SERVER_RDB = '175.207.13.225'
SERVER_PDB = '175.207.13.224'
SERVER_API = 'http://server1.memento.live:8080/api/persist/'

def get_query_result(host, query, func=lambda x: list(x)):
    conn = pymysql.connect(host=host,
                           user='memento',
                           password=environ['MEMENTO_PASS'],
                           db='memento',
                           charset='utf8')
    cur = conn.cursor()
    result = cur.execute(query)
    ret = func(cur)
    cur.close()
    conn.close()
    return ret

news_columns = ['id', 'keyword', 'title', 'content', 'published_time', 'reply_count', 'href_naver']
comment_columns = ['content']
quote_columns = ['quote']
def get_data(qs, qe):
    def get_comments(ids):
        q = "SELECT " + ",".join(comment_columns) + " FROM naver_comments where target = {} order by reply_count + sympathy_count - antipathy_count desc limit 255"
        for idx in ids:
            res = get_query_result(SERVER_RDB, q.format(idx))
            yield res
    def get_quote(ids):
        q = "SELECT " + ",".join(quote_columns) + " FROM naver_quote where target = {} and flag = 0"
        for idx in ids:
            res = get_query_result(SERVER_RDB, q.format(idx))
            yield res

    q = "SELECT " + ",".join(news_columns) + " FROM naver_news where published_time between \'" + qs + "\' and \'" + qe +"\'"
    f = lambda cur: pd.DataFrame(list(cur), columns=news_columns)
    
    df = get_query_result(SERVER_RDB, q, f)
    df['comments'] = list(get_comments(df.id.values))
    df['quotes'] = list(get_quote(df.id.values))

    return df

headers = { "Content-Type" : "application/json",
            "charset": "utf-8",
            "Authorization": environ['MEMENTO_BASIC']}

keyword_columns = ['id', 'nickname', 'realname', 'subkey']
def get_keywords(headers=headers):
    req = requests.get(SERVER_API + 'entities/waiting', headers=headers)
    return [{c: k[c] for c in keyword_columns} for k in json.loads(req.text)]

keywords = get_keywords()
print (keywords)
def get_keyword_index(keyword):
    for idx, key in enumerate(keywords):
        if key['realname'] == keyword:
            return idx
    return 0

def get_keyword(keyword):
    idx = get_keyword_index(keyword)
    return keywords[idx]

def put_data(host, payload={}, headers=headers):
    while True:
        try:
            # print ('requests-host', host)
            # print ('requests-payload', payload)
            # req = requests.post(host, json=payload, headers=headers)
            # print ('response', req.text)
            # return req.text
            return "{\"id\": 1}"
        except requests.exceptions.ConnectionError:
            print ('ConnectionError')
            print ('wait 0.05 seconds')
            sleep(0.05)
            continue
