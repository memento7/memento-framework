from os import environ

from time import time, sleep
import requests
import pymysql
import json
import pandas as pd

SERVER_PDB = 'server1.memento.live'
SERVER_RDB = 'server2.memento.live'
SERVER_API = 'https://api.memento.live/persist/'

def connection(host=SERVER_RDB, user='memento', password=environ['MEMENTO_PASS'], db='memento', charset='utf8mb4'):
    conn = pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db,
                           charset=charset)
    cur = conn.cursor()
    return conn, cur

def disconnect(conn, cur):
    cur.close()
    conn.close()

def get_query_result(query, cur, func=lambda x: list(x), debug=False):
    if debug:
        print('execute query:', query)
    result = cur.execute(query)
    return func(cur)

news_columns = ['id', 'keyword', 'title', 'content', 'published_time', 'reply_count', 'href_naver']
comment_columns = ['content']
quote_columns = ['quote']
def get_navernews(qs, qe):
    conn, cur = connection()

    def get_comments(ids, cur=cur):
        q = "SELECT " + ",".join(comment_columns) + " FROM naver_comment where target = {} order by reply_count + sympathy_count - antipathy_count desc limit 255"
        for idx in ids:
            res = get_query_result(q.format(idx), cur)
            yield res
    def get_quote(ids, cur=cur):
        q = "SELECT " + ",".join(quote_columns) + " FROM naver_quote where target = {} and flag = 0"
        for idx in ids:
            res = get_query_result(q.format(idx), cur)
            yield res

    q = "SELECT " + ",".join(news_columns) + " FROM naver_news where published_time between \'" + qs + "\' and \'" + qe +"\'"
    f = lambda c: pd.DataFrame(list(c), columns=news_columns)
    df = get_query_result(q, cur, f, debug=True)
    df['comments'] = list(get_comments(df.id.values))
    df['quotes'] = list(get_quote(df.id.values))

    disconnect(conn, cur)
    return df

def get_entities():
    conn, cur = connection()

    def get_entity(target, cur=cur):
        def _get_(table, target):
            q = "SELECT * FROM entity_{} WHERE target={}".format(table, target)
            f = lambda c: [tuple(v for k,v in zip(c.description, x) if not k[0] in ['id', 'target', 'flag']) for x in c.fetchall()]
            return get_query_result(q, cur, f)

        return {table: _get_(table, target) for table in ['accent', 'link', 'strike', 'tag']}

    q = "SELECT id, keyword FROM entity"
    f = lambda c: [{k[0]:v for k,v in zip(c.description, x)} for x in c.fetchall()]
    r = get_query_result(q, cur, f, debug=True)

    entities = {entity['keyword']: get_entity(entity['id']) for entity in r}
    
    disconnect(conn, cur)
    return entities

headers = { "Content-Type" : "application/json",
            "charset": "utf-8",
            "Authorization": environ['MEMENTO_BASIC']}

keyword_columns = ['id', 'nickname', 'realname', 'subkey']
def get_keywords(headers=headers):
    req = requests.get(SERVER_API + 'entities/waiting', headers=headers)
    try:
        res = json.loads(req.text)
    except:
        raise 'api server error, cannot get entities/waiting'

    return [{c: k[c] for c in keyword_columns} for k in res]

keywords = get_keywords()
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
            print ('requests-host', host)
            print ('requests-payload', payload)
            req = requests.post(host, json=payload, headers=headers)
            print ('response', req.text)
            return req.text
            # return "{\"id\": 1}"
        except requests.exceptions.ConnectionError:
            print ('ConnectionError')
            print ('wait 0.05 seconds')
            sleep(0.05)
            continue
