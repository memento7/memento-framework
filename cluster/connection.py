from os import environ
from time import sleep
import json

from utility import Logging
import memento_settings as MS

import requests
import pymysql
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def connection(kwargs=MS.SERVER_RDB_INFO):
    conn = pymysql.connect(**kwargs,
                           cursorclass=pymysql.cursors.SSCursor)
    cur = conn.cursor()
    return conn, cur

def disconnect(conn, cur):
    cur.close()
    conn.close()

def get_query_result(query, cur, func=list, debug=False):
    if debug:
        Logging.log('<<Query Executed>>: {}'.format(query))
    result = cur.execute(query)
    if debug:
        Logging.log('<<Query Result>>: {}'.format(result))
    return func(cur)

@Logging
def get_navernews(date_start, date_end, columns=['id', 'keyword', 'title', 'content', 'published_time', 'reply_count', 'href_naver', 'cate']):
    conn, cur = connection()

    def get_comments(ids, cur=cur, columns=['content']):
        query = "SELECT {} FROM naver_comment WHERE target = {} ORDER BY {} DESC LIMIT {}".format(
            ",".join(columns), "{}", 'reply_count + sympathy_count - antipathy_count', 255)
        for idx in ids:
            res = get_query_result(query.format(idx), cur)
            yield res

    def get_imgs(ids, cur=cur, columns=['src']):
        query = "SELECT {} FROM naver_img WHERE target = {}".format(
            ",".join(columns), "{}")
        for idx in ids:
            res = get_query_result(query.format(idx), cur)
            yield res

    def get_quote(ids, cur=cur, columns=['quote']):
        query = "SELECT {} FROM naver_quote WHERE target = {} AND flag = {}".format(
            ",".join(columns), "{}", 0)
        for idx in ids:
            res = get_query_result(query.format(idx), cur)
            yield res

    query = "SELECT {} FROM naver_news WHERE published_time BETWEEN \'{}\' AND \'{}\'".format(
        ",".join(columns), date_start, date_end)
    func = lambda c: pd.DataFrame(list(c), columns=columns)
    result_frame = get_query_result(query, cur, func, debug=True)
    result_frame['imgs'] = list(get_imgs(result_frame.id.values))
    result_frame['comments'] = list(get_comments(result_frame.id.values))
    result_frame['quotes'] = list(get_quote(result_frame.id.values))

    total_bytes = get_query_result("SHOW SESSION STATUS LIKE 'bytes_sent'", cur)
    Logging.log('<<read total bytes>>: {}'.format(total_bytes))
    disconnect(conn, cur)
    return result_frame

@Logging
def get_entities():
    conn, cur = connection()

    def get_entity(target, cur=cur):
        def _get_(table, target):
            query = "SELECT * FROM entity_{} WHERE target={}".format(table, target)
            func = lambda c: [tuple(v for k, v in zip(c.description, x) if not k[0] in ['id', 'target', 'flag']) for x in c.fetchall()]
            return get_query_result(query, cur, func)

        return {table: _get_(table, target) for table in ['accent', 'link', 'strike', 'tag']}

    query = "SELECT id, keyword FROM entity"
    func = lambda c: [{k[0]:v for k, v in zip(c.description, x)} for x in c.fetchall()]
    result = get_query_result(query, cur, func, debug=True)

    entities = {entity['keyword']: get_entity(entity['id']) for entity in result}
    disconnect(conn, cur)
    return entities

HEADERS = {"Content-Type" : "application/json",
           "charset": "utf-8",
           "Authorization": environ['MEMENTO_BASIC']}

@Logging
def get_keywords(headers=HEADERS, columns=['id', 'nickname', 'realname', 'subkey']):
    req = requests.get(MS.SERVER_API + 'entities/waiting', headers=headers, verify=True)
    try:
        res = json.loads(req.text)
    except:
        raise 'api server error, cannot get entities/waiting'

    return [{c: k[c] for c in columns} for k in res]

keywords = get_keywords()

@Logging
def get_keyword(keyword):
    def get_keyword_index(keyword):
        for key in keywords:
            if key['nickname'] == keyword:
                return key['id']
        raise 'no entity named ' + keyword
    return keywords[get_keyword_index(keyword) - 1]

PAYLOAD = {"default": "default"}
@Logging
def put_data(host, payload=PAYLOAD, headers=HEADERS):
    while True:
        try:
            # Logging.log('requests-host: {}'.format(host), 'DEBUG')
            # Logging.log('requests-payload: {}'.format(payload), 'DEBUG')
            # req = requests.post(host, json=payload, headers=headers)
            # Logging.log('response: {}'.format(req.text), 'DEBUG')
            # return req.text
            return "{\"id\": 1}"
        except requests.exceptions.ConnectionError:
            Logging.log('ConnectionError')
            Logging.log('wait 3 seconds')
            sleep(3)
            continue
