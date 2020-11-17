#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import time
from Queue import Queue
from threading import Thread

import pandas as pd
import requests
from lxml import etree

reload(sys)
sys.setdefaultencoding('utf-8')


def getUrl(url, num_retries=20):
    headers = {
        'Cookie':'SF_cookie_1=37059734; _trs_uv=khk5hseb_6_8qjf; __utma=207252561.1278509431.1605582567.1605582567.1605582567.1; __utmc=207252561; __utmz=207252561.1605582567.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=207252561.14.10.1605582567; wzws_cid=85693aa680301909619977192cd74400c1fbb526816e307e57b81cb0e7b5c3d92577e1755e1477cedcb83a12c0ad7dea1ad2977d69d03c256bb96ceea40ca5df0f38e3c98d44b299ac11bf802b52e8eb',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'GBK'
        data = response.text
        return data
    except BaseException as e:
        if num_retries > 0:
            time.sleep(10)
            print(url)
            print("request fail,retry!")
            return getUrl(url, num_retries - 1)

        else:
            print("retry fail!")
            print("errors:%s" % e + "" + url)


def getTown(url_list):
    queue_town = Queue()
    thread_num = 5
    town = []

    def produce_url(url_list):
        for url in url_list:
            queue_town.put(url)

    def getData():
        while not queue_town.empty():
            url = queue_town.get()
            data = getUrl(url=url)
            selector = etree.HTML(data)
            townList = selector.xpath('//tr[@class="towntr"]')
            if len(townList) == 0:
                queue_town.put(url)
                continue
            for i in townList:
                townCode = i.xpath('td[1]/a/text()')
                townLink = i.xpath('td[1]/a/@href')
                townName = i.xpath('td[2]/a/text()')

                for j in range(len(townLink)):
                    parentCode = url[-11:-5]
                    townURL = url[:-11] + townLink[j]
                    town.append(
                        {'code': complementDistrictCode(townCode[j]), 'link': townURL, 'name': townName[j], 'level': 3,
                         'parentCode': complementDistrictCode(parentCode)})

    def run(url_list):
        produce_url(url_list)

        ths = []
        for _ in range(thread_num):
            th = Thread(target=getData)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

    run(url_list)
    return town


def complementDistrictCode(code):
    return str(code).ljust(9, '0')


if __name__ == '__main__':
    # df_county = pd.read_csv('county.csv', encoding="utf-8",header=0,names=["code","level","link","name","parentCode"])
    readRows = 100
    a = 30
    # for a in range(5,7):
    df_county = pd.read_csv('county.csv', encoding="utf-8", header=0,
                            names=["code", "level", "link", "name", "parentCode"],
                            # usecols=['link'],
                            nrows=readRows,
                            skiprows=a * readRows)
    # print df_county
    # 获取镇级
    town = getTown(df_county['link'])
    df_town = pd.DataFrame(town)
    df_town.info()
    df_town.to_csv('town'+str(a)+'.csv', sep=',', header=True, index=False)
