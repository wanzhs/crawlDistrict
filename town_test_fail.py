#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import time

import pandas as pd
import requests
import threadpool
from lxml import etree

reload(sys)
sys.setdefaultencoding('utf-8')


def getUrl(url, num_retries=20):
    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"}
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


def getTown(url):
    town = []

    def getData():
        data = getUrl(url=url)
        selector = etree.HTML(data)
        townList = selector.xpath('//tr[@class="towntr"]')

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

    getData()
    df_town = pd.DataFrame(town)
    df_town.info()
    df_town.to_csv('town.csv', sep=',', header=False, index=False)


def complementDistrictCode(code):
    return str(code).ljust(9, '0')


if __name__ == '__main__':
    df_county = pd.read_csv('county.csv', encoding="utf-8", header=0,
                            names=["code", "level", "link", "name", "parentCode"])
    # print df_county['link']
    pool = threadpool.ThreadPool(5)
    requests = threadpool.makeRequests(getTown, df_county['link'])
    [pool.putRequest(req) for req in requests]
    pool.wait()
