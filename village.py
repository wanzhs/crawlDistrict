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


def getVillage(url_list):
    queue_village = Queue()
    thread_num = 5
    village = []

    def produce_url(url_list):
        for url in url_list:
            queue_village.put(url)

    def getData():
        while not queue_village.empty():
            url = queue_village.get()
            data = getUrl(url=url)
            selector = etree.HTML(data)
            villageList = selector.xpath('//tr[@class="villagetr"]')

            for i in villageList:
                villageCode = i.xpath('td[1]/text()')
                villageName = i.xpath('td[3]/text()')

                for j in range(len(villageName)):
                    parentCode = url[-14:-5]
                    village.append(
                        {'code': complementDistrictCode(villageCode[j]), 'name': villageName[j], 'level': 4,
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
    return village


def complementDistrictCode(code):
    return str(code).ljust(9, '0')


if __name__ == '__main__':
    df_town = pd.read_csv('town.csv', encoding="utf-8",header=0,names=["code","level","link","name","parentCode"])
    print df_town['link']
    # 获取乡级
    # village = getVillage(df_town['link'])
    # df_village = pd.DataFrame(village)
    # df_village.info()
    # df_village.to_csv('village.csv', sep=',', header=True, index=False)
