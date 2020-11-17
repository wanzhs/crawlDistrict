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


def getProvice(url):
    provice = []
    data = getUrl(url)
    selector = etree.HTML(data)
    proviceList = selector.xpath('//tr[@class="provincetr"]')
    for i in proviceList:
        proviceName = i.xpath('td/a/text()')
        proviceLink = i.xpath('td/a/@href')
        for j in range(len(proviceLink)):
            parentCode = 0
            provinceCode = proviceLink[j][0:-5]
            proviceURL = url[:-10] + proviceLink[j]
            provice.append({'name': proviceName[j], 'code': complementDistrictCode(provinceCode), 'link': proviceURL,
                            'level': 0,
                            'parentCode': parentCode})
    return provice


def getCity(url_list):
    city_all = []
    for url in url_list:
        data = getUrl(url)
        selector = etree.HTML(data)
        cityList = selector.xpath('//tr[@class="citytr"]')

        city = []
        for i in cityList:
            cityCode = i.xpath('td[1]/a/text()')
            cityLink = i.xpath('td[1]/a/@href')
            cityName = i.xpath('td[2]/a/text()')

            for j in range(len(cityLink)):
                parentCode = url[-7:-5]
                cityURL = url[:-7] + cityLink[j]
                city.append(
                    {'name': cityName[j], 'code': complementDistrictCode(cityCode[j]), 'link': cityURL, 'level': 1,
                     'parentCode': complementDistrictCode(parentCode)})
        city_all.extend(city)
    return city_all


def getCounty(url_list):
    queue_county = Queue()
    thread_num = 5
    county = []

    def produce_url(url_list):
        for url in url_list:
            queue_county.put(url)

    def getData():
        while not queue_county.empty():
            url = queue_county.get()
            data = getUrl(url=url)
            selector = etree.HTML(data)
            countyList = selector.xpath('//tr[@class="countytr"]')

            for i in countyList:
                countryCode = i.xpath('td[1]/a/text()')
                countyLink = i.xpath('td[1]/a/@href')
                countyName = i.xpath('td[2]/a/text()')

                for j in range(len(countyLink)):
                    parentCode = url[-9:-5]
                    countyURL = url[:-9] + countyLink[j]
                    county.append(
                        {'code': complementDistrictCode(countryCode[j]), 'link': countyURL, 'name': countyName[j],
                         'level': 2,
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
    return county

def complementDistrictCode(code):
    return str(code).ljust(9, '0')


if __name__ == '__main__':
    # 获取省份
    pro = getProvice("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html")
    df_province = pd.DataFrame(pro)
    df_province.info()
    df_province.to_csv('province.csv', sep=',', header=True, index=False)
    # 获取市级
    city = getCity(df_province['link'])
    df_city = pd.DataFrame(city)
    df_city.info()
    df_city.to_csv('city.csv', sep=',', header=True, index=False)
    # 获取县级
    county = getCounty(df_city['link'])
    df_county = pd.DataFrame(county)
    df_county.info()
    df_county.to_csv('county.csv', sep=',', header=True, index=False)