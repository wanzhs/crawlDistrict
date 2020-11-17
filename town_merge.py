#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

import pandas as pd

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    a = 30
    df_town=pd.DataFrame()
    for a in range(30):
        df_town_sub = pd.read_csv('town' + str(a) + '.csv', encoding="utf-8", header=0,
                                names=["code", "level", "link", "name", "parentCode"])
        df_town=pd.concat([df_town,df_town_sub],axis=0)
    # 获取镇级
    # print town
    # df_town = pd.DataFrame(town)
    df_town.info()
    df_town.to_csv('town.csv', sep=',', header=True, index=False)
