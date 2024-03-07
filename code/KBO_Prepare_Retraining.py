from makeResultData import *
import tensorflow as tf
import numpy as np
import pandas as pd
import timeit
import datetime
from tensorflow import keras
import elasticsearch

import pandas as pd
import json
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from tensorflow.compat.v1 import ConfigProto
from tensorflow.compat.v1 import InteractiveSession

# 경기 종료 된(작업 기준으로 어제) 어제 데이터 전체 가져오기
def es_yesterdayResult(yesterday):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')

    if es_client.indices.exists(index="day_result"):
        response = es_client.search(index='day_result', size=800, body={
            'query': {
                'bool': {
                    'filter': {
                        'bool':{
                            'must':[
                                {
                                 'match': {
                                     '날짜': yesterday
                                 }
                                }
                            ]
                    }
                }
            }
        }})
    
    return response['hits']['hits']

# 경기 종료 된(작업 기준으로 어제) 어제 데이터와 동일한 작년 데이터를 추출하여 예측용 데이터프레임 생성
def prepare_trainData(yesterday, lastYear, thisYear):
    daily_data = pd.DataFrame()
    for game in es_yesterdayResult(yesterday):

        title = game['_source']['명칭']
        game_round = game['_source']['경기 분류']
        this_round = int(game_round-1)
    
        if game_round == 1:
            try:
                df_this = resultDf_this(title, 16, lastYear)
            except IndexError:
                title_a = title[0:2]
                title_h = title[2:4]
                title = title_h + title_a
                title
                df_this = resultDf_this(title, 16, lastYear)

        else:
            df_this = resultDf_this(title, this_round, thisYear)

        try:
            df_last = resultDf_last(title, game_round, lastYear)
        except IndexError:
            title_a = title[0:2]
            title_h = title[2:4]
            title = title_h + title_a
            title
            df_last = resultDf_last(title, game_round, lastYear)

        df_result = pd.concat([df_last, df_this],axis=1)

        daily_data = daily_data.append(df_result, ignore_index=True, sort=False)
        
    return daily_data


def pre_train(today):
    yesterday = today - datetime.timedelta(1)
    today_year = int(today.strftime('%Y'))
    thisYear = str(today_year)
    lastYear = str(today_year-1)

    daily_data = prepare_trainData(yesterday, lastYear, thisYear)
    data = pd.read_csv('./data_set.csv')
    del data['Unnamed: 0']
    data = data.append(daily_data)
    data.to_csv('data_set.csv')
    
#정규화 (MaxAbs) - data_x
    data = pd.read_csv('./data_set.csv')
    del data['Unnamed: 0']

    target_y = 'y' #result
    test_size=0.2 #비율

    data_x, data_y = data, data.pop(target_y)
    data_y = pd.DataFrame(data_y, columns=[target_y])

    train_x, test_x, train_y, test_y = train_test_split(data_x, data_y, test_size=test_size, 
                                                        shuffle=True, stratify=data_y, random_state=34)

    train_data = train_x.copy()
    train_data['y'] = train_y

    test_data = test_x.copy()
    test_data['y'] = test_y

    # 전처리된 전체 데이터 : df, 데이터/라벨 분리
    df = data_x.copy()
    df_x = data_x.copy()
    df_y = data_y.copy()
    df['y'] = data_y['y']

    # 전처리된 학습 데이터 : df_train, 데이터/라벨 분리
    df_train = train_data.copy()
    df_train_x = train_x.copy()
    df_train_y = train_y.copy()

    # 전처리된 학습 데이터 : df_test, 데이터/라벨 분리
    df_test = test_data.copy()
    df_test_x = test_x.copy()
    df_test_y = test_y.copy()

    #정규화 (MaxAbs) - data_x
    maxAbsScaler = MaxAbsScaler()
    maxAbsScaler.fit(df_x)
    df_x_maxabsScaled = maxAbsScaler.transform(df_x)
    df_x_maxabs = pd.DataFrame(df_x_maxabsScaled, columns=df_x.columns)

    #정규화 (MaxAbs) - train_x
    maxAbsScaler2 = MaxAbsScaler()
    maxAbsScaler2.fit(df_train_x)
    df_train_x_maxabsScaled = maxAbsScaler2.transform(df_train_x)
    df_train_x_maxabs = pd.DataFrame(df_train_x_maxabsScaled, columns=df_train_x.columns)

    #정규화 (MaxAbs) - test_x
    df_test_x_maxabsScaled = maxAbsScaler2.transform(df_test_x)
    df_test_x_maxabs = pd.DataFrame(df_test_x_maxabsScaled, columns=df_test_x.columns)

    df_maxabs = df_x_maxabs.copy()
    df_maxabs['y'] = df_y['y']

    df_maxabs.to_csv('./kbo_data_prepared_maxabs.csv', index=False)

    df_train_maxabs = df_train_x_maxabs.copy()
    df_train_maxabs['y'] = df_train_y['y'].reset_index(drop=True) # df_train_y의 index를 초기화(0,1,2)해서 index 순으로 merge하게 설정

    df_train_maxabs.to_csv('./kbo_data_prepared_train_maxabs.csv', index=False)

    df_test_maxabs = df_test_x_maxabs.copy()
    df_test_maxabs['y'] = df_test_y['y'].reset_index(drop=True)

    df_test_maxabs.to_csv('./kbo_data_prepared_test_maxabs.csv', index=False)