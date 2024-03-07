from EsSearch import *
import tensorflow as tf
import numpy as np
import pandas as pd
import timeit
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

# 다음 경기 예측을 위한 전처리
def dnn_pre_predict(today):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')

    yesterday = today - datetime.timedelta(1)
    date = str(yesterday)
    
    # 해당 날짜의 모든 경기를 담을 빈 데이터프레임 생성
    daily_data = pd.DataFrame()
    
    # elasticsearch 'preview' index에서 다음 날짜 경기 정보를 가져옴
    for preview_game in es_searchDayTotalPreivew(today):
        title = preview_game['_source']['명칭']
        game_round = preview_game['_source']['경기 분류']
        game_id = pd.DataFrame([{'game_id' : preview_game['_id']}])
        print(title, game_round)

    # 명칭과 경기 분류 값을 통하여 작년에 같은 경기를 추출하기 위함
    # 경기 분류는 올해보다 작년 값이 한 라운드 더 높게 추출
    # ex) 작년 : 1차전, 올해 : 16차전(마지막)
    #     작년 : 2차전, 올해 : 1차전
    
    # 같은 상대여도 당시 경기 분류에 홈팀과 원정팀이 다를 수 있음.
    # 즉, 명칭도 앞뒤가 달라짐
    # 명칭이 다른 경우 Es에서 index에러가 발생하여 명칭을 앞뒤를 바꿔서 검색하도록 예외처리
        try:
            resultDf_this(title, int(game_round)-1)
        except IndexError:
            title_a = title[0:2]
            title_h = title[2:4]
            title = title_h + title_a
            title
        print('올해 : ', title, int(game_round)-1)
        df_this = resultDf_this(title, int(game_round)-1)

        try:
            resultDf_last(title, game_round)
        except IndexError:
            title_a = title[0:2]
            title_h = title[2:4]
            title = title_h + title_a
            title
        df_last = resultDf_last(title, game_round)
        print('작년 : ', title, game_round)

        # 한 경기 데이터프레임 생성
        df_result = pd.concat([game_id, df_last, df_this],axis=1)
        # 미리 생성한 빈 데이터프레임에 한 줄씩 추가
        # 경기 취소가 없는 경우 일반적으로 하루에 5게임 진행
        daily_data = daily_data.append(df_result, ignore_index=True, sort=False) 

    del daily_data['y']
    daily_data.to_csv('data_result.csv')
    # csv 파일로 저장 후 scaler를 위해 파일 다시 로드
    data = pd.read_csv('./data_result.csv')
    del data['Unnamed: 0']
    del data['game_id']

    #정규화 (MaxAbs) - data
    
    maxAbsScaler = MaxAbsScaler()
    maxAbsScaler.fit(data)
    data_maxabsScaled = maxAbsScaler.transform(data)
    data_maxabs = pd.DataFrame(data_maxabsScaled, columns=data.columns)
    data_maxabs.to_csv('kbo_predict_data_maxabs.csv', index=False)

