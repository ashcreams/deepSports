from EsSearch import *
from numFormat import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler
from sklearn.model_selection import train_test_split
from imblearn.combine import *

import tensorflow as tf
from tensorflow.keras import utils
from tensorflow.keras.utils import to_categorical
from tensorflow.keras import layers, models
from tensorflow.keras.callbacks import EarlyStopping
# from tensorflow.keras.utils import np_utils #from tensorflow.keras import utils -> utils.to_categorical

import json
import joblib
import time

# 오늘 날짜의 경기 예측
def dnn_predict(today):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')
    # 학습된 모델 불러오기
    model = models.load_model('./model_kbo.h5')
    # 전처리 준비 된 데이터 불러오기
    prepared_data = pd.read_csv('./kbo_predict_data_maxabs.csv')
    # 오늘 날짜의 경기 id 리스트 불러오기
    # 전처리 된 경기가 어떤 경기인지 확인하기 어려워 경기 id를 가져와서 확인하기 위함
    title_data = pd.read_csv('./data_result.csv')
    # 예측
    predict = model.predict(prepared_data) 

    game_title = []
    today = date.today()

    # es 'preview' index에 저장된 경기 정보 가져오기
    for preview_game in es_searchDayTotalPreivew(today):
        title = preview_game['_source']['명칭']
        game_round = preview_game['_source']['경기 분류']
        # 경기 명칭 추출하여 빈 리스트에 추가
        game_title.append(title)

    for i, pred in enumerate(predict):
        # 경기 id
        game_str = title_data['game_id'][i]
        # 원정 팀 약칭
        title_a = replace_db_team(game_title[i][0:2])
        # 홈 팀 약칭
        title_h = replace_db_team(game_title[i][2:4])
        # 팀 약칭 숫자화
        num_a = replace_team_num(game_title[i][0:2])
        num_h = replace_team_num(game_title[i][2:4])

        per = 0
        
        # 11개의 라벨 중에 해당 두 팀에 해당 되는 값을 비교하여 값이 더 큰 팀이 승리 
        if pred[num_a] > pred[num_h]:
            pred_result = title_a
            away_result = '승 (예측)'
            home_result = '패 (예측)'
        elif pred[num_a] < pred[num_h]:
            pred_result = title_h
            away_result = '패 (예측)'
            home_result = '승 (예측)'
        
        # 딕셔너리로 정리
        predict_dict = {'날짜' : str(today), '명칭' : game_title[i], '원정 팀' : title_a, '홈 팀' : title_h, 
                        '예측 승리팀' : pred_result, '원정 팀 예측' : away_result , '홈 팀 예측' : home_result, '승리 확률' : ''}
        
        # es 'predict' index로 저장
        response = es_client.index(index='predict', id= game_str, doc_type='_doc', body=predict_dict)
    #     print(pred_result+' 승리', per, game_title[i])
        print(predict_dict)
