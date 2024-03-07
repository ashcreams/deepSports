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



def DNN(Nin, Nh_l, Nout, d_out):
    model = models.Sequential()
    # 첫 번째 은닉층 : N in, Nh_l[1] out & relu(sigmoid 사용시 역전파시 성능 저하)
    model.add(layers.Dense(Nh_l[0], activation='relu', input_shape=(Nin,), name='Hidden-0'))
    # 두 번쨰 은닉층 : N[idx] in, N[idx+1] out & relu
    for idx, el in enumerate(Nh_l):
        if not(idx == 0):
            layer_name = 'Hidden-'+str(idx)
            model.add(layers.Dense(Nh_l[idx], activation='relu', name=layer_name))
            layers.Dropout(d_out)
    # softmax를 사용하여 출력 값들의 합이 1이 되도록 만들어 준다.
    model.add(layers.Dense(Nout, activation='softmax'))
    # 손실함수 : 교차 엔트로
    model.compile(loss='categorical_crossentropy',optimizer='adam',metrics=[tf.keras.metrics.CategoricalAccuracy()])
#     ['accuracy']
    return model


def testResultEnsembleWeight(predictions, test_y, filename, gp=90., bp=30., weight=1.):
    classes = ('0','1','2','3','4','5','6','7','8','9','10')    
    test_loss=0
    correct=0
    val_loss_summary = 0.0
    class_correct = list(0. for i in range(11))
    class_total = list(0. for i in range(11))  
    class_t = list(0. for i in range(11))
    
    true_list = []
    false_list = []
    true_num = 0
    false_num = 0
       
    for index, row in test_y.iterrows():

        result = 0
        target = 0

        c = predictions[index] 
        
        # 확인값
        result = int(np.argmax(c))

        target_label = int(np.argmax(row))
        
        
        print('prediction : %d,\ttest : %d'%(result,target_label))
        
        if target_label == result:
            true_list.append('True')
            true_num = len(true_list)
        else:
            false_list.append('False')
            false_num = len(false_list)
        
        total_num = true_num+false_num  
        print('예측률 : {:.2f}%'.format((true_num/total_num)*100))
    
    print('예측성공 : {0}, 예측실패 : {1}'.format(true_num, false_num))
    
    
# DNN을 사용하여 학습을 한다.
def modelTrainning(Ni, Nh, No, do, t_size, b_size, train_x, train_y, test_x, test_y):
    early_stopping = EarlyStopping(patience=100, verbose=1)
    
    train_X, val_x, train_Y, val_y = train_test_split(train_x, train_y, test_size=t_size, shuffle=True, 
                                                      stratify=train_y, random_state=int(time.time()))
    
    model = DNN(Ni, Nh, No, do)
    history = model.fit(#train_x, train_y, 
                        train_X, train_Y, 
                        shuffle=False,validation_data=(val_x,val_y), # 조합=>train/validation loss graph 불안정, epochs=300
                        #shuffle=True,validation_data=(val_x,val_y)  # 조합=>early_stopping 호출 안됨, patience=10, epochs=300
                        epochs=1000, batch_size=b_size, 
                        #validation_split=0.0, #t_size, 
                        verbose=0,
                        callbacks=[early_stopping])
    performace_test = model.evaluate(test_x, test_y, batch_size=b_size, verbose=0)
    print('Test Loss and Accuracy ->', performace_test)
    model.save('./model_kbo.h5')
        
    predictions = model.predict(test_x) 
    
    return model, history, predictions, train_X


def model_train(loop_num):
    # GPU 선택
    gpus = tf.config.experimental.list_physical_devices('XLA_GPU')
    gpu_no = 1 

    # METRICS 정의
    METRICS = [
          tf.metrics.TruePositives(name='tp'),
          tf.metrics.FalsePositives(name='fp'),
          tf.metrics.TrueNegatives(name='tn'),
          tf.metrics.FalseNegatives(name='fn'), 
          tf.metrics.BinaryAccuracy(name='accuracy'),
          tf.metrics.Precision(name='precision'),
          tf.metrics.Recall(name='recall'),
          tf.metrics.AUC(name='auc'),
    ]

    target_y = 'y' #result
    test_size=0.2 #비율

    data = pd.read_csv('./kbo_data_prepared_maxabs.csv') # 전처리 & 정규화 된 데이터

    data_x, data_y = data, data.pop(target_y)
    data_y = pd.DataFrame(data_y, columns=[target_y])

    train_x, test_x, train_y, test_y = train_test_split(data_x, data_y, test_size=test_size, shuffle=True, 
                                                        stratify=data_y, random_state=34)

    # 파일 읽어오기

    pre = False
    if pre:
        data = pd.read_csv('./kbo_data_prepared_maxabs.csv') # 전처리 & 정규화 된 데이터
        data_x, data_y = data, data.pop(target_y)
        data_y = pd.DataFrame(data_y, columns=[target_y])
        train_x, test_x, train_y, test_y = train_test_split(data_x, data_y, test_size=test_size, shuffle=True, 
                                                            stratify=data_y, random_state=34)
    elif not pre:
        train_x = pd.read_csv('./kbo_data_prepared_train_maxabs.csv') # 전처리 & 정규화 된 데이터 80% (훈련 데이터)
        train_x, train_y = train_x, train_x.pop(target_y)
        train_y = pd.DataFrame(train_y, columns=[target_y])

        test_x = pd.read_csv('./kbo_data_prepared_test_maxabs.csv') # 전처리 & 정규화 된 데이터 20% (테스트 데이터)
        test_x, test_y = test_x, test_x.pop(target_y)
        test_y = pd.DataFrame(test_y, columns=[target_y])

    N, D = train_x.shape

    # 레이어
    Nin = D # train_x.shape[1]
    Nh_l = [Nin, 400, 200, 100, 50, 25] #11까지 줄이면 X
    number_of_class = 11
    Nout = number_of_class
    d_out = 0.2
    nStep = 400
    split_ratio=0.2; batch_size=20

    cat_w = ['무', 'nc', 'ht', 'ob', 'sk', 'hh', 'ss', 'kt', 'lg', 'wo', 'lt']

    train_y = utils.to_categorical(train_y)
    test_y = utils.to_categorical(test_y)

    train_y = pd.DataFrame(train_y, columns=[cat_w])
    test_y = pd.DataFrame(test_y, columns=[cat_w])
    
    for i in range(loop_num):
        train_model, history, predictions, train_X = modelTrainning(Nin, Nh_l, Nout, d_out, split_ratio, batch_size)
    
