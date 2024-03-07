from KBO_Day_Crawling import runCrawler
from KBO_DNN_Pre_Predict import dnn_pre_predict
from KBO_DNN_Predict import dnn_predict
from KBO_Prepare_Retraining import *
from KBO_Retraining import *

import datetime
from apscheduler.schedulers.blocking import BlockingScheduler



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
# 경기 종료 후 데이터 추가하여 재학습을 위한 준비
def exec_pre_train():
    today = datetime.date.today()
    pre_train(today)
    
# 기존 데이터에 경기 종료된 데이터를 추가하여 재학습
def exec_train():
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
    print(data.shape)
    # print(data.head(10))

    train_x, test_x, train_y, test_y = train_test_split(data_x, data_y, test_size=test_size, shuffle=True, 
                                                        stratify=data_y, random_state=34)
    # 파일 읽어오기
    pre = False
    if pre:
        data = pd.read_csv('./kbo_data_prepared_maxabs.csv') # 전처리 & 정규화 된 데이터

        data_x, data_y = data, data.pop(target_y)
        data_y = pd.DataFrame(data_y, columns=[target_y])
        print(data.shape)

        train_x, test_x, train_y, test_y = train_test_split(data_x, data_y, test_size=test_size, shuffle=True, 
                                                            stratify=data_y, random_state=34)
    elif not pre:
        train_x = pd.read_csv('./kbo_data_prepared_train_maxabs.csv') # 전처리 & 정규화 된 데이터 80% (훈련 데이터)
        train_x, train_y = train_x, train_x.pop(target_y)
        train_y = pd.DataFrame(train_y, columns=[target_y])
        print(train_x.shape)

        test_x = pd.read_csv('./kbo_data_prepared_test_maxabs.csv') # 전처리 & 정규화 된 데이터 20% (테스트 데이터)
        test_x, test_y = test_x, test_x.pop(target_y)
        test_y = pd.DataFrame(test_y, columns=[target_y])

    N, D = train_x.shape
    print("N = %d, D = %d"%(N, D))

    # 레이어
    Nin = D # train_x.shape[1]
    Nh_l = [Nin, 400, 200, 100, 50, 25] #11까지 줄이면 X
    number_of_class = 11
    Nout = number_of_class
    d_out = 0.2

    cat_w = ['무', 'nc', 'ht', 'ob', 'sk', 'hh', 'ss', 'kt', 'lg', 'wo', 'lt']

    # type exchange : DataFrame -> numpy.ndarray
    train_y = utils.to_categorical(train_y)
    test_y = utils.to_categorical(test_y)

    train_y = pd.DataFrame(train_y, columns=[cat_w])
    test_y = pd.DataFrame(test_y, columns=[cat_w])

    nStep = 400
    split_ratio=0.2; batch_size=20

    for i in range(20):
        train_model, history, predictions, train_X = \
            modelTrainning(Nin, Nh_l, Nout, d_out, split_ratio, batch_size, train_x, train_y, test_x, test_y)

# 다음 경기 예측 분석
def exec_predict():
    print('exec_predict')

    today = datetime.date.today()

    try:
        exists_result, exists_preview = runCrawler(today)       # KBO 데이터 크롤링
    except:
        exists_result, exists_preview = runCrawler(today)
    print(exists_preview)

    if exists_preview == '프리뷰 완료':
        dnn_pre_predict(today)      # DNN 
        dnn_predict(today)          # DNN 학습/테스트
    else:
        print("오늘은 프리뷰가 없습니다!")

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(exec_pre_train, 'cron', hour='1', minute='30', id='pre_train')
    scheduler.add_job(exec_train, 'cron', hour='1', minute='40', id='train')
    scheduler.add_job(exec_predict, 'cron', hour='3', minute='10', id='predict')
    # 오류를 대비하여 같은 작업 추가
    scheduler.add_job(exec_predict, 'cron', hour='3', minute='20', id='predict2')

    
    scheduler.start()


