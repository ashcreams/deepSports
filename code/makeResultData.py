import elasticsearch
import traceback
import pandas as pd
import numpy as np
import timeit
import datetime
from datetime import date
import csv

# 여기서부터 resultDf_last 함수까지 esSearch.py 함수와 거의 동일.
def es_searchResult(title, game_round, year):
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
                                     '명칭': title
                                 }
                                },
                                {
                                 'match': {
                                     '경기 분류': game_round
                                 }
                                }
                            ],
                            "filter": {
                            "range": {
                              "날짜": {
                                "gte": year+'-01-01',
                                "lte": year+'-12-31'
                        }
                    }
                }
            }
        }}}})
    
    return response['hits']['hits'][0]['_source']


def make_game_df(title, game_round, lastOrThis, year):
    game_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)
    del source['원정 선발 투수'], source['원정 교체 투수'], source['원정 선발 타자'], source['원정 교체 타자']
    del source['홈 선발 투수'], source['홈 교체 투수'], source['홈 선발 타자'], source['홈 교체 타자']
    del source['시간'], source['날짜'], source['명칭']
    
    col = ['경기 분류', '더블헤더', '장소', '관중', '원정 팀', '원정체크', '원정 승', '원정 패', '원정 무', '원정 팀 점수',
      '원정 안타', '원정 실책','원정 사구', '홈 팀', '홈 체크', '홈 승', '홈 패', '홈 무', '홈 팀 점수', '홈 안타', '홈 실책',
      '홈 사구', '경기 결과']
    
    rename_dict = {'경기 분류' : lastOrThis+'_game_round', '더블헤더' : lastOrThis+'_game_dh', 
                   '장소' : lastOrThis+'_game_park', '관중' : lastOrThis+'_game_crowd',
                   '원정 팀' : lastOrThis+'_away_team', '원정체크' : lastOrThis+'_away_check', 
                   '원정 승' : lastOrThis+'_away_win', '원정 패' : lastOrThis+'_away_lose',
                   '원정 무' : lastOrThis+'_away_draw', '원정 팀 점수' : lastOrThis+'_away_score',
                   '원정 안타' : lastOrThis+'_away_h', '원정 실책' : lastOrThis+'_away_e',
                   '원정 사구' : lastOrThis+'_away_bb', 
                   '홈 팀' : lastOrThis+'_home_team', '홈 체크' : lastOrThis+'_home_check', 
                   '홈 승' : lastOrThis+'_home_win', '홈 패' : lastOrThis+'_home_lose',
                   '홈 무' : lastOrThis+'_home_draw', '홈 팀 점수' : lastOrThis+'_home_score',
                   '홈 안타' : lastOrThis+'_home_h', '홈 실책' : lastOrThis+'_home_e',
                   '홈 사구' : lastOrThis+'_home_bb', '경기 결과' : lastOrThis+'_game_result'
                  }

    game = pd.DataFrame(source, index=[0], columns=col)
    game.rename(columns = rename_dict, inplace = True)
    game_df = pd.concat([game_df,game],axis=1)
    
    return game_df


def make_asp_df(title, game_round, lastOrThis, year):
    asp_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['원정 선발 투수']
    col = ['선수id', '등판', '결과', '승', '패', '세', '이닝', '타자', '투구수', 
           '타수', '피안타', '홈런', '4사구', '삼진', '실점', '자책', '평균자책점']
    rename_dict = {'선수id' : lastOrThis+'_asp_name', '등판' : lastOrThis+'_asp_sr', 
                   '결과' : lastOrThis+'_asp_result', '승' : lastOrThis+'_asp_win',
                   '패' : lastOrThis+'_asp_lose', '세' : lastOrThis+'_asp_save', 
                   '이닝' : lastOrThis+'_asp_ip', '타자' : lastOrThis+'_asp_tbf',
                   '투구수' : lastOrThis+'_asp_np', '타수' : lastOrThis+'_asp_ab',
                   '피안타' : lastOrThis+'_asp_h', '홈런' : lastOrThis+'_asp_hr',
                   '4사구' : lastOrThis+'_asp_b', '삼진' : lastOrThis+'_asp_so',
                   '실점' : lastOrThis+'_asp_r', '자책' : lastOrThis+'_asp_er',
                   '평균자책점' : lastOrThis+'_asp_era'}

    asp = pd.DataFrame(source[0], index=[0], columns= col)
    del asp['등판']
    asp.rename(columns = rename_dict, inplace = True)
    asp_df = pd.concat([asp_df,asp],axis=1)
    
    return asp_df


def make_abp_df(title, game_round, lastOrThis, year):
    abp_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['원정 교체 투수']
    num = 0
    col = ['선수id', '등판', '결과', '승', '패', '세', '이닝', '타자', '투구수', 
           '타수', '피안타', '홈런', '4사구', '삼진', '실점', '자책', '평균자책점']
    
    for num in range(0, len(source)):
        abp = pd.DataFrame(source[num], index=[0], columns= col)
        rename_dict = {'선수id' : lastOrThis+'_abp{0}_name'.format(num+1), '등판' : lastOrThis+'_abp{0}_sr'.format(num+1), 
                       '결과' : lastOrThis+'_abp{0}_result'.format(num+1), '승' : lastOrThis+'_abp{0}_win'.format(num+1),
                       '패' : lastOrThis+'_abp{0}_lose'.format(num+1), '세' : lastOrThis+'_abp{0}_save'.format(num+1), 
                       '이닝' : lastOrThis+'_abp{0}_ip'.format(num+1), '타자' : lastOrThis+'_abp{0}_tbf'.format(num+1),
                       '투구수' : lastOrThis+'_abp{0}_np'.format(num+1), '타수' : lastOrThis+'_abp{0}_ab'.format(num+1),
                       '피안타' : lastOrThis+'_abp{0}_h'.format(num+1), '홈런' : lastOrThis+'_abp{0}_hr'.format(num+1),
                       '4사구' : lastOrThis+'_abp{0}_b'.format(num+1), '삼진' : lastOrThis+'_abp{0}_so'.format(num+1),
                       '실점' : lastOrThis+'_abp{0}_r'.format(num+1), '자책' : lastOrThis+'_abp{0}_er'.format(num+1),
                       '평균자책점' : lastOrThis+'_abp{0}_era'.format(num+1)}

        abp.rename(columns = rename_dict, inplace = True)
        abp_df = pd.concat([abp_df,abp],axis=1)
        
        if num > 2:
            break
        else:
            pass

    if len(source) < 4:
        for num in range(len(source), 4):
            col_name = [lastOrThis+'_abp{0}_name'.format(num+1), lastOrThis+'_abp{0}_sr'.format(num+1), 
                           lastOrThis+'_abp{0}_result'.format(num+1), lastOrThis+'_abp{0}_win'.format(num+1),
                           lastOrThis+'_abp{0}_lose'.format(num+1), lastOrThis+'_abp{0}_save'.format(num+1), 
                           lastOrThis+'_abp{0}_ip'.format(num+1), lastOrThis+'_abp{0}_tbf'.format(num+1),
                           lastOrThis+'_abp{0}_np'.format(num+1), lastOrThis+'_abp{0}_ab'.format(num+1),
                           lastOrThis+'_abp{0}_h'.format(num+1), lastOrThis+'_abp{0}_hr'.format(num+1),
                           lastOrThis+'_abp{0}_b'.format(num+1), lastOrThis+'_abp{0}_so'.format(num+1),
                           lastOrThis+'_abp{0}_r'.format(num+1), lastOrThis+'_abp{0}_er'.format(num+1),
                           lastOrThis+'_abp{0}_era'.format(num+1)]
            abp2 = pd.DataFrame(columns=col_name)
            abp2.fillna(0,inplace=True)
            abp_df = pd.concat([abp_df,abp2],axis=1)

    else:
        pass

        
    return abp_df


def make_ash_df(title, game_round, lastOrThis, year):
    ash_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['원정 선발 타자']
    col = ['선수id', '타순', '타수', '안타', '타점', '득점', '타율']

    for num in range(0, len(source)):
        rename_dict = {'선수id' : lastOrThis+'_ash{0}_name'.format(num+1), '타순' : lastOrThis+'_ash{0}_order'.format(num+1),
                       '타수' : lastOrThis+'_ash{0}_ab'.format(num+1), '안타' : lastOrThis+'_ash{0}_h'.format(num+1),
                       '타점' : lastOrThis+'_ash{0}_rbi'.format(num+1), '득점' : lastOrThis+'_ash{0}_rh'.format(num+1),
                       '타율' : lastOrThis+'_ash{0}_avg'.format(num+1)}
            
        ash = pd.DataFrame(source[num], index=[0], columns=col)

        ash.rename(columns = rename_dict, inplace = True)
        ash_df = pd.concat([ash_df,ash],axis=1)
        
    return ash_df


def make_abh_df(title, game_round, lastOrThis, year):
    abh_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['원정 교체 타자']
    col = ['선수id', '타순', '타수', '안타', '타점', '득점', '타율']
    
    for num in range(0, len(source)):
        rename_dict = {'선수id' : lastOrThis+'_abh{0}_name'.format(num+1), '타순' : lastOrThis+'_abh{0}_order'.format(num+1),
                       '타수' : lastOrThis+'_abh{0}_ab'.format(num+1), '안타' : lastOrThis+'_abh{0}_h'.format(num+1),
                       '타점' : lastOrThis+'_abh{0}_rbi'.format(num+1), '득점' : lastOrThis+'_abh{0}_rh'.format(num+1),
                       '타율' : lastOrThis+'_abh{0}_avg'.format(num+1)}
            
        abh = pd.DataFrame(source[num], index=[0],columns=col)

        abh.rename(columns = rename_dict, inplace = True)
        abh_df = pd.concat([abh_df,abh],axis=1)
        
        if num > 1:
            break
        else:
            pass
        
        
    if len(source) < 3:
        for num in range(len(source), 3):
            col_name = [lastOrThis+'_abh{0}_name'.format(num+1), lastOrThis+'_abh{0}_order'.format(num+1),
                        lastOrThis+'_abh{0}_ab'.format(num+1), lastOrThis+'_abh{0}_h'.format(num+1),
                        lastOrThis+'_abh{0}_rbi'.format(num+1), lastOrThis+'_abh{0}_rh'.format(num+1),
                        lastOrThis+'_abh{0}_avg'.format(num+1)]
            abh2 = pd.DataFrame(columns=col_name)
            abh_df = pd.concat([abh_df,abh2],axis=1)
            abh_df.fillna(0, inplace=True)

    else:
        pass
          
    return abh_df


def make_hsp_df(title, game_round, lastOrThis, year):
    hsp_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['홈 선발 투수']
    col = ['선수id', '등판', '결과', '승', '패', '세', '이닝', '타자', '투구수', 
           '타수', '피안타', '홈런', '4사구', '삼진', '실점', '자책', '평균자책점']
    
    rename_dict = {'선수id' : lastOrThis+'_hsp_name', '등판' : lastOrThis+'_hsp_sr', 
                   '결과' : lastOrThis+'_hsp_result', '승' : lastOrThis+'_hsp_win',
                   '패' : lastOrThis+'_hsp_lose', '세' : lastOrThis+'_hsp_save', 
                   '이닝' : lastOrThis+'_hsp_ip', '타자' : lastOrThis+'_hsp_tbf',
                   '투구수' : lastOrThis+'_hsp_np', '타수' : lastOrThis+'_hsp_ab',
                   '피안타' : lastOrThis+'_hsp_h', '홈런' : lastOrThis+'_hsp_hr',
                   '4사구' : lastOrThis+'_hsp_b', '삼진' : lastOrThis+'_hsp_so',
                   '실점' : lastOrThis+'_hsp_r', '자책' : lastOrThis+'_hsp_er',
                   '평균자책점' : lastOrThis+'_hsp_era'}

    hsp = pd.DataFrame(source[0], index=[0], columns=col)
    del hsp['등판']
    hsp.rename(columns = rename_dict, inplace = True)
    hsp_df = pd.concat([hsp_df,hsp],axis=1)
    
    return hsp_df

def make_hbp_df(title, game_round, lastOrThis, year):
    hbp_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['홈 교체 투수']
    col = ['선수id', '등판', '결과', '승', '패', '세', '이닝', '타자', '투구수', 
           '타수', '피안타', '홈런', '4사구', '삼진', '실점', '자책', '평균자책점']
    
    for num in range(0, len(source)):
        rename_dict = {'선수id' : lastOrThis+'_hbp{0}_name'.format(num+1), '등판' : lastOrThis+'_hbp{0}_sr'.format(num+1), 
                       '결과' : lastOrThis+'_hbp{0}_result'.format(num+1), '승' : lastOrThis+'_hbp{0}_win'.format(num+1),
                       '패' : lastOrThis+'_hbp{0}_lose'.format(num+1), '세' : lastOrThis+'_hbp{0}_save'.format(num+1), 
                       '이닝' : lastOrThis+'_hbp{0}_ip'.format(num+1), '타자' : lastOrThis+'_hbp{0}_tbf'.format(num+1),
                       '투구수' : lastOrThis+'_hbp{0}_np'.format(num+1), '타수' : lastOrThis+'_hbp{0}_ab'.format(num+1),
                       '피안타' : lastOrThis+'_hbp{0}_h'.format(num+1), '홈런' : lastOrThis+'_hbp{0}_hr'.format(num+1),
                       '4사구' : lastOrThis+'_hbp{0}_b'.format(num+1), '삼진' : lastOrThis+'_hbp{0}_so'.format(num+1),
                       '실점' : lastOrThis+'_hbp{0}_r'.format(num+1), '자책' : lastOrThis+'_hbp{0}_er'.format(num+1),
                       '평균자책점' : lastOrThis+'_hbp{0}_era'.format(num+1)}
            
        hbp = pd.DataFrame(source[num], index=[0], columns= col)

        hbp.rename(columns = rename_dict, inplace = True)
        hbp_df = pd.concat([hbp_df,hbp],axis=1)
        hbp_df.fillna(0, inplace=True)
        
        if num > 2:
            break
        else:
            pass
        
    if len(source) < 4:
        for num in range(len(source), 4):
            col_name = [lastOrThis+'_hbp{0}_name'.format(num+1), lastOrThis+'_hbp{0}_sr'.format(num+1), 
                        lastOrThis+'_hbp{0}_result'.format(num+1), lastOrThis+'_hbp{0}_win'.format(num+1),
                        lastOrThis+'_hbp{0}_lose'.format(num+1), lastOrThis+'_hbp{0}_save'.format(num+1), 
                        lastOrThis+'_hbp{0}_ip'.format(num+1), lastOrThis+'_hbp{0}_tbf'.format(num+1),
                        lastOrThis+'_hbp{0}_np'.format(num+1), lastOrThis+'_hbp{0}_ab'.format(num+1),
                        lastOrThis+'_hbp{0}_h'.format(num+1), lastOrThis+'_hbp{0}_hr'.format(num+1),
                        lastOrThis+'_hbp{0}_b'.format(num+1), lastOrThis+'_hbp{0}_so'.format(num+1),
                        lastOrThis+'_hbp{0}_r'.format(num+1), lastOrThis+'_hbp{0}_er'.format(num+1),
                        lastOrThis+'_hbp{0}_era'.format(num+1)]
            hbp2 = pd.DataFrame(columns=col_name)
            hbp2.fillna(0,inplace=True)
            hbp_df = pd.concat([hbp_df,hbp2],axis=1)

    else:
        pass
        
    return hbp_df

def make_hsh_df(title, game_round, lastOrThis, year):
    hsh_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['홈 선발 타자']
    col = ['선수id', '타순', '타수', '안타', '타점', '득점', '타율']
    
    for num in range(0, len(source)):
        rename_dict = {'선수id' : lastOrThis+'_hsh{0}_name'.format(num+1), '타순' : lastOrThis+'_hsh{0}_order'.format(num+1),
                       '타수' : lastOrThis+'_hsh{0}_ab'.format(num+1), '안타' : lastOrThis+'_hsh{0}_h'.format(num+1),
                       '타점' : lastOrThis+'_hsh{0}_rbi'.format(num+1), '득점' : lastOrThis+'_hsh{0}_rh'.format(num+1),
                       '타율' : lastOrThis+'_hsh{0}_avg'.format(num+1)}
            
        hsh = pd.DataFrame(source[num], index=[0], columns=col)

        hsh.rename(columns = rename_dict, inplace = True)
        hsh_df = pd.concat([hsh_df,hsh],axis=1)
        
    return hsh_df


def make_hbh_df(title, game_round, lastOrThis, year):
    hbh_df = pd.DataFrame()
    source = es_searchResult(title, game_round, year)['홈 교체 타자']
    col = ['선수id', '타순', '타수', '안타', '타점', '득점', '타율']

    for num in range(0, len(source)):
        rename_dict = {'선수id' : lastOrThis+'_hbh{0}_name'.format(num+1), '타순' : lastOrThis+'_hbh{0}_order'.format(num+1),
                       '타수' : lastOrThis+'_hbh{0}_ab'.format(num+1), '안타' : lastOrThis+'_hbh{0}_h'.format(num+1),
                       '타점' : lastOrThis+'_hbh{0}_rbi'.format(num+1), '득점' : lastOrThis+'_hbh{0}_rh'.format(num+1),
                       '타율' : lastOrThis+'_hbh{0}_avg'.format(num+1)}
            
        hbh = pd.DataFrame(source[num], index=[0], columns=col)
        hbh.rename(columns = rename_dict, inplace = True)
        hbh_df = pd.concat([hbh_df,hbh],axis=1)
        
        if num > 1:
            break
        else:
            pass
        
    if len(source) < 3:
        for num in range(len(source), 3):
            col_name = [lastOrThis+'_hbh{0}_name'.format(num+1), lastOrThis+'_hbh{0}_order'.format(num+1),
                        lastOrThis+'_hbh{0}_ab'.format(num+1), lastOrThis+'_hbh{0}_h'.format(num+1),
                        lastOrThis+'_hbh{0}_rbi'.format(num+1), lastOrThis+'_hbh{0}_rh'.format(num+1),
                        lastOrThis+'_hbh{0}_avg'.format(num+1)]
            hbh2 = pd.DataFrame(columns=col_name)
            hbh_df = pd.concat([hbh_df,hbh2],axis=1)
            hbh_df.fillna(0, inplace=True)

    else:
        pass
        
    return hbh_df


def resultDf_this(title, game_round, year):
    lastOrThis = 'this'
#     year = '2021'
    df1 = make_game_df(title, game_round, lastOrThis, year)
    df2 = make_asp_df(title, game_round, lastOrThis, year)
    df3 = make_abp_df(title, game_round, lastOrThis, year)
    df4 = make_ash_df(title, game_round, lastOrThis, year)
    df5 = make_abh_df(title, game_round, lastOrThis, year)
    df6 = make_hsp_df(title, game_round, lastOrThis, year)
    df7 = make_hbp_df(title, game_round, lastOrThis, year)
    df8 = make_hsh_df(title, game_round, lastOrThis, year)
    df9 = make_hbh_df(title, game_round, lastOrThis, year)
    df_this = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9],axis=1)
    del df_this['this_game_result']
    df_this.insert(358, 'this_game_result', df1['this_game_result'])
    df_this.rename(columns = {'this_game_result' : 'y'}, inplace = True)
    df_this.fillna(0, inplace=True)
    return df_this

def resultDf_last(title, game_round, year):
    lastOrThis = 'last'
#     year = '2020'
    df1 = make_game_df(title, game_round, lastOrThis, year)
    df2 = make_asp_df(title, game_round, lastOrThis, year)
    df3 = make_abp_df(title, game_round, lastOrThis, year)
    df4 = make_ash_df(title, game_round, lastOrThis, year)
    df5 = make_abh_df(title, game_round, lastOrThis, year)
    df6 = make_hsp_df(title, game_round, lastOrThis, year)
    df7 = make_hbp_df(title, game_round, lastOrThis, year)
    df8 = make_hsh_df(title, game_round, lastOrThis, year)
    df9 = make_hbh_df(title, game_round, lastOrThis, year)
    df_last = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9],axis=1)
    del df_last['last_game_result']
    df_last.insert(358, 'last_game_result', df1['last_game_result'])
    df_last.fillna(0, inplace=True)
    return df_last

# 지정연도와 지정한 경기분류로 데이터 검색
def es_dayResult(game_round, lastYear):
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
                                     '경기 분류': game_round
                                 }
                                }
                            ],
                            "filter": {
                            "range": {
                              "날짜": {
                                "gte": lastYear+'-01-01',
                                "lte": lastYear+'-12-31'
                        }
                    }
                }
            }
        }}}})
    
    return response['hits']['hits']

# 2개 년도 데이터셋 생성
def totalGetResult(lastYear, thisYear):

    daily_data = pd.DataFrame()

    for game_round in range(1, 17):
        print(game_round)
        for game in es_dayResult(game_round, lastYear):
            title = game['_source']['명칭']
            print(title)
            df_last = resultDf_last(title, game_round, lastYear)

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
                try:
                    df_this = resultDf_this(title, int(game_round)-1, thisYear)
                except IndexError:
                    title_a = title[0:2]
                    title_h = title[2:4]
                    title = title_h + title_a
                    title
                    df_this = resultDf_this(title, int(game_round)-1, thisYear)

            df_result = pd.concat([df_last, df_this],axis=1)

            daily_data = daily_data.append(df_result, ignore_index=True, sort=False)
    daily_data.to_csv('data_{}.csv'.format(thisYear))

# 지정연도 모든 데이터 검색    
def es_seasonResult(year):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')

    if es_client.indices.exists(index="day_result"):
        response = es_client.search(index='day_result', size=800, body={
            'query': {
                'bool': {
                    'filter': {
                        'bool':{
                            "filter": {
                            "range": {
                              "날짜": {
                                "gte": year+'-01-01',
                                "lte": year+'-12-31'
                        }
                    }
                }
            }
        }}}})
    
    return response['hits']['hits']

# 올해 진행중인 시즌 데이터셋 생성
# totalGetResult 함수를 기반으로 수정해서 비슷하지만 range 반복을 사용하지 않음.
# totalGetResult는 경기 분류 값으로 range를 사용했지만 현재 진행중인 시즌은 해당 경기 분류가 아직 없을 수도 있기 때문에 에러가 생김
def thisSeasonGetResult(lastYear, thisYear):
    daily_data = pd.DataFrame()

    for game in es_seasonResult(thisYear):
        title = game['_source']['명칭']
        tmp_round = game['_source']['경기 분류']
        print(title, tmp_round)
        
        if tmp_round == 1:
            try:
                df_this = resultDf_this(title, 16, lastYear)
            except IndexError:
                title_a = title[0:2]
                title_h = title[2:4]
                title = title_h + title_a
                title
                df_this = resultDf_this(title, 16, lastYear)

        else:
            df_this = resultDf_this(title, tmp_round, thisYear)
        
        try:
            df_last = resultDf_last(title, tmp_round, lastYear)
        except IndexError:
            title_a = title[0:2]
            title_h = title[2:4]
            title = title_h + title_a
            title
            df_last = resultDf_last(title, tmp_round, lastYear)

        df_result = pd.concat([df_last, df_this],axis=1)

        daily_data = daily_data.append(df_result, ignore_index=True, sort=False)
    daily_data.to_csv('data_{}.csv'.format(thisYear))
