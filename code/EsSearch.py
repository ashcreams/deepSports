import elasticsearch
import traceback
import pandas as pd
import numpy as np
import elasticsearch
import timeit
import datetime
from datetime import date
import csv

# 선수 개인 정보 추출
# player_name: 선수이름, team_name: 팀명, col: 검색할 항목
def es_searchPlayer(player_name, team_name, col):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')
    
    if es_client.indices.exists(index="player_basic"):
        response = es_client.search(index='player_basic', size=800, body={
            'query': {
                'bool': {
                    'filter': {
                        'bool':{
                            'must':[
                                {
                                 'match': {
                                     '이름': player_name
                                 }
                                },
                                {
                                 'match': {
                                     '팀명': team_name
                                 }
                                }
                            ]
                        }
                    }
                }
            }
        })
        
        # 동명이인이 있는 경우 연봉이 가장 높은 사람을 1명 출력하게 함
        if len(response['hits']['hits']) > 1:
            tmp_res = 0
            for i in range(0,len(response['hits']['hits'])):
                res = int(response['hits']['hits'][i]['_source']['연봉'])

                if tmp_res ==0:
                    pass

                elif res > tmp_res:
                    res = response['hits']['hits'][i]['_source'][col]

                else:
                    pass

                tmp_res = res
        
        # 1명일 경우
        if len(response['hits']['hits']) == 1:
            res = response['hits']['hits'][0]['_source'][col]
            
        else:
            res = '0'
            pass

    return res


# 지정한 연도의 게임 결과 데이터 추출
# title: 게임 명칭, game_round: 게임 분류, year: 연도
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



# 지정한 연도의 게임 결과 데이터 추출
# 위의 es_searchResult 함수와 동일하지만 리턴 값이 다름
# es의 단일 doc 내용(_id값 등등)을 전부 출력
def es_searchResultCount(title, game_round, year):
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
    
    return response['hits']['hits']

# 데이터 셋을 위한 Dataframe 생성
# 공통 데이터
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

# 원정 선발 투수
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

# 원정 교체 투수
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

# 원정 선발 타자
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

# 원정 교체 타자
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

# 홈 선발 투수
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

# 홈 교체 투수
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

# 홈 선발 타자
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

# 홈 교체 타자
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
#         del hbh['이름'], hbh['포지션']
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

# 올해 데이터로 dataframe 생성
# 공통,홈/원정 투수,타자 데이터를 총 병합
def resultDf_this(title, game_round):
    lastOrThis = 'this'
    year = '2021'
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
    # 결과 항목은 맨 뒤에 넣기 위해 지웠다가 재 삽입
    del df_this['this_game_result']
    df_this.insert(358, 'this_game_result', df1['this_game_result'])
    # 추후 분석시 라벨표시를 위해 y로 컬럼명 변경
    df_this.rename(columns = {'this_game_result' : 'y'}, inplace = True)
    # 결측치 0으로 변경
    df_this.fillna(0, inplace=True)
    
    return df_this

# 작년 데이터로 dataframe 생성
# 공통,홈/원정 투수,타자 데이터를 총 병합
def resultDf_last(title, game_round):
    lastOrThis = 'last'
    year = '2020'
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
    # 결과 항목은 맨 뒤에 넣기 위해 지웠다가 재 삽입
    del df_last['last_game_result']
    df_last.insert(358, 'last_game_result', df1['last_game_result'])
    # 결측치 0으로 변경
    df_last.fillna(0, inplace=True)
    
    return df_last


# 지정 날짜에 대한 모든 경기 검색
def es_searchDayTotalResult(date):
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
                                     '날짜': date
                                 }
                                }
                            ]
                        }
                    }
                }
            }
        })
    return response['hits']['hits']

# 해당 날짜 경기 일정 검색
def es_searchDayTotalPreivew(today):
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')
    
    if es_client.indices.exists(index="preview"):
        response = es_client.search(index='preview', size=800, body={
            'query': {
                'bool': {
                    'filter': {
                        'bool':{
                            'must':[
                                {
                                 'match': {
                                     '날짜': today
                                 }
                                }
                            ]
                        }
                    }
                }
            }
        })
    return response['hits']['hits']