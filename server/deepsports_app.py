from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields
from elasticsearch import Elasticsearch
from crawling.crawling import newsCrawler, teamRankCrawler

from datetime import datetime, timedelta
from numFormat import *

app = Flask(__name__) # Flask App 생성
api = Api(app, version='1.0', title='DeepSports API', description='KBO 야구 승부 예측 조회 API입니다')
ns = api.namespace('sports', description='KBO 야구 승부 예측 조회')

# es_url = '192.168.0.14:9222'
es_url = 'localhost:9222'
es = Elasticsearch(es_url)

model = api.model('Model', {
    'date': fields.String(readOnly=True, required=True, description='경기날짜', help='경기날짜는 필수'),
    'play_results': fields.String(required=True, description='경기결과', help='경기결과는 필수')
})

# 경기 예측 결과 조회
@ns.route('/kbo/predict/<string:play_date>')
@ns.response(404, '경기결과를 찾을 수가 없습니다')
@ns.param('play_date', '조회할 경기날짜를 입력해주세요')
class KBO(Resource):
    def get(self, play_date):

        body = {
            "query": {
                "match": {
                    "날짜": play_date
                }
            }
        }

        play_results = []
        if es.indices.exists(index="predict"):                        
            res_lineup = es.search(index="predict", body=body)       
            for result in res_lineup['hits']['hits']:
                print(result['_source'])
                play_results.append(result['_source'])

        return jsonify(play_results)

# 지난 경기 결과 조회
@ns.route('/kbo/score/<string:play_date>')
@ns.response(404, '경기결과를 찾을 수가 없습니다')
@ns.param('play_date', '조회할 경기날짜를 입력해주세요')
class KBOSCORE(Resource):
    # @api.marshal_with(model)
    def get(self, play_date):

        if play_date is None:
            return ''
        
        play_results = self.getScore(play_date)     # 해당 날짜에 경기 결과 
        print(play_results)

        while play_results == []:
            print(play_date)
            
            play_results = self.getScore(play_date)
            print(play_results)           

        return jsonify(play_results)

    # 해당 날짜에 경기 결과 리턴
    def getScore(self, play_date):   

#         t = ['월', '화', '수', '목', '금', '토', '일']            
        body = {
            'query': {
                'bool': {
                    'filter': {
                        'bool':{
                            'must':[
                                {'match': {'날짜': play_date}}
                            ]
                        }
                    }
                }
            }
        }

        play_results = []
        predict_results = self.getPredict(play_date)
        
        res = es.search(index='day_result', size=800, body=body)
        if res['hits']['total']['value'] > 0:           
            for result in res['hits']['hits']:
                # 팀 승리 확률 리턴
                game_predict = self.getTeamPredict(result['_source']['홈 팀'], result['_source']['원정 팀'], predict_results)
                print(game_predict)

                team_result = {
                    '날짜': result['_source']['날짜'],
                    '홈 팀': replace_num_teamName(result['_source']['홈 팀']),
                    '원정 팀': replace_num_teamName(result['_source']['원정 팀']),
                    '장소': replace_park_name(result['_source']['장소']),
                    '경기 분류': str(result['_source']['경기 분류'])+'차전',
                    '홈 팀 점수': result['_source']['홈 팀 점수'],
                    '원정 팀 점수': result['_source']['원정 팀 점수'],
                    '경기 예측': game_predict,
                    '경기 결과': replace_num_teamName(result['_source']['경기 결과'])+ ' 승'
                    
                }
                play_results.append(team_result)

#             print(play_results)
            
        else:
            play_results.append("경기 결과가 없습니다.")
#             print(play_results)
            

        return play_results
    
    # 해당 날짜에 경기 예측 값 리턴
    def getPredict(self, play_date):

        body = {
            "query": {
                "match": {
                    "날짜": play_date
                }
            }
        }

        play_results = []
        if es.indices.exists(index="predict"):        
            res_lineup = es.search(index="predict", body=body)        
            for result in res_lineup['hits']['hits']:
                print(result['_source'])
                play_results.append(result['_source'])

        return play_results
    
    # 팀 승리 확률 리턴
    def getTeamPredict(self, home_team, away_team, predict_result):
#         home_team_predict = ''
#         away_team_predict = ''
        result_predict = ''

        for result in predict_result:
            if result['홈 팀'] == replace_num_teamName(home_team) and result['원정 팀'] == replace_num_teamName(away_team):
                result_predict = result['예측 승리팀']
#                 home_team_predict = result['홈 팀 승리 확률']
#                 away_team_predict = result['원정 팀 승리 확률']

        return result_predict

# 경기 결과 조회 (팀 순위)
@ns.route('/kbo/team/<string:play_date>')
@ns.response(404, '팀순위결과를 찾을 수가 없습니다')
@ns.param('play_date', '조회할 경기날짜를 입력해주세요')
class KBOTEAM(Resource):
    def get(self, play_date):
        team_rank = teamRankCrawler(play_date)        
        return jsonify(team_rank)

# 야구 뉴스 조회
@ns.route('/kbo/news/')
@ns.response(404, '뉴스를 찾을 수가 없습니다')
class KBONEWS(Resource):
    def get(self):
        news_results = newsCrawler()        
        return jsonify(news_results)

@ns.route('/data')
class KBODATA(Resource):
    def index():
        results = es.get(index='contents', doc_type='title', id='my-new-slug')
        return jsonify(results['_source'])

if __name__ == '__main__':
    app.run(host='192.168.0.14', port=5111, debug=True)

    