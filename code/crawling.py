import requests
from bs4 import BeautifulSoup
import elasticsearch
import re


es_url = '192.168.0.14:9222'

def newsCrawler(): 
    
    # url = 'https://www.google.com'
    url = 'https://sports.news.naver.com/kbaseball/index.nhn'
    naver_url = 'https://sports.news.naver.com'
    naver_mobile_url = 'https://m.sports.naver.com/news.nhn'
    html = requests.get(url)
    # print(html.text)
    soup = BeautifulSoup(html.text, 'html.parser')

    # print(soup)
    # select = soup.head.find_all('meta')    
    # select = soup.head.find_all('home_news_list') 
    news_list = soup.select("#content > div > div.home_grid > div.content > div.home_article > div.home_news > ul:nth-child(2) > li > a")
    # print(news_list) 

    results_news = []

    for news in news_list:
        # print(news.get('href'))
        news_link = news.get('href')
        news_url, news_parameter = news_link.split("?")

        news_a = naver_mobile_url + '?' + news_parameter
        # print(news_a)
        # print(news.get('title'))  
        baseball_news = {
            'title': news.get('title'),
            'url': news_a
        }
        results_news.append(baseball_news)
    
    # for meta in select:
    #     print(meta.get('content'))

    # for link in soup.find_all('a'):
    #     print(link.text.strip(), link.get('href'))

    print(results_news)

    return results_news

def teamRankCrawler(play_date): 

    es_client = elasticsearch.Elasticsearch(es_url)

    # response 의 type 은 dictionary
    if es_client.indices.exists(index="teamrank"):
        response = es_client.search(index='teamrank', doc_type='teamrank', size=800, body={
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
        })        

        print(response['hits']['hits'])

        if response['hits']['hits']:            
            return response['hits']['hits'][0]['_source']['팀순위']
    
    url = 'https://www.koreabaseball.com/TeamRank/TeamRank.aspx'
    html = requests.get(url)    
    soup = BeautifulSoup(html.text, 'html.parser')

    # results = []

    date = soup.select_one("#cphContents_cphContents_cphContents_lblSearchDateTitle").text[:-3].replace('.', '-')

    # print(date_info[0].text)
    teamrank_date = {}


    teams_rank = soup.select("#cphContents_cphContents_cphContents_udpRecord > table > tbody > tr")
    # print(teams_rank)    

    results_rank = []

    for team in teams_rank:        
        team_info = team.select('td')
        # print(team_info[0].text)
        baseball_team = {
            '순위': team_info[0].text,
            '팀명': team_info[1].text,
            '경기': team_info[2].text,
            '승': team_info[3].text,
            '패': team_info[4].text,
            '무': team_info[5].text,
            '승률': team_info[6].text,
            '게임차': team_info[7].text,
            '최근10경기': team_info[8].text,
            '연속': team_info[9].text,
            '홈': team_info[10].text,
            '방문': team_info[11].text
        }
        results_rank.append(baseball_team)

    team_rank = {
        '팀순위': results_rank
    }

    results = {
        '날짜': date,
        '팀순위': results_rank
    }
    response = es_client.index(index='teamrank', doc_type='teamrank', id=date, body=results)

    return results    

# crawler()
# teamRankCrawler()