import traceback
import pandas as pd
import elasticsearch
import timeit
import time
import datetime
import numpy as np

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import NoAlertPresentException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import UnexpectedAlertPresentException, ElementNotInteractableException
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By

# 게임 날짜별 참여선수 url
player_line_up_URL = 'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx'
exec_driver_path = '/home/ash/deepsports/chromedriver'

# 저장할 csv 파일 이름
csv_name = '2021_KBO_경기 데이터.csv'

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")
# 혹은 options.add_argument("--disable-gpu")

browser = webdriver.Chrome(executable_path=exec_driver_path, chrome_options=options)
# browser = webdriver.Chrome(executable_path=exec_driver_path)
browser.implicitly_wait(30)

max_inning = 0

count_play = 3


# 지정한 날짜의 프리뷰 Crawling
def dayPreviewCrawling(year, month, day):      

    # elastic search 오브젝트 생성
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')    

    # 기존에 저장된 파일 읽어오기
    is_ex_df = True
    try:
        ex_df = pd.read_csv(csv_name, engine='python')
    except FileNotFoundError:
        is_ex_df = False

    # 경기 기록 긁어오기
    browser.get(player_line_up_URL)
    browser.implicitly_wait(30)
    time.sleep(2)

    baseball_df = pd.DataFrame(
        columns=['날짜', '명칭', '경기 분류', '더블헤더', '장소', '원정 팀', '홈 팀', '홈 체크'])

    # 시작 페이지의 날짜 선택
    datepicker = browser.find_element_by_class_name('ui-datepicker-trigger')
    datepicker.click()
    # 시작 년도
    datepicker_year = browser.find_element_by_class_name('ui-datepicker-year')
    for option in datepicker_year.find_elements_by_tag_name('option'):
        if option.text == year:
            option.click()
            break
    # 시작 월
    datepicker_month = browser.find_element_by_class_name('ui-datepicker-month')
    for option in datepicker_month.find_elements_by_tag_name('option'):
        if option.text == monthFormat(month):
            option.click()
            break
            
    # 시작 일(현재는 검색 시작 하루 후(다음날))
    datepicker_calendar = browser.find_element_by_class_name('ui-datepicker-calendar')
    aes = datepicker_calendar.find_elements_by_tag_name('a')
    for a in aes:
        if a.text == day:
            a.click()
            break
    
    today = browser.find_element_by_class_name('today').text[:-3].replace('.', '-')
    search_day = today
    print('검색일(프리뷰) : ', search_day)

    input_day = year + '-' + month + '-' + dayFormat(day)
    print('입력일(프리뷰) : ', input_day)

    if search_day != input_day:
        print('해당 날짜에 대한 정보가 없습니다!')
        return -1
    
    # print(browser.find_element_by_id('gameComment').text)
    # 우천 등으로 모든 경기가 취소된 경우
    if browser.find_element_by_id('gameComment').text == '당일 경기는 모두 취소되었습니다.':
        print('당일 경기는 모두 취소되었습니다.')
        return

    # 선택된 날짜에 열린 경기들 선택
    game_list = browser.find_element_by_class_name('game-list').find_elements_by_tag_name('li')
    # 하루에 연속 게임을 하는 경우 검사
    is_continuous = False
    b_home_team = -1

    game_results = []    


    preview_count = 0

    for game in game_list:
        try:
            game.click()
        except ElementClickInterceptedException:
            click_time = timeit.default_timer()
            while timeit.default_timer() - click_time > 3:
                break
            game.click()      

            
        tmp_broadcasting = game.find_element_by_class_name('broadcasting').text
        tmp_broadcasting = tmp_broadcasting.split()       
        game_str = game.get_attribute('g_id')
        
        away_team = game.get_attribute('away_id')
        home_team = game.get_attribute('home_id')
        away_check = 1
        home_check = 0
        away_sp = game.get_attribute('away_p_id')
        home_sp = game.get_attribute('home_p_id')
        game_round = tmp_broadcasting[0][:-2]
        game_park = game.get_attribute('s_nm')
        game_title = game_str[8:12]
        game_dh = game_str[12]
 
        print(away_team, home_team)

        # 경기 정보를 dictionary 형태로 변환
        baseball_dict = {
            '날짜': today,
            '명칭': game_title,
            '경기 분류': game_round,
            '더블헤더': game_dh,
            '장소': replace_park_num(game_park),
            '원정 팀': replace_team_num(away_team),
            '홈 팀': replace_team_num(home_team),
        }
        
        # 변환된 dictionary 를 elastic search 에 저장
        response = es_client.index(index='preview', doc_type='_doc', id=game_str, body=baseball_dict)    
    
    return '프리뷰 완료'
     
def dayResultCrawling(year, month, day):      

    # elastic search 오브젝트 생성
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')    

    # 기존에 저장된 파일 읽어오기
    is_ex_df = True
    try:
        ex_df = pd.read_csv(csv_name, engine='python')
    except FileNotFoundError:
        is_ex_df = False

    # 경기 기록 긁어오기
    browser.get(player_line_up_URL)
    browser.implicitly_wait(30)
    time.sleep(2)

    baseball_df = pd.DataFrame(
        columns=['날짜', '명칭', '경기 분류', '더블헤더', '장소', '원정 팀', '원정 체크', '원정 팀 점수', 
                 '홈 팀', '홈 체크', '홈 팀 점수', '경기 결과'])
    
    # 시작 페이지의 날짜 선택
    datepicker = browser.find_element_by_class_name('ui-datepicker-trigger')
    datepicker.click()
    # 시작 년도
    datepicker_year = browser.find_element_by_class_name('ui-datepicker-year')
    for option in datepicker_year.find_elements_by_tag_name('option'):
        if option.text == year:
            option.click()
            break
    # 시작 월
    datepicker_month = browser.find_element_by_class_name('ui-datepicker-month')
    for option in datepicker_month.find_elements_by_tag_name('option'):
        # 1월(JAN) 2월(FEB) 3월(MAR) 4월(APR) 5월(MAY) 6월(JUN) 7월(JUL) 8월(AUG) 9월(SEP) 10월(OCT) 11월(NOV) 12월(DEC)
        if option.text == monthFormat(month):
            option.click()
            break

    datepicker_calendar = browser.find_element_by_class_name('ui-datepicker-calendar')
    aes = datepicker_calendar.find_elements_by_tag_name('a')
    for a in aes:
        if a.text == day:
            a.click()
            break

    today = browser.find_element_by_class_name('today').text[:-3].replace('.', '-')
    search_day = today
    print('검색일(프리뷰) : ', search_day)
    
    input_day = year + '-' + month + '-' + dayFormat(day)
    print('입력일(프리뷰) : ', input_day)
    
    if search_day != input_day:
        print('해당 날짜에 대한 정보가 없습니다!')
        return

    # print(browser.find_element_by_id('gameComment').text)
    # 우천 등으로 모든 경기가 취소된 경우
    if browser.find_element_by_id('gameComment').text == '당일 경기는 모두 취소되었습니다.':
        print('당일 경기는 모두 취소되었습니다.')
        return

    # 선택된 날짜에 열린 경기들 선택
    game_list = browser.find_element_by_class_name('game-list').find_elements_by_tag_name('li')
    # 하루에 연속 게임을 하는 경우 검사
    is_continuous = False
    b_home_team = -1

    game_results = []    

    home_pitcher_list = []
    away_pitcher_list = []
    home_hitter_list = []
    away_hitter_list = []

    home_pitcher_df = []
    away_pitcher_df = []
    home_hitter_df = []
    away_hitter_df = []

    for game in game_list:
        try:
            game.click()
        except ElementClickInterceptedException:
            click_time = timeit.default_timer()
            while timeit.default_timer() - click_time > 3:
                break
            game.click()      
              
        is_preview = -1  
  
        # 게임의 상태 : 경기종료, 취소, "", : 등
        game_state = game.find_element_by_class_name('time').text
        if '취소' in game_state:
            continue
        elif '' == game_state:
            continue
        elif ':' == game_state:
            continue
        # 현재 게임 진행중인 경우 사용
        elif '회' in game_state:
            continue
        
        tmp_broadcasting = game.find_element_by_class_name('broadcasting').text
        tmp_broadcasting = tmp_broadcasting.split()       
        game_str = game.get_attribute('g_id')
        
        away_team = game.get_attribute('away_id')
        home_team = game.get_attribute('home_id')
        away_check = 1
        home_check = 0
        away_sp = game.get_attribute('away_p_id')
        home_sp = game.get_attribute('home_p_id')
        game_round = tmp_broadcasting[0][:-2]
        game_park = game.get_attribute('s_nm')
        game_title = game_str[8:12]
        game_dh = game_str[12]
        game_crowd = None
        game_time = None
        away_win = None
        away_lose = None
        away_draw = None
        away_hit = None
        away_error = None
        away_bb = None
        home_win = None
        home_lose = None
        home_draw = None
        home_hit = None
        home_error = None
        home_bb = None

        print(away_team, home_team)

        # home team 이 이전 홈 팀과 같다면 하루에 연속으로 게임하는 경우에 속함
        if home_team == b_home_team:
            is_continuous = True
        else:
            is_continuous = False
            b_home_team = home_team
        
        if game_state == '경기종료':
            # 경기 승/패 결과
            score_list = game.find_elements_by_class_name('score')
            a_score, h_score = int(score_list[0].text), int(score_list[1].text)

            # 경기 별 홈, 원정 팀 점수
            if a_score == h_score:
                game_result = 0
            elif a_score > h_score:
                game_result = away_team
            elif a_score < h_score:
                game_result = home_team
            
        else:
            game_result = 0
            a_score, h_score = 0, 0

        result_json = {
            '날짜': today,
            '명칭': game_title,
            '경기 분류': game_round,
            '더블헤더': game_dh,
            '장소': game_park,
            '원정 팀': away_team,
            '원정 체크': away_check,
            '원정 팀 점수': a_score,
            '홈 팀': home_team,
            '홈 체크' : home_check,
            '홈 팀 점수': h_score,
            '경기 결과': game_result   
        }

        baseball_df = baseball_df.append(result_json, ignore_index=True)
        game_results.append(result_json)

        # 프리뷰가 있다면 열리지 않은 경기라는 뜻
        if browser.find_element_by_class_name('tab-tit').text == '프리뷰':
            is_preview = 1
            baseball_df, home_pitcher_list, away_pitcher_list, home_hitter_list, away_hitter_list = \
            previewData(baseball_df, today, home_team, away_team, home_pitcher_list, 
                        away_pitcher_list, home_hitter_list, away_hitter_list)

        # 프리뷰가 없다면 진행된 경기라는 뜻
        else:
            baseball_df, game_crowd, game_time, away_win, away_lose, away_draw, away_hit, away_error, away_bb,\
            home_win, home_lose, home_draw, home_hit, home_error, home_bb,\
            away_pitcher_list, away_pitcher_df, away_hitter_list, away_hitter_df,\
            home_pitcher_list, home_pitcher_df, home_hitter_list, home_hitter_df = \
            reviewData(baseball_df, today, home_team, away_team, home_pitcher_list,
                       away_pitcher_list, home_hitter_list, away_hitter_list)

        # 경기 정보를 dictionary 형태로 변환
        baseball_dict = {
            '날짜': today,
            '명칭': game_title,
            '경기 분류': int(game_round),
            '경기 결과': int(replace_team_num(game_result)),
            '원정 팀 점수': int(a_score),
            '홈 팀 점수': int(h_score),
            '시간': game_time,
            '더블헤더': int(game_dh),
            '관중': int(game_crowd),
            '장소': int(replace_park_num(game_park)),
            '원정체크' : int(away_check),
            '원정 팀': int(replace_team_num(away_team)),
            '원정 승' : int(away_win),
            '원정 패' : int(away_lose),
            '원정 무' : int(away_draw),
            '원정 안타' : int(away_hit),
            '원정 실책' : int(away_error),
            '원정 사구' : int(away_bb),
            '원정 선발 투수': [],
            '원정 선발 타자': [],
            '원정 교체 투수': [],
            '원정 교체 타자': [],
            '홈 팀': int(replace_team_num(home_team)),
            '홈 체크' : int(home_check),
            '홈 승' : int(home_win),
            '홈 패' : int(home_lose),
            '홈 무' : int(home_draw),
            '홈 안타' : int(home_hit),
            '홈 실책' : int(home_error),
            '홈 사구' : int(home_bb),
            '홈 선발 투수': [],
            '홈 선발 타자': [],
            '홈 교체 투수': [],
            '홈 교체 타자': []
        }
    
        if is_preview == -1:
            baseball_dict = makeTeamDict(baseball_dict, baseball_df,
                    home_pitcher_list, home_pitcher_df,
                    home_hitter_list, home_hitter_df,
                    away_pitcher_list, away_pitcher_df,
                    away_hitter_list, away_hitter_df,
                    today, home_team, away_team)   

            # 변환된 dictionary 를 elastic search 에 저장
            response = es_client.index(index='day_result', doc_type='_doc', id=game_str, body=baseball_dict)         
        else:
            baseball_dict = makePreTeamDict(baseball_dict, baseball_df, 
                    home_pitcher_list, home_pitcher_df, 
                    home_hitter_list, home_hitter_df, 
                    away_pitcher_list, away_pitcher_df, 
                    away_hitter_list, away_hitter_df,
                    today, home_team, away_team)             

    return game_results  


def beforeResultUrl(year, month):
    browser.get('https://www.koreabaseball.com/Schedule/Schedule.aspx?seriesId=0,9')
    
    url_list = []
    
    year_select = browser.find_element_by_xpath\
        ("//select[@id='ddlYear']/option[text()='{0}']".format(year))          
    try:
        year_select.click()
    except TimeoutException:
        time.sleep(10)
        print('타임슬립')
        browser.refresh()
    except StaleElementReferenceException:
        time.sleep(10)
        print('타임슬립')
        browser.refresh()
    finally:
        element = WebDriverWait(browser, 10).until\
                (EC.presence_of_element_located((By.CLASS_NAME, "tbl")))
    
        element = WebDriverWait(browser, 10).until\
                (EC.presence_of_element_located((By.TAG_NAME, "a")))

    
    month_select = browser.find_element_by_xpath\
        ("//*[@id='ddlMonth']/option[text()='{0}']".format(month))
    month_select.click()
    result_btn = browser.find_elements_by_id('btnReview')
    for result in result_btn:
        if result.text == '리뷰':
            url = result.get_attribute('href')
            url_list.append(url)
        else:
            pass
    
    return url_list


def beforeResultCrawling(url_list):      

    # elastic search 오브젝트 생성
    es_client = elasticsearch.Elasticsearch('192.168.0.14:9222')    

    for url in url_list:
    # 경기 기록 긁어오기
        browser.get(url)
        try:
            element = WebDriverWait(browser, 10).until\
                (EC.presence_of_element_located((By.CLASS_NAME, "tbl")))
        except:
            browser.refresh()
            time.sleep(10)
        

        baseball_df = pd.DataFrame(
            columns=['날짜', '명칭', '경기 분류', '더블헤더', '장소', '원정 팀', '원정 체크', '원정 팀 점수', 
                     '홈 팀', '홈 체크', '홈 팀 점수', '경기 결과'])

        game_results = []    

        home_pitcher_list = []
        away_pitcher_list = []
        home_hitter_list = []
        away_hitter_list = []

        home_pitcher_df = []
        away_pitcher_df = []
        home_hitter_df = []
        away_hitter_df = []

        today = browser.find_element_by_class_name('today').text[:-3].replace('.', '-')
        
        try:
            game = browser.find_element_by_css_selector('#contents > div.today-game > div > div.bx-viewport > ul > li.list-review.on')
            game_str = game.get_attribute('g_id')
        except:
            browser.refresh()
            time.sleep(10)
            game = browser.find_element_by_css_selector('#contents > div.today-game > div > div.bx-viewport > ul > li.list-review.on')
            game_str = game.get_attribute('g_id')
        
        

        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        tmp_broadcasting = soup.find("li", {"g_id":game_str}) 
        tmp_broadcasting = tmp_broadcasting.find("div", {"class": "broadcasting"}).text
        tmp_broadcasting = tmp_broadcasting.split() 
        game_round = tmp_broadcasting[0][:-2]

        print(game_round, today)

        away_team = game.get_attribute('away_id')
        home_team = game.get_attribute('home_id')
        away_check = 1
        home_check = 0
        away_sp = game.get_attribute('away_p_id')
        home_sp = game.get_attribute('home_p_id')
        game_round = tmp_broadcasting[0][:-2]
        game_park = game.get_attribute('s_nm')
        print(game_park)
        game_title = game_str[8:12]
        game_dh = game_str[12]
        game_crowd = None
        game_time = None
        away_win = None
        away_lose = None
        away_draw = None
        away_hit = None
        away_error = None
        away_bb = None
        home_win = None
        home_lose = None
        home_draw = None
        home_hit = None
        home_error = None
        home_bb = None

        print(away_team, home_team)

        a_score = int(browser.find_element_by_class_name('run_T').text)
        h_score = int(browser.find_element_by_class_name('run_B').text)

        # 경기 별 홈, 원정 팀 점수
        if a_score == h_score:
            game_result = 0
        elif a_score > h_score:
            game_result = away_team
        elif a_score < h_score:
            game_result = home_team

        else:
            game_result = 0
            a_score, h_score = 0, 0

        result_json = {
            '날짜': today,
            '명칭': game_title,
            '경기 분류': game_round,
            '더블헤더': game_dh,
            '장소': game_park,
            '원정 팀': away_team,
            '원정 체크': away_check,
            '원정 팀 점수': a_score,
            '홈 팀': home_team,
            '홈 체크' : home_check,
            '홈 팀 점수': h_score,
            '경기 결과': game_result   
        }

        baseball_df = baseball_df.append(result_json, ignore_index=True)
        game_results.append(result_json)

        baseball_df, game_crowd, game_time, away_win, away_lose, away_draw, away_hit, away_error, away_bb,\
        home_win, home_lose, home_draw, home_hit, home_error, home_bb,\
        away_pitcher_list, away_pitcher_df, away_hitter_list, away_hitter_df,\
        home_pitcher_list, home_pitcher_df, home_hitter_list, home_hitter_df = \
        reviewData(baseball_df, today, home_team, away_team, home_pitcher_list,
                   away_pitcher_list, home_hitter_list, away_hitter_list)

        # 경기 정보를 dictionary 형태로 변환
        baseball_dict = {
            '날짜': today,
            '명칭': game_title,
            '경기 분류': int(game_round),
            '경기 결과': int(replace_team_num(game_result)),
            '원정 팀 점수': int(a_score),
            '홈 팀 점수': int(h_score),
            '시간': game_time,
            '더블헤더': int(game_dh),
            '관중': int(game_crowd.replace(',','')),
            '장소': int(replace_park_num(game_park)),
            '원정체크' : int(away_check),
            '원정 팀': int(replace_team_num(away_team)),
            '원정 승' : int(away_win),
            '원정 패' : int(away_lose),
            '원정 무' : int(away_draw),
            '원정 안타' : int(away_hit),
            '원정 실책' : int(away_error),
            '원정 사구' : int(away_bb),
            '원정 선발 투수': [],
            '원정 선발 타자': [],
            '원정 교체 투수': [],
            '원정 교체 타자': [],
            '홈 팀': int(replace_team_num(home_team)),
            '홈 체크' : int(home_check),
            '홈 승' : int(home_win),
            '홈 패' : int(home_lose),
            '홈 무' : int(home_draw),
            '홈 안타' : int(home_hit),
            '홈 실책' : int(home_error),
            '홈 사구' : int(home_bb),
            '홈 선발 투수': [],
            '홈 선발 타자': [],
            '홈 교체 투수': [],
            '홈 교체 타자': []
        }

        baseball_dict = makeTeamDict(baseball_dict, baseball_df,
                home_pitcher_list, home_pitcher_df,
                home_hitter_list, home_hitter_df,
                away_pitcher_list, away_pitcher_df,
                away_hitter_list, away_hitter_df,
                today, home_team, away_team)    

        # 변환된 dictionary 를 elastic search 에 저장
        response = es_client.index(index='day_result', doc_type='_doc', id=game_str, body=baseball_dict)         
        print(game_park, int(replace_park_num(game_park)))
    return game_results  


# 프리뷰 데이터 정리
def previewData(baseball_df, today, home_team, away_team, home_pitcher_list, away_pitcher_list, home_hitter_list, away_hitter_list):
    # is_preview = 1
    # 열리지 않은 경기는 선발 투수를 가져옴
    if browser.find_element_by_class_name('sub-tit').text == '선발투수 전력분석':
        starter_button = browser.find_element_by_partial_link_text('선발투수 전력분석')
        click_time = timeit.default_timer()
        starter_button.click()
        
        while True:
            if (timeit.default_timer() - click_time) > 10:
                print('경과 시간(초) :', timeit.default_timer() - click_time)
                starter_button.click()
                click_time = timeit.default_timer()
            try:
                if browser.find_element_by_class_name('sub-tit').text == '선발투수 전력분석':
                    break
            except StaleElementReferenceException:
                continue

        pitcher_names = browser.find_elements_by_class_name('name')
        away_pitcher = pitcher_names[0].text
        away_pitcher_list = [away_pitcher]
        home_pitcher = pitcher_names[1].text
        home_pitcher_list = [home_pitcher]

        print(away_team + '_' + away_pitcher)
        print(home_team + '_' + home_pitcher)
        print(today)
        
    # 열리지 않은 경기는 라인업을 가져옴
    line_up_button = browser.find_element_by_link_text('라인업 분석')
    click_time = timeit.default_timer()
    line_up_button.click()
    while True:
        if (timeit.default_timer() - click_time) > 10:
            print('경과 시간(초) :', timeit.default_timer() - click_time)
            line_up_button.click()
            click_time = timeit.default_timer()
        try:
            if browser.find_element_by_class_name('sub-tit').text == '라인업 분석':
                break
        except StaleElementReferenceException:
            continue

    line_up_df = pd.read_html(browser.page_source, attrs={
        'id': 'tblAwayLineUp'
    })
    line_up_df = pd.DataFrame(line_up_df[0])
    away_hitter_list = list(line_up_df['선수명'].unique())

    line_up_df = pd.read_html(browser.page_source, attrs={
        'id': 'tblHomeLineUp'
    })
    line_up_df = pd.DataFrame(line_up_df[0])
    home_hitter_list = list(line_up_df['선수명'].unique())
    
    return baseball_df, home_pitcher_list, away_pitcher_list, home_hitter_list, away_hitter_list



# 리뷰 데이터 정리
def reviewData(baseball_df, today, home_team, away_team, home_pitcher_list, away_pitcher_list, home_hitter_list, away_hitter_list):
    # 선택된 경기의 리뷰 열기
    review_button = browser.find_element_by_link_text('리뷰')
    review_class_name = review_button.get_attribute('class')
    print(review_class_name)
    click_time = timeit.default_timer()
    review_button.click()
    while True:
        if (timeit.default_timer() - click_time) > 10:
            print('경과 시간(초) :', timeit.default_timer() - click_time)
            review_button.click()
            click_time = timeit.default_timer()
        try:
            if browser.find_element_by_class_name('sub-tit').text == '리뷰':
                break
        except StaleElementReferenceException:
            continue
        except NoSuchElementException:
            browser.refresh()
            time.sleep(10)
            if browser.find_element_by_class_name('sub-tit').text == '리뷰':
                break
    
    # tblAwayHitter1이라는 id의 테이블을 모두 가져와 list 형태로 담음
    away_hitter_df1 = pd.read_html(browser.page_source, attrs={
        'id': 'tblAwayHitter1'
    })

    # table 2 의 경우에는 id 가 table 이 아닌 table 부모 div 태그에 달려있음
    table_list = pd.read_html(browser.page_source)
    away_hitter_df2 = pd.DataFrame(table_list[5])[:-1]
    away_hitter_df3 = pd.read_html(browser.page_source, attrs={
        'id': 'tblAwayHitter3'
    })
    away_hitter_df1 = pd.DataFrame(away_hitter_df1[0])
    away_hitter_df1.dropna(axis='rows', how='all', inplace=True)
    away_hitter_df3 = pd.DataFrame(away_hitter_df3[0])
    # total 기록 제거
    away_hitter_df3 = away_hitter_df3[:-1]
    away_hitter_df = pd.concat((away_hitter_df1, away_hitter_df2), axis=1)
    away_hitter_df = pd.concat((away_hitter_df, away_hitter_df3), axis=1)
    away_hitter_df.rename({'Unnamed: 0':'타순'}, axis='columns', inplace=True)
    away_hitter_df.rename({'Unnamed: 1':'포지션'}, axis='columns', inplace=True)
    away_hitter_df.fillna('0', inplace=True)
    away_hitter_df.replace('-', '0', inplace=True)
    
    away_hitter_list = list(away_hitter_df['선수명'].unique())
    # end for away hitter

    home_hitter_df1 = pd.read_html(browser.page_source, attrs={
        'id': 'tblHomeHitter1'
    })
 
    # table 2 의 경우에는 id 가 table 이 아닌 table 부모 div 태그에 달려있음
    home_hitter_df2 = pd.DataFrame(table_list[8])[:-1]
    home_hitter_df3 = pd.read_html(browser.page_source, attrs={
        'id': 'tblHomeHitter3'
    })
    home_hitter_df1 = pd.DataFrame(home_hitter_df1[0])
    home_hitter_df1.dropna(axis='rows', how='all', inplace=True)
    
    home_hitter_df2.dropna(axis='rows', how='all', inplace=True)
    home_hitter_df3 = pd.DataFrame(home_hitter_df3[0])
    # total 기록 제거
    home_hitter_df3 = home_hitter_df3[:-1]

    home_hitter_df = pd.concat((home_hitter_df1, home_hitter_df2), axis=1)
    home_hitter_df = pd.concat((home_hitter_df, home_hitter_df3), axis=1)
    home_hitter_df.rename({'Unnamed: 0':'타순'}, axis='columns', inplace=True)
    home_hitter_df.rename({'Unnamed: 1':'포지션'}, axis='columns', inplace=True)
    home_hitter_df.fillna('0', inplace=True)
    home_hitter_df.replace('-', '0', inplace=True)    
   
    home_hitter_list = list(home_hitter_df['선수명'].unique())
    # end for home hitter

    away_pitcher_df = pd.read_html(browser.page_source, attrs={
        'id': 'tblAwayPitcher'
    })
    # 투수 테이블은 마지막에 total 또한 선수명에 포함되어 있으므로 마지막 행 제거
    away_pitcher_df = pd.DataFrame(away_pitcher_df[0][:-1])
    away_pitcher_df.dropna(axis='rows', how='all', inplace=True)
    away_pitcher_df.fillna('0', inplace=True)
    away_pitcher_df.replace('-', '0', inplace=True)
    away_pitcher_list = list(away_pitcher_df['선수명'].unique())

    home_pitcher_df = pd.read_html(browser.page_source, attrs={
        'id': 'tblHomePitcher'
    })
    home_pitcher_df = pd.DataFrame(home_pitcher_df[0][:-1])
    home_pitcher_df.dropna(axis='rows', how='all', inplace=True)
    home_pitcher_df.fillna('0', inplace=True)
    home_pitcher_df.replace('-', '0', inplace=True)
    home_pitcher_list = list(home_pitcher_df['선수명'].unique())
  
    team_result_df1 = pd.read_html(browser.page_source, header = 0)[0]
    team_result_df2 = pd.read_html(browser.page_source, header = 0)[2]
    team_result_df = pd.concat((team_result_df1, team_result_df2), axis=1)
    team_result_df.rename({'TEAM':'전적'}, axis='columns', inplace=True)
    team_result_df.rename({'Unnamed: 0':'승패'}, axis='columns', inplace=True)
        
    tmp_game_crowd = browser.find_element_by_id('txtCrowd').text
    tmp_game_crowd = tmp_game_crowd.split()
    tmp_game_time = browser.find_element_by_id('txtStartTime').text 
    tmp_game_time = tmp_game_time.split()
    tmp_away_record = team_result_df['전적'][0].split()
    tmp_home_record = team_result_df['전적'][1].split()
    
    game_crowd = tmp_game_crowd[2].replace(',','')
    game_time = tmp_game_time[2]
    
    away_win = tmp_away_record[0][:-1]
    away_lose = tmp_away_record[1][:-1]
    away_draw =tmp_away_record[2][:-1]
    away_hit = team_result_df['H'][0]
    away_error = team_result_df['E'][0]
    away_bb = team_result_df['B'][0]
    
    home_win = tmp_home_record[0][:-1]
    home_lose = tmp_home_record[1][:-1]
    home_draw = tmp_home_record[2][:-1]
    home_hit = team_result_df['H'][1]
    home_error = team_result_df['E'][1]
    home_bb = team_result_df['B'][1]
    
    return baseball_df, game_crowd, game_time, away_win, away_lose, away_draw, away_hit, away_error, away_bb,\
            home_win, home_lose, home_draw, home_hit, home_error, home_bb,\
            away_pitcher_list, away_pitcher_df, away_hitter_list, away_hitter_df,\
            home_pitcher_list, home_pitcher_df, home_hitter_list, home_hitter_df



# 선수 데이터 기록 정리 (homr/away 투구,타자) - list 형태로 리턴
def makeTeamDict(baseball_dict, baseball_df,
                    home_pitcher_list, home_pitcher_df,
                    home_hitter_list, home_hitter_df,
                    away_pitcher_list, away_pitcher_df,
                    away_hitter_list, away_hitter_df,
                    today, home_team, away_team):    

    # home 투수 기록 정리
    try: home_pitcher_list
    except NameError: home_pitcher_list = None

        
    for pitcher in home_pitcher_list:
        pitcher_dict = {
                '이름': pitcher,
                '선수id': es_searchPlayer(pitcher, home_team, '선수id'),
                '등판': float(home_pitcher_record(home_pitcher_df, pitcher, '등판')),
                '결과': int(replace_result_num(home_pitcher_record(home_pitcher_df, pitcher, '결과'))),
                '승': int(home_pitcher_record(home_pitcher_df, pitcher, '승')),
                '패': int(home_pitcher_record(home_pitcher_df, pitcher, '패')),
                '세': int(home_pitcher_record(home_pitcher_df, pitcher, '세')),
                '이닝': float(replace_ing_num(home_pitcher_record(home_pitcher_df, pitcher, '이닝'))),
                '타자': int(home_pitcher_record(home_pitcher_df, pitcher, '타자')),
                '투구수': int(home_pitcher_record(home_pitcher_df, pitcher, '투구수')),
                '타수': int(home_pitcher_record(home_pitcher_df, pitcher, '타수')),
                '피안타': int(home_pitcher_record(home_pitcher_df, pitcher, '피안타')),
                '홈런': int(home_pitcher_record(home_pitcher_df, pitcher, '홈런')),
                '4사구': int(home_pitcher_record(home_pitcher_df, pitcher, '4사구')),
                '삼진': int(home_pitcher_record(home_pitcher_df, pitcher, '삼진')),
                '실점': int(home_pitcher_record(home_pitcher_df, pitcher, '실점')),
                '자책': int(home_pitcher_record(home_pitcher_df, pitcher, '자책')),
                '평균자책점': round(float(home_pitcher_record(home_pitcher_df, pitcher, '평균자책점')), 2)
            }
        if pitcher_dict['등판'] == 1:
            baseball_dict['홈 선발 투수'].append(pitcher_dict)
        else:
            baseball_dict['홈 교체 투수'].append(pitcher_dict)

    # home 타자 기록 정리
    try: home_hitter_list
    except NameError: home_hitter_list = None
            
    for index, hitter in enumerate(home_hitter_list):
        hitter_dict = {
                '이름': hitter,
                '선수id': es_searchPlayer(hitter, home_team, '선수id'),
                '포지션': home_hitter_record(home_hitter_df, hitter, '포지션'),
                '타순': int(float(home_hitter_record(home_hitter_df, hitter, '타순'))),
                '타수': int(home_hitter_record(home_hitter_df, hitter, '타수')),
                '안타': int(home_hitter_record(home_hitter_df, hitter, '안타')),
                '타점': int(home_hitter_record(home_hitter_df, hitter, '타점')),
                '득점': int(home_hitter_record(home_hitter_df, hitter, '득점')),
                '타율': round(float(home_hitter_record(home_hitter_df, hitter, '타율')), 3)
            }
        
        hitter_check = home_hitter_df['타순'].duplicated()
        order = home_hitter_list.index(hitter)
        if hitter_check[order] == True:
            baseball_dict['홈 교체 타자'].append(hitter_dict)
        else:
            baseball_dict['홈 선발 타자'].append(hitter_dict)

    # away 투수 기록 정리
    try: away_pitcher_list
    except NameError: away_pitcher_list = None
            
    for pitcher in away_pitcher_list:
        pitcher_dict = {
                '이름': pitcher,
                '선수id': es_searchPlayer(pitcher, away_team, '선수id'),
                '등판': float(away_pitcher_record(away_pitcher_df, pitcher, '등판')),
                '결과': int(replace_result_num(away_pitcher_record(away_pitcher_df, pitcher, '결과'))),
                '승': int(away_pitcher_record(away_pitcher_df, pitcher, '승')),
                '패': int(away_pitcher_record(away_pitcher_df, pitcher, '패')),
                '세': int(away_pitcher_record(away_pitcher_df, pitcher, '세')),
                '이닝': float(replace_ing_num(away_pitcher_record(away_pitcher_df, pitcher, '이닝'))),
                '타자': int(away_pitcher_record(away_pitcher_df, pitcher, '타자')),
                '투구수': int(away_pitcher_record(away_pitcher_df, pitcher, '투구수')),
                '타수': int(away_pitcher_record(away_pitcher_df, pitcher, '타수')),
                '피안타': int(away_pitcher_record(away_pitcher_df, pitcher, '피안타')),
                '홈런': int(away_pitcher_record(away_pitcher_df, pitcher, '홈런')),
                '4사구': int(away_pitcher_record(away_pitcher_df, pitcher, '4사구')),
                '삼진': int(away_pitcher_record(away_pitcher_df, pitcher, '삼진')),
                '실점': int(away_pitcher_record(away_pitcher_df, pitcher, '실점')),
                '자책': int(away_pitcher_record(away_pitcher_df, pitcher, '자책')),
                '평균자책점': round(float(away_pitcher_record(away_pitcher_df, pitcher, '평균자책점')), 2)
            }
        if pitcher_dict['등판'] == 1:
            baseball_dict['원정 선발 투수'].append(pitcher_dict)
        else:
            baseball_dict['원정 교체 투수'].append(pitcher_dict)

    # away 타자 기록 정리
    try: away_hitter_list
    except NameError: away_hitter_list = None
            
    for index, hitter in enumerate(away_hitter_list):
        hitter_dict = {
                '이름': hitter,
                '선수id': es_searchPlayer(hitter, away_team, '선수id'),
                '포지션': away_hitter_record(away_hitter_df, hitter, '포지션'),
                '타순': int(float(away_hitter_record(away_hitter_df, hitter, '타순'))),
                '타수': int(away_hitter_record(away_hitter_df, hitter, '타수')),
                '안타': int(away_hitter_record(away_hitter_df, hitter, '안타')),
                '타점': int(away_hitter_record(away_hitter_df, hitter, '타점')),
                '득점': int(away_hitter_record(away_hitter_df, hitter, '득점')),
                '타율': round(float(away_hitter_record(away_hitter_df, hitter, '타율')), 3)
        }
        
        hitter_check = away_hitter_df['타순'].duplicated()
        order = away_hitter_list.index(hitter)
        if hitter_check[order] == True:
            baseball_dict['원정 교체 타자'].append(hitter_dict)
        else:
            baseball_dict['원정 선발 타자'].append(hitter_dict)


    return baseball_dict



# 날짜 형식 맞춤 - 월 (두자리)
def monthFormat(month):       
    if month == '01':
        return '1월(JAN)'
    elif month == '02':        
        return '2월(FEB)'
    elif month == '03':        
        return '3월(MAR)'
    elif month == '04':
        return '4월(APR)'
    elif month == '05':
        return '5월(MAY)'
    elif month == '06':
        return '6월(JUN)'
    elif month == '07':
        return '7월(JUL)'
    elif month == '08':
        return '8월(AUG)'
    elif month == '09':
        return '9월(SEP)'
    elif month == '10':
        return '10월(OCT)'
    elif month == '11':
        return '11월(NOV)'
    elif month == '12':
        return '12월(DEC)'
    else:
        return ''
    
    
# 날짜 형식 맞춤 - 일 (두자리)
def dayFormat(day):       
    if day == '1':
        return '01'
    elif day == '2':        
        return '02'
    elif day == '3':        
        return '03'
    elif day == '4':
        return '04'
    elif day == '5':
        return '05'
    elif day == '6':
        return '06'
    elif day == '7':
        return '07'
    elif day == '8':
        return '08'
    elif day == '9':
        return '09'    
    else:
        return day
    
    
def replace_teamName_num(team_name):
    if team_name == 'NC':
        team_num = 1
    elif team_name == 'KIA':
        team_num = 2
    elif team_name == '두산':
        team_num = 3
    elif team_name == 'SSG':
        team_num = 4
    elif team_name == '한화':
        team_num = 5
    elif team_name == '삼성':
        team_num = 6
    elif team_name == 'KT':
        team_num = 7
    elif team_name == 'LG':
        team_num = 8
    elif team_name == '키움':
        team_num = 9
    elif team_name == '롯데':
        team_num = 10
    else:
        team_num = 0
        
    return team_num



def replace_team_num(team_name):
    if team_name == 'NC':
        team_num = 1
    elif team_name == 'HT':
        team_num = 2
    elif team_name == 'OB':
        team_num = 3
    elif team_name == 'SK':
        team_num = 4
    elif team_name == 'HH':
        team_num = 5
    elif team_name == 'SS':
        team_num = 6
    elif team_name == 'KT':
        team_num = 7
    elif team_name == 'LG':
        team_num = 8
    elif team_name == 'WO':
        team_num = 9
    elif team_name == 'LT':
        team_num = 10
    else:
        team_num = 0
        
    return team_num



def replace_team_db(team_name):
    if team_name == 'NC':
        team_db = 'NC'
    elif team_name == 'KIA':
        team_db = 'HT'
    elif team_name == '두산':
        team_db = 'OB'
    elif team_name == 'SSG':
        team_db = 'SK'
    elif team_name == '한화':
        team_db = 'HH'
    elif team_name == '삼성':
        team_db = 'SS'
    elif team_name == 'KT':
        team_db = 'KT'
    elif team_name == 'LG':
        team_db = 'LG'
    elif team_name == '키움':
        team_db = 'WO'
    elif team_name == '롯데':
        team_db = 'LT'
    else:
        team_db = 0
        
    return team_db



def replace_park_num(park_name):
    if park_name == '잠실':
        park_num = 1
    elif park_name == '문학':
        park_num = 2
    elif park_name == '대구':
        park_num = 3
    elif park_name == '수원':
        park_num = 4
    elif park_name == '고척':
        park_num = 5
    elif park_name == '광주':
        park_num = 6
    elif park_name == '사직':
        park_num = 7
    elif park_name == '대전':
        park_num = 8
    elif park_name == '창원':
        park_num = 9
    elif park_name == '마산':
        park_num = 10
    elif park_name == '포항':
        park_num = 11
    elif park_name == '울산':
        park_num = 12
    elif park_name == '청주':
        park_num = 13
    else:
        park_num = 0
        
    return park_num



def replace_result_num(result_name):
    if result_name == '승':
        result_num = 1
    elif result_name == '패':
        result_num = -1
    elif result_name == '세':
        result_num = 2
    elif result_name == '홀':
        result_num = 3
    else:
        result_num = 0
        
    return result_num




def replace_ing_num(ing_num):
    s_num = ing_num.split()
    if len(s_num) == 1:
        if s_num[0] == '1/3':
            num = 0.33
        elif s_num[0] == '2/3':
            num = 0.67
        else:
            num = int(s_num[0])
        
    elif len(s_num) == 2:
        x = int(s_num[0])
        if s_num[1] == '1/3':
            y = 0.33
        elif s_num[1] == '2/3':
            y = 0.67
        num = x + y
    
    return num



def away_pitcher_record(away_pitcher_df, pitcher, col):
    if '선발' == str(away_pitcher_df[away_pitcher_df['선수명'] == pitcher][col].array[0]):
        pitcher_starting = 1
    else:
        pitcher_starting = str(away_pitcher_df[away_pitcher_df['선수명'] == pitcher][col].array[0])
        
    return pitcher_starting
    
def home_pitcher_record(home_pitcher_df, pitcher, col):
    if '선발' == str(home_pitcher_df[home_pitcher_df['선수명'] == pitcher][col].array[0]):
        pitcher_starting = 1
    else:
        pitcher_starting = str(home_pitcher_df[home_pitcher_df['선수명'] == pitcher][col].array[0])
        
    return pitcher_starting
    
def away_hitter_record(away_hitter_df, hitter, col):
    return str(away_hitter_df[away_hitter_df['선수명'] == hitter][col].array[0])
    
def home_hitter_record(home_hitter_df, hitter, col):
    return str(home_hitter_df[home_hitter_df['선수명'] == hitter][col].array[0])



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
                                     '팀약칭': team_name
                                 }
                                }
                            ]
                        }
                    }
                }
            }
        })

        if len(response['hits']['hits']) > 1:
            tmp_res = 0
            for i in range(0,len(response['hits']['hits'])):
                res = int(response['hits']['hits'][i]['_source']['연봉'])

                if tmp_res == 0:
                    pass

                elif res > tmp_res:
                    res = response['hits']['hits'][i]['_source'][col]

                else:
                    pass

                tmp_res = res
            return res

        elif len(response['hits']['hits']) == 1:
            res = response['hits']['hits'][0]['_source'][col]
            return res
            
        else:
            res = 0
            return res
    elif es_client.indices.exists(index="player_retired"):
        response = es_client.search(index='player_retired', size=800, body={
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
                                     '팀약칭': team_name
                                 }
                                }
                            ]
                        }
                    }
                }
            }
        })
        res = response['hits']['hits'][0]['_source'][col]
        return res  
    else:
        pass
    
    
# 과거 경기 기록 가져오기(1개월 단위)
def beforeRunCrawler(year, month):
    beforeResultCrawling(beforeResultUrl(year, month))


    
# 오늘날짜에 KBO 데이터 Crawling
def runCrawler(today):

    yesterday = today - datetime.timedelta(1)
    print('오늘 : ', today)          # 2015-04-19 12:11:32.669083
    print('어제 : ', yesterday)
    
    today_year = today.strftime('%Y')
    today_month = today.strftime('%m')
    today_day = str(today.day)

    yesterday_year = yesterday.strftime('%Y')
    yesterday_month = yesterday.strftime('%m')
    yesterday_day = str(yesterday.day)

    print(today_year, today_month, today_day)
    print(yesterday_year, yesterday_month, yesterday_day)
    
    exists_result = dayResultCrawling(yesterday_year, yesterday_month, yesterday_day)        
    exists_preview = dayPreviewCrawling(today_year, today_month, today_day)    

    return exists_result, exists_preview


if __name__ == '__main__':
# 직전 경기 결과 / 당일 경기 라인업
    today = datetime.date.today()
    runCrawler(today)
    print('완료')
    

