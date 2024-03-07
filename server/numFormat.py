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
    if day == '01':
        return '1'
    elif day == '02':        
        return '2'
    elif day == '03':        
        return '3'
    elif day == '04':
        return '4'
    elif day == '05':
        return '5'
    elif day == '06':
        return '6'
    elif day == '07':
        return '7'
    elif day == '08':
        return '8'
    elif day == '09':
        return '9'    
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

def replace_num_teamName(team_num):
    if team_num == 1:
        team_name = 'NC'
    elif team_num == 2:
        team_name = 'KIA'
    elif team_num == 3:
        team_name = '두산'
    elif team_num == 4:
        team_name = 'SSG'
    elif team_num == 5:
        team_name = '한화'
    elif team_num == 6:
        team_name = '삼성'
    elif team_num == 7:
        team_name = 'KT'
    elif team_num == 8:
        team_name = 'LG'
    elif team_num == 9:
        team_name = '키움'
    elif team_num == 10:
        team_name = '롯데'
    else:
        team_name = 'Unnamed'
        
    return team_name

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

def replace_db_team(team_db):
    if team_db == 'NC':
        team_name = 'NC'
    elif team_db == 'HT':
        team_name = 'KIA'
    elif team_db == 'OB':
        team_name = '두산'
    elif team_db == 'SK':
        team_name = 'SSG'
    elif team_db == 'HH':
        team_name = '한화'
    elif team_db == 'SS':
        team_name = '삼성'
    elif team_db == 'KT':
        team_name = 'KT'
    elif team_db == 'LG':
        team_name = 'LG'
    elif team_db == 'WO':
        team_name = '키움'
    elif team_db == 'LT':
        team_name = '롯데'
    else:
        team_name = 'Unnamed'
        
    return team_name

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

def replace_park_name(park_num):
    if park_num == 1:
        park_name = '잠실'
    elif park_num == 2:
        park_name = '문학'
    elif park_num == 3:
        park_name = '대구'
    elif park_num == 4:
        park_name = '수원'
    elif park_num == 5:
        park_name = '고척'
    elif park_num == 6:
        park_name = '광주'
    elif park_num == 7:
        park_name = '사직'
    elif park_num == 8:
        park_name = '대전'
    elif park_num == 9:
        park_name = '창원'
    elif park_num == 10:
        park_name = '마산'
    elif park_num == 11:
        park_name = '포항'
    elif park_num == 12:
        park_name = '울산'
    elif park_num ==  13:
        park_name = '청주'
    else:
        park_name = '기타'
        
    return park_name

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
