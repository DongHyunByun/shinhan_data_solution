# 자료별 수행시기
# RUN_SCHEDULE = {순번 : [파일명, (나가는 월들), 나가는일자, 수행날짜와 원천의 차이]
EVERY_MONTH = (1,2,3,4,5,6,7,8,9,10,11,12)

# schedule
RUN_SCHEDULE = {
                "1"    :["리얼탑KB아파트단지매핑",EVERY_MONTH, "25", None],
                "2"    :["리얼탑 kb아파트평형시세매핑(sas)",EVERY_MONTH,"25", None],
                "4"    :["건축물신축단가관리(excel)",(1,),"말일", None],
                "5"    :["산단격차율",EVERY_MONTH,"5", 2],
                "6"    :["토지격차율(sas)",(1,8),"말일", None],
                "8"    :["전국주택 매매가격지수",EVERY_MONTH,"20",1],
                "9"    :["오피스텔 매매가격지수",EVERY_MONTH,"20",1],
                "10"   :["용도지역별 지가지수",EVERY_MONTH,"말일",1],
                "11"   :["리얼탑토지특성정보",(0,),None,"?",0], #공개 후 1개월 이내
                "32"   :["이용상황별 지가변동률",EVERY_MONTH,"20",2],
                "33-51":["공동주택 통합 매매실거래가격지수 ~ 연립 다세대 매매 평균가격",EVERY_MONTH,"20",3], # 37은 제외, 아래에 있음
                "37"   :["아파트 매매 실거래가격지수_시군구분기별",(1,4,7,10),"20","?"],
                "52"   :["경기종합지수(2020=100) (10차)",EVERY_MONTH,"20",2],
                "53"   :["품목별 소비자물가지수(품목성질별: 2020=100)",EVERY_MONTH,"20",1],
                "54"   :["생산자물가지수(품목별)(2020=100)",EVERY_MONTH,"20",2],
                "55"   :["면적별 건축물 현황",(6,),"말일",None],
                "56"   :["용도별 건축물 현황",(6,),"말일",None],
                "57"   :["층수별 건축물 현황",(6,),"말일",None],
                "58"   :["동수별 연면적별 건축착공현황",EVERY_MONTH,"말일",2],
                "59"   :["동수별 연면적별 건축허가현황",EVERY_MONTH,"말일",2],
                "60"   :["시도별 건축물착공현황",EVERY_MONTH,"말일",2],
                "61"   :["연도별 건축허가현황",(7,),"말일",None],
                "62"   :["시도별 재건축사업 현황 누계",(7,),"말일",None],
                "63"   :["(新)주택보급률",(4,),"20",None],
                "64"   :["주택 멸실현황",(3,),"말일",None],
                "65"   :["부문별 주택건설 인허가실적(월별누계)",EVERY_MONTH,"말일",2],
                "66"   :["주택건설실적총괄",(3,),"20",None],
                "67"   :["주택규모별 주택건설 인허가실적(월별누계)",EVERY_MONTH,"말일",2],
                "68"   :["지역별 주택건설 인허가실적",(3,),"20",None],
                "69"   :["공사완료후 미분양현황",EVERY_MONTH,"20",3],
                "70"   :["규모별 미분양현황",EVERY_MONTH,"말일",2],
                "71"   :["미분양현황종합",(2,),"20",None],
                "72"   :["시군구별 미분양현황",EVERY_MONTH,"말일",2],
                "73"   :["공동주택현황",EVERY_MONTH,"20",2],
                "74"   :["주택유형별 주택준공실적_ 다가구구분",EVERY_MONTH,"말일",2],
                "75"   :["주택유형별 착공실적_다가구 구분,월계",EVERY_MONTH,"말일",2],
                "76-80":["부동산시장 소비심리지수~주택전세시장 소비심리지수",EVERY_MONTH,"말일",1],
                "81"   :["국토부 상가수익률",(2,5,8,11),"말일",None],
                "82"   :["K-REMAP지수",EVERY_MONTH,"말일",2],
                "83"   :["전국산업단지현황통계",(3,6,9,12),"말일",None],
                "84"   :["국가산업단지산업동향",EVERY_MONTH,"말일",2],
                "85"   :["팩토리온 등록공장현황",EVERY_MONTH,"말일",2],
                "86"   :["이용상황별 지가지수",EVERY_MONTH,"20",2],
                "87"   :["주요정책사업(혁신도시) 지가지수",EVERY_MONTH,"20",2],
                "88"   :["예금취급기관의 가계대출[주택담보대출+기타대출] 지역별(월별)",EVERY_MONTH,"20",3],
}

# key
ex_key_dict = {
    "1":[0,1,4],
    "2":[0,1,5],
    "3":[0,1,5],
    "4":[0,1,3,6],
    "5":[0,1,3,6],
    "6":[0,1,4], # 실제데이터받고 한번더확인
    "7":[0,1,5],
    "8":[0,1,4],
    "9":[0,1,3,6],
    "10":[0,1,3,6],
    "11":[0,1,4],
    "12":[0,1,4],
    "13":[0,1,4],
    "14":[0,1,4],
    "15":[0,1,4],
    "16":[0,1,4],
    "17":[0,1,4],
    "18":[0,1,4],
    "19":[0,1,4],
    "20":[0,1,4],
    "21":[0,31],
    "22":[0,1,3,8],
    "23":[0,1,5],
    "24":[0,1,3,7],
    "25":[0,1,3,7],
    "26":[0,1,3,7],
    "27":[0,1,2,6],
    "28":[0,1,2,6],
    "29":[0,1,3,5,6,9,11,14],
    "30":[0,1,5], # 실제데이터받고 한번더확인
    "31":[0, 1, 3, 4, 8], # 실제데이터받고 한번더확인
    "32":[0, 1, 3, 6], # 실제데이터받고 한번더확인
    "33":[0, 1, 3, 6], # 실제데이터 받고 한번더 확인
    "34":[0,1,3,6],
    "35":[0, 1, 4],
    "36":[0,1,3,6],
    "37":[0,1,4],
    "38":[0,1,3,6],
    "39":[0,1,3,6],
    "40":[0,1,4],
    "41":[0,1,4],
    "42":[0,1,3,6],
    "43":[0,1,3,6],
    "44":[0,1,3,6],
    "45":[0,1,4],
    "46":[0,1,4],
    "47":[0,1,4],
    "48":[0,1,4],
    "49":[0,1,4],
    "50":[0,1,2,18],
    "51":[0,1,4],
    "52":[0,1],
    "53":[0,1,2],
    "54":[0,1,2],
    "55":[0,1,3,6],
    "56":[0,1,4],
    "57":[0,1,3,6]
}