# 자료별 수행시기
# RUN_SCHEDULE = {순번 : [파일명, (나가는 월들), 나가는일자)
EVERY_MONTH = (1,2,3,4,5,6,7,8,9,10,11,12)

RUN_SCHEDULE = {
                "1"    :["리얼탑KB아파트단지매핑",EVERY_MONTH, "25"],
                "2"    :["리얼탑 kb아파트평형시세매핑(sas)",EVERY_MONTH,"25"],
                "4"    :["건축물신축단가관리(excel)",(1,),"말일"],
                "5"    :["산단격차율",EVERY_MONTH,"5"],
                "6"    :["토지격차율(sas)",(1,8),"말일"],
                "8"    :["전국주택 매매가격지수",EVERY_MONTH,"20"],
                "9"    :["오피스텔 매매가격지수",EVERY_MONTH,"20"],
                "10"   :["용도지역별 지가지수",EVERY_MONTH,"말일"],
                "11"   :["리얼탑토지특성정보",(0,),None], #공개 후 1개월 이내
                "32"   :["이용상황별 지가변동률",EVERY_MONTH,"20"],
                "33-51":["공동주택 통합 매매실거래가격지수 ~ 연립 다세대 매매 평균가격",EVERY_MONTH,"20"], # 37은 제외, 아래에 있음
                "37"   :["아파트 매매 실거래가격지수_시군구분기별",(1,4,7,10),"20"],
                "52"   :["경기종합지수(2020=100) (10차)",EVERY_MONTH,"20"],
                "53"   :["품목별 소비자물가지수(품목성질별: 2020=100)",EVERY_MONTH,"20"],
                "54"   :["생산자물가지수(품목별)(2020=100)",EVERY_MONTH,"20"],
                "55"   :["면적별 건축물 현황",(6,),"말일"],
                "56"   :["용도별 건축물 현황",(6,),"말일"],
                "57"   :["층수별 건축물 현황",(6,),"말일"],
                "58"   :["동수별 연면적별 건축착공현황",EVERY_MONTH,"말일"],
                "59"   :["동수별 연면적별 건축허가현황",EVERY_MONTH,"말일"],
                "60"   :["시도별 건축물착공현황",EVERY_MONTH,"말일"],
                "61"   :["연도별 건축허가현황",(7,),"말일"],
                "62"   :["시도별 재건축사업 현황 누계",(7,),"말일"],
                "63"   :["(新)주택보급률",(4,),"20"],
                "64"   :["주택 멸실현황",(3,),"말일"],
                "65"   :["부문별 주택건설 인허가실적(월별누계)",EVERY_MONTH,"말일"],
                "66"   :["주택건설실적총괄",(3,),"20"],
                "67"   :["주택규모별 주택건설 인허가실적(월별누계)",EVERY_MONTH,"말일"],
                "68"   :["지역별 주택건설 인허가실적",(3,),"20"],
                "69"   :["공사완료후 미분양현황",EVERY_MONTH,"20"],
                "70"   :["규모별 미분양현황",EVERY_MONTH,"말일"],
                "71"   :["미분양현황종합",(2,),"20"],
                "72"   :["시군구별 미분양현황",EVERY_MONTH,"말일"],
                "73"   :["공동주택현황",EVERY_MONTH,"20"],
                "74"   :["주택유형별 주택준공실적_ 다가구구분",EVERY_MONTH,"말일"],
                "75"   :["주택유형별 착공실적_다가구 구분,월계",EVERY_MONTH,"말일"],
                "76-80":["부동산시장 소비심리지수~주택전세시장 소비심리지수",EVERY_MONTH,"말일"],
                "81"   :["국토부 상가수익률",(2,5,8,11),"말일"],
                "82"   :["K-REMAP지수",EVERY_MONTH,"말일"],
                "83"   :["전국산업단지현황통계",(3,6,9,12),"말일"],
                "84"   :["국가산업단지산업동향",EVERY_MONTH,"말일"],
                "85"   :["팩토리온 등록공장현황",EVERY_MONTH,"말일"],
                "86"   :["이용상황별 지가지수",EVERY_MONTH,"20"],
                "87"   :["주요정책사업(혁신도시) 지가지수",EVERY_MONTH,"20"],
                "88"   :["예금취급기관의 가계대출[주택담보대출+기타대출] 지역별(월별)",EVERY_MONTH,"20"],
}