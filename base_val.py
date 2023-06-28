# schedule
from datetime_func import *

class BaseVal:
    str_d = None
    last_day = None

    RUN_SCHEDULE = None # RUN_SCHEDULE = {순번 : [파일명, (나가는 월들), 나가는일자, 수행날짜와 원천의 차이]
    EX_KEY_DICT = None
    FINAL_FILE_NAME_DICT = None
    INCREASE_AMOUNT = None



    def __init__(self, str_d):
        self.str_d = str_d  # yyyymm
        self.last_day = return_last_day_of_yyyymm(str_d[:4],str_d[4:])
        EVERY_MONTH = (1,2,3,4,5,6,7,8,9,10,11,12)

        # RUN_SCHEDULE = {순번 : [파일명, (나가는 월들), 나가는일자, 수행날짜와 원천의 차이]
        RUN_SCHEDULE = {
                        "1"    :["리얼탑KB아파트단지매핑",EVERY_MONTH, "25", None],
                        "2"    :["리얼탑 kb아파트평형시세매핑(sas)",EVERY_MONTH,"25", None],
                        "4"    :["건축물신축단가관리(excel)",(1,),"말일", None, None],
                        "5"    :["산단격차율",EVERY_MONTH,"5", 2,],
                        "6"    :["토지격차율(sas)",(1,8),"말일", None],
                        "8"    :["전국주택 매매가격지수",EVERY_MONTH,"20",1],
                        "9"    :["오피스텔 매매가격지수",EVERY_MONTH,"20",1],
                        "10"   :["용도지역별 지가지수",EVERY_MONTH,"말일",1],
                        "11"   :["리얼탑토지특성정보",(0,),None,"?",0], #공개 후 1개월 이내
                        "32"   :["이용상황별 지가변동률",EVERY_MONTH,"20",2],
                        "33-51":["공동주택 통합 매매실거래가격지수 ~ 연립 다세대 매매 평균가격",EVERY_MONTH,"20",3], # 37은 제외, 아래에 있음
                        "37"   :["아파트 매매 실거래가격지수_시군구분기별",(1,4,7,10),"20",None],
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
                        "71"   :["미분양현황종합",(2,),"20",None,None],
                        "72"   :["시군구별 미분양현황",EVERY_MONTH,"말일",2],
                        "73"   :["공동주택현황",EVERY_MONTH,"20",2,None],
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
        self.RUN_SCHEDULE = RUN_SCHEDULE

        # 최종파일이름
        FINAL_FILE_NAME_DICT = {
                        "1"    :None,
                        "2"    :None,
                        "4"    :None,
                        "5"    :None,
                        "6"    :None,
                        "8"    :None,
                        "9"    :None,
                        "10"   :f"rtp_landpi_inf_{self.str_d}.txt",
                        "11"   :None, #공개 후 1개월 이내
                        "32"   :f"1.rtp_usecase_jg_{self.str_d}20.txt",
                        "33"   :f"2.rtp_gdhse_t_inf_{self.str_d}20.txt",
                        "34"   :f"3.rtp_gdhse_sea_inf_{self.str_d}20.txt",
                        "35"   :f"4.rtp_sz_apt_t_inf_{self.str_d}20.txt",
                        "36"   :f"5.rtp_sz_apt_js_inf_{self.str_d}20.txt",
                        "37"   :None,
                        "38"   :f"7.rtp_apt_t_inf_{self.str_d}20.txt",
                        "39"   :f"8.rtp_apt_js_inf_{self.str_d}20.txt",
                        "40"   :f"9.rtp_apt_sz_mid_{self.str_d}20.txt",
                        "41"   :f"10.rtp_apt_sz_avg_{self.str_d}20.txt",
                        "42"   :f"11.rtp_apt_t_mid_{self.str_d}20.txt",
                        "43"   :f"12.rtp_apt_t_avg_{self.str_d}20.txt",
                        "44"   :f"13.rtp_apt_js_mid_{self.str_d}20.txt",
                        "45"   :f"14.rtp_apt_js_avg_{self.str_d}20.txt",
                        "46"   :f"15.rtp_sz_yd_s_inf_{self.str_d}20.txt",
                        "47"   :f"16.rtp_yd_t_inf_{self.str_d}20.txt",
                        "48"   :f"17.rtp_yd_sz_mid_{self.str_d}20.txt",
                        "49"   :f"18.rtp_yd_sz_avg_{self.str_d}20.txt",
                        "50"   :f"19.rtp_yd_t_mid_{self.str_d}20.txt",
                        "51"   :f"20.rtp_yd_t_avg_{self.str_d}20.txt",
                        "52"   :f"21.rtp_cei_inf_{self.str_d}20.txt",
                        "53"   :f"22.rtp_item_cpi1_inf_{self.str_d}20.txt",
                        "54"   :f"23.rtp_item_ppi_inf_{self.str_d}20.txt",
                        "55"   :f"24.rtp_sqr_con_{self.str_d}{self.last_day}.txt",
                        "56"   :f"25.rtp_yongdo_con_{self.str_d}{self.last_day}.txt",
                        "57"   :f"26.rtp_floor_con_{self.str_d}{self.last_day}.txt",
                        "58"   :f"27.rtp_d_alsqr_st_{self.str_d}{self.last_day}.txt",
                        "59"   :f"28.rtp_d_alsqr_pm_{self.str_d}{self.last_day}.txt",
                        "60"   :f"29.rtp_sido_st_{self.str_d}{self.last_day}.txt",
                        "61"   :None,
                        "62"   :None,
                        "63"   :None,
                        "64"   :None,
                        "65"   :f"34.rtp_field_hse_pm_m_{self.str_d}{self.last_day}.txt",
                        "66"   :None,
                        "67"   :f"36.rtp_hse_sz_pm_{self.str_d}{self.last_day}.txt",
                        "68"   :None,
                        "69"   :f"38.rtp_gsat_us_{self.str_d}20.txt",
                        "70"   :f"39.rtp_sz_us_{self.str_d}{self.last_day}.txt",
                        "71"   :None,
                        "72"   :f"41.rtp_sigungu_us_{self.str_d}{self.last_day}.txt",
                        "73"   :f"42.rtp_gdhse_now_{self.str_d}20.txt",
                        "74"   :f"43.rtp_hse_ut_m_{self.str_d}{self.last_day}.txt",
                        "75"   :f"44.rtp_hse_st_m_{self.str_d}{self.last_day}.txt",
                        "76"   :f"45.rtp_re_csi_inf_{self.str_d}{self.last_day}.txt",
                        "77"   :f"46.rtp_hse_csi_inf_{self.str_d}{self.last_day}.txt",
                        "78"   :f"47.rtp_ld_csi_inf_{self.str_d}{self.last_day}.txt",
                        "79"   :f"48.rtp_hse_t_csi_inf_{self.str_d}{self.last_day}.txt",
                        "80"   :f"49.rtp_hse_js_csi_inf_{self.str_d}{self.last_day}.txt",
                        "81"   :None,
                        "82"   :f"51.rtp_k_remap_{self.str_d}{self.last_day}.txt",
                        "83"   :f"52.ked_sandan_st_{self.str_d}{self.last_day}.txt",
                        "84"   :f"53.ked_sandan_in_{self.str_d}{self.last_day}.txt",
                        "85"   :f"ked_fac_on_stt_{self.str_d}{self.last_day}.txt",
                        "86"   :f"55.rtp_usecase_jg_index_inf_{self.str_d}20.txt",
                        "87"   :f"56.rtp_hyuksin_city_jg_index_inf_{self.str_d}20.txt",
                        "88"   :f"57.rtp_householdloan_{self.str_d}20.txt",
        }
        self.FINAL_FILE_NAME_DICT = FINAL_FILE_NAME_DICT

        # 외부통계 key 컬럼 인덱스 번호
        EX_KEY_DICT = {
            "32":[0,1,4],
            "33":[0,1,5],
            "34":[0,1,4],
            "35":[0,1,3,6],
            "36":[0,1,3,6],
            "37":[0,1,4], # 실제데이터받고 한번더확인
            "38":[0,1,5],
            "39":[0,1,4],
            "40":[0,1,3,6],
            "41":[0,1,3,6],
            "42":[0,1,4],
            "43":[0,1,4],
            "44":[0,1,4],
            "45":[0,1,4],
            "46":[0,1,4],
            "47":[0,1,4],
            "48":[0,1,4],
            "49":[0,1,4],
            "50":[0,1,4],
            "51":[0,1,4],
            "52":[0,31],
            "53":[0,1,3,8],
            "54":[0,1,5],
            "55":[0,1,3,7],
            "56":[0,1,3,7],
            "57":[0,1,3,7],
            "58":[0,1,2,6],
            "59":[0,1,2,6],
            "60":[0,1,3,5,6,9,11,14],
            "61":[0,1,5], # 실제데이터받고 한번더확인
            "62":[0, 1, 3, 4, 8], # 실제데이터받고 한번더확인
            "63":[0, 1, 3, 6], # 실제데이터받고 한번더확인
            "64":[0, 1, 3, 6], # 실제데이터 받고 한번더 확인
            "65":[0,1,3,6],
            "66":[0, 1, 4],
            "67":[0,1,3,6],
            "68":[0,1,4],
            "69":[0,1,3,6],
            "70":[0,1,3,6],
            "71":[0,1,4],
            "72":[0,1,4],
            "73":[0,1,3,6],
            "74":[0,1,3,6],
            "75":[0,1,3,6],
            "76":[0,1,4],
            "77":[0,1,4],
            "78":[0,1,4],
            "79":[0,1,4],
            "80":[0,1,4],
            "81":[0,1,2,18],
            "82":[0,1,4],
            "83":[0,1],
            "84":[0,1,2],
            "85":[0,1,2],
            "86":[0,1,3,6],
            "87":[0,1,4],
            "88":[0,1,3,6]
        }
        self.EX_KEY_DICT = EX_KEY_DICT

        # 원천과의 날짜 차이(실행월-n일 = 원천)
        MONTH_DIFF = {
            "1": None,
            "2": None,
            "4": None,
            "5": 2,
            "6": None,
            "8": 1,
            "9": 1,
            "10":1,
            "11":None,  # 공개 후 1개월 이내
            "32":2,
            "33": 2,
            "34": 3,
            "35": 3,
            "36": 4,
            "37": None,
            "38": 2,
            "39": 4,
            "40": 3,
            "41": 3,
            "42": 3,
            "43": 3,
            "44": 4,
            "45": 4,
            "46": 3,
            "47": 2,
            "48": 3,
            "49": 3,
            "50": 3,
            "51": 3,
            "52": 2,
            "53": 1,
            "54": 2,
            "55": None,
            "56": None,
            "57": None,
            "58": 2,
            "59": 2,
            "60": 2,
            "61": None,
            "62": None,
            "63": None,
            "64": None,
            "65": 2,
            "66": None,
            "67": 2,
            "68": None,
            "69": 3,
            "70": 2,
            "71": None,
            "72": 2,
            "73": 2,
            "74": 2,
            "75": 2,
            "76": 1,
            "77": 1,
            "78": 1,
            "79": 1,
            "80": 1,
            "81": None,
            "82": 2,
            "83": None,
            "84": 2, # 제공데이터가 원천과 똑같다
            "85": 2,
            "86": 2,
            "87": 2,
            "88": 3,
        }
        self.MONTH_DIFF = MONTH_DIFF

        # 가장최근날짜의 데이터개수(추가되는 개수)
        INCREASE_AMOUNT = {
            "1": None,
            "2": None,
            "4": None,
            "5": None,
            "6": None,
            "8": None,
            "9": None,
            "10": 366,
            "11": None,
            "32": 2009,
            "33": 9,
            "34": 9,
            "35": 20,
            "36": 20,
            "37": None,
            "38": 28,
            "39": 28,
            "40": 20,
            "41": 20,
            "42": 25,
            "43": 25,
            "44": 25,
            "45": 25,
            "46": 2,
            "47": 9,
            "48": 2,
            "49": 2,
            "50": 6,
            "51": 6,
            "52": 1,
            "53": 36,
            "54": 2,
            "55": 144,
            "56": 90,
            "57": 144,
            "58": 22,
            "59": 22,
            "60": 23256,
            "61": None,
            "62": None,
            "63": None,
            "64": None,
            "65": 234,
            "66": 8,
            "67": 90,
            "68": 18,
            "69": 1275,
            "70": 95,
            "71": 27,
            "72": 254,
            "73": 54,
            "74": 102,
            "75": 102,
            "76": 17,
            "77": 17,
            "78": 17,
            "79": 17,
            "80": 17,
            "81": None,
            "82": 188,
            "83": None,
            "84": 468,
            "85": None,
            "86": 2009,
            "87": 10,
            "88": 54,
        }
        self.INCREASE_AMOUNT = INCREASE_AMOUNT