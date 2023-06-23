# 원천데이터를 크롤링하는 class

# 기본 lib
import time
import tqdm
import warnings
import sys
import re

# 크롤링
import requests

from bs4 import BeautifulSoup as bs

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

from fake_useragent import UserAgent
from urllib import request,parse

# 링커
from datetime_func import *
from file_sys_func import *
from run_month import *

warnings.filterwarnings(action='ignore')
class FileDown:
    str_d = None
    d = None
    browser = None
    path = None

    def __init__(self,project_path,str_d,work_day,RUN_SCHEDULE):
        self.str_d = str_d
        self.project_path = project_path
        self.work_day = work_day
        self.RUN_SCHEDULE = RUN_SCHEDULE

        self.data_path = f"{self.project_path}\\data"
        self.path = f"{self.project_path}\\data\\{str_d}"

        self.d = datetime.strptime(str_d, '%Y%m')

        self.y = str_d[:4]
        self.last_y = str(int(self.y)-1)
        self.m = str_d[5:].lstrip('0')

        self.func_dict={"1"    : [],#리얼탑KB아파트단지매핑
                        "2"    : [],#리얼탑 kb아파트평형시세매핑(sas)
                        "4"    : [],#건축물신축단가관리(excel)
                        "5"    : [self.filedown_5,return_y_m_before_n(self.d, 2)],
                        "6"    : [],#토지격차율(sas)
                        "8"    : [self.filedown_8],
                        "9"    : [self.filedown_9,return_y_m_before_n(self.d, 1)],
                        "10"   : [self.filedown_10,return_y_m_before_n(self.d, 1)],
                        "11"   : [],#리얼탑토지특성정보(공개 후 1개월)
                        "32"   : [self.filedown_32_ex1,return_y_m_before_n(self.d, 2)],
                        "33-51": [self.filedown_33_51_ex2_20, return_y_m_before_n(self.d, 3)],
                        "52"   : [self.filedown_52_ex21],
                        "53"   : [self.filedown_53_ex22],
                        '54'   : [self.filedown_54_ex23],
                        "55"   : [self.filedown_55_ex24],
                        "56"   : [self.filedown_56_ex25],
                        "57"   : [self.filedown_57_ex26],
                        "58"   : [self.filedown_58_ex27],
                        "59"   : [self.filedown_59_ex28],
                        "60"   : [self.filedown_60_ex29],
                        "61"   : [self.filedown_61_ex30],
                        "62"   : [self.filedown_62_ex31],# 시도별 재건축사업 현황 누계(매년 7월말), 업데이트 시점 확인필요
                        "63"   : [],#(新)주택보급률(매년 4월20일)
                        "64"   : [],#주택 멸실현황(매년 3월말일)
                        "65"   : [self.filedown_65_ex34],
                        "66"   : [],#주택건설실적총괄(매년 3월20일)
                        "67"   : [self.filedown_67_ex36],
                        "68"   : [],#지역별 주택건설 인허가실적(매년 3월20일)
                        "69"   : [self.filedown_69_ex38],
                        "70"   : [self.filedown_70_ex39],
                        "71"   : [],#미분양현황종합(매년 2월 20일)
                        "72"   : [self.filedown_72_ex41],
                        "73"   : [self.filedown_73_ex42],
                        "74"   : [self.filedown_74_ex43],
                        "75"   : [self.filedown_75_ex44],
                        "76-80": [self.filedown_76_80_ex45_49],
                        "81"   : [self.filedown_81_ex50],
                        "82"   : [self.filedown_82_ex51],
                        "83"   : [self.filedown_83_ex52],
                        "84"   : [self.filedown_84_ex53, return_y_m_before_n_v2(self.d, 2)],
                        "85"   : [], #팩토리온 등록공장현황
                        "86"   : [self.filedown_86_ex55, return_y_m_before_n(self.d, 2)],
                        "87"   : [self.filedown_87_ex56],
                        "88"   : [self.filedown_88_ex57],
        }

        dir_dict = {str_d : {"5" : {"원천":None, "원천_처리후":None},
                             "20": {"원천":None, "원천_처리후":None},
                             "말일": {"원천":None, "원천_처리후":None},}
                    }
        mkdir_dfs(self.data_path, dir_dict)

        now_month_date = {"num":[],"file_name":[],"day":[],"crawling":[]}
        for num,vals in RUN_SCHEDULE.items():
            file_name = vals[0]
            months = vals[1]
            day = vals[2]
            if (int(self.m) in months) and (self.work_day in (day,"all")):
                print(f"{(num+'.'+file_name).center(60,'-')}")
                now_month_date["num"].append(num)
                now_month_date["file_name"].append(file_name)
                now_month_date["day"].append(day)
                if self.func_dict[num]:
                    if len(self.func_dict[num])==2:
                        func = self.func_dict[num][0]
                        param = self.func_dict[num][1]
                        if self.try_twice(func,param):
                            now_month_date["crawling"].append("성공")
                        else:
                            now_month_date["crawling"].append("실패")
                    else:
                        func = self.func_dict[num][0]
                        if self.try_twice(func):
                            now_month_date["crawling"].append("성공")
                        else:
                            now_month_date["crawling"].append("실패")

                else:
                    now_month_date["crawling"].append("함수없음")
                    print("함수없음")

        pd.DataFrame(now_month_date).to_csv(f"{self.project_path}/monthly_file/{self.str_d}{self.work_day}.csv", index=False, encoding='ANSI')

    def try_twice(self,func,param=(),n=3):
        '''
        func을 n번 반복한다
        '''
        for i in range(n):
            print(f"{i+1}번째시도 : ",end=" ")
            try:
                func(*param)
                return True
            except:
                pass

            if i==n-1:
                print("실패")
                return False

    def filedown_5(self,y,m):
        file_num = "5"
        print(f"{file_num}.산단격차율")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]

        urls = ["https://www.factoryon.go.kr/bbs/frtblRecsroomBbsList.do",
                "https://www.factoryon.go.kr/bbs/frtblRecsroomBbsList.do?pageIndex=2"] # 크롤링할 싸이트

        for url in urls :
            page = self.try_request(url)
            soup = bs(page.text, "html.parser")

            fin_flag=False
            for row in soup.select('div.subCont>table.cellType_b.inpCell.mt10>tbody>tr')[0].select('td>p.inTxt.al'):
                title = row.text

                if  f"({y}.{m.zfill(2)}월말기준)_전국_지식산업센터현황" == title:
                    p = re.compile('\(([^)]+)')
                    num = p.findall(row.a["href"])[0]
                    fin_flag = True
                    break

            if fin_flag:
                row_url = f"https://www.factoryon.go.kr/bbs/frtblRecsroomBbsDetail.do?selectBbsSn={num}"
                row_page = self.try_request(row_url)
                soup = bs(row_page.text, "html.parser")

                down_url_last = soup.select("table.cellType_a.inpCell.lastLine>tbody>tr")[3].a["href"]
                file_name = soup.select("table.cellType_a.inpCell.lastLine>tbody>tr")[3].a.text
                down_url = f"https://www.factoryon.go.kr{down_url_last}"

                request.urlretrieve(down_url, f"{self.path}/{day_folder_name}/원천/5.산단격차율_{file_name}")
                break
            else:
                continue


    def filedown_8(self):
        file_num="8"
        print(f"{file_num}.전국주택 매매가격지수")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]

        file_dir_dict = {
            "단독":
                {"월간_매매가격지수_단독.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21411&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=S&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_월세가격지수_단독.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21432&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=R2&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_월세통합가격지수_단독.xlsx":"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21431&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=R1&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_전세가격지수_단독.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21421&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=D&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_전월세통합지수_단독.xlsx"  :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_24411&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=T&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_준월세가격지수_단독.xlsx"  :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21433&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=R3&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_준전세가격지수_단독.xlsx"  :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21434&houseSubGbn=HOUSE_INDEX&aptType=7&weekFlag=M&trGbn=R4&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all"},
            "아파트":
                {"월간_매매가격지수_아파트.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21211&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=S&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_월세가격지수_아파트.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21232&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=R2&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_월세통합가격지수_아파트.xlsx":"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21231&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=R1&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_전세가격지수_아파트.xlsx"   :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21221&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=D&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_전월세통합지수_아파트.xlsx" :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_24211&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=T&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_준월세가격지수_아파트.xlsx" :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21233&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=R3&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_준전세가격지수_아파트.xlsx" :"https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21234&houseSubGbn=HOUSE_INDEX&aptType=1&weekFlag=M&trGbn=R4&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all"},
            "연립":
                {"월간_매매가격지수_연립다세대.xlsx"   : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21311&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=S&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_월세가격지수_연립다세대.xlsx"   : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21332&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=R2&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_월세통합가격지수_연립다세대.xlsx": "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21331&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=R1&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_전세가격지수_연립다세대.xlsx"   : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21321&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=D&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_전월세통합지수_연립다세대.xlsx" : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_24311&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=T&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_준월세가격지수_연립다세대.xlsx" : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21333&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=R3&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_준전세가격지수_연립다세대.xlsx" : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21334&houseSubGbn=HOUSE_INDEX&aptType=3&weekFlag=M&trGbn=R4&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all"},
            "종합":
                {"월간_매매가격지수_종합.xlsx"   : "https://www.reb.or.kr/r-one/statistics/excelDownLoadAllType1.do?statCd=HOUSE_21111&houseSubGbn=HOUSE_INDEX&weekFlag=M&aptType=0&trGbn=S&priceGbn=&excelType=all",
                 "월간_월세가격지수_종합.xlsx"   : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21132&houseSubGbn=HOUSE_INDEX&aptType=0&weekFlag=M&trGbn=R2&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_월세통합가격지수_종합.xlsx": "https://www.reb.or.kr/r-one/statistics/excelDownLoadAllType1.do?statCd=HOUSE_21131&houseSubGbn=HOUSE_INDEX&weekFlag=M&aptType=0&trGbn=R1&priceGbn=&excelType=all",
                 "월간_전세가격지수_종합.xlsx"   : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21121&houseSubGbn=HOUSE_INDEX&aptType=0&weekFlag=M&trGbn=D&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_전월세통합지수_종합.xlsx"  : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_24111&houseSubGbn=HOUSE_INDEX&aptType=0&weekFlag=M&trGbn=T&regionCd=&regulation=true&researchDate_s=200311&priceGbn=&excelType=all",
                 "월간_준월세가격지수_종합.xlsx"  : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21133&houseSubGbn=HOUSE_INDEX&aptType=0&weekFlag=M&trGbn=R3&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all",
                 "월간_준전세가격지수_종합.xlsx"  : "https://www.reb.or.kr/r-one/statistics/getPriceIndicesListAJAX.do?statCd=HOUSE_21134&houseSubGbn=HOUSE_INDEX&aptType=0&weekFlag=M&trGbn=R4&regionCd=&regulation=true&researchDate_s=201506&priceGbn=&excelType=all"},
        }

        # 폴더 경로생성
        start_path = f"{self.path}/{day_folder_name}/원천/"
        dir_dict = {"8.전국주택 매매가격지수": {"단독": None, "아파트": None, "연립": None, "종합": None}}
        mkdir_dfs(start_path, dir_dict)

        for folder in file_dir_dict:
            name_url_dict = file_dir_dict[folder]
            for file_name, url in name_url_dict.items():
                print(file_name)
                request.urlretrieve(url, f"{self.path}/{day_folder_name}/원천/8.전국주택 매매가격지수/{folder}/{file_name}")

    def filedown_9(self, y, m):
        file_num="9"
        print(f"{file_num}.오피스탤 매매가격지수")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        url = "https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=OFST"
        page = self.try_request(url)
        soup = bs(page.text, "html.parser")

        # 날짜
        if y and m:
            y = f"{y}년"
            m = f"{m}월"

            # 파일다운로드
            for a in soup.select('tr>td>a.nttInfoBtn'):
                if (y in a.text) and (m in a.text):
                    post_id = a["data-id"]
                    down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보세스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=LFR&listCo=10&searchType=sj&searchValue="

                    file_list = self.try_request(down_file_response).json()["nttFileList"]
                    for file in file_list:
                        if "xlsx" in file["fileNm"]:
                            file_name = file["fileNm"]
                            file_type = file_name.split('.')[-1]
                            down_key = file["dwldUrl"]
                            down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                            break
                    request.urlretrieve(down_url, f"{folder_path}/9.{file_name}")
        # else:
        #     post_id = soup.select('tr>td>a.nttInfoBtn')[0]["data-id"]
        #     down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보세스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=LFR&listCo=10&searchType=sj&searchValue="
        #
        #     file = self.try_request(down_file_response).json()["nttFileList"][1]
        #
        #     file_name = file["fileNm"]
        #     file_type = file_name.split('.')[-1]
        #     down_key = file["dwldUrl"]
        #     down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
        #
        #     request.urlretrieve(down_url, f"{folder_path}/9.{file_name}")

    def filedown_10(self,y=None,m=None):
        file_num = "10"
        print(f"{file_num}.용도지역별 지가지수")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        url = "https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=LFR"
        page = self.try_request(url)
        soup = bs(page.text, "html.parser")

        # 날짜
        if y and m:
            y = f"{y}년"
            m = f"{m}월"

            # 파일다운로드
            for a in soup.select('tr>td>a.nttInfoBtn'):
                if (y in a.text) and (m in a.text):
                    post_id = a["data-id"]
                    down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보세스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=LFR&listCo=10&searchType=sj&searchValue="

                    file = self.try_request(down_file_response).json()["nttFileList"][0]

                    file_name = file["fileNm"]
                    file_type = file_name.split('.')[-1]
                    down_key = file["dwldUrl"]
                    down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"

                    request.urlretrieve(down_url, f"{folder_path}/{file_name}")
        else:
            post_id = soup.select('tr>td>a.nttInfoBtn')[0]["data-id"]
            down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보세스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=LFR&listCo=10&searchType=sj&searchValue="

            file = self.try_request(down_file_response).json()["nttFileList"][1]

            file_name = file["fileNm"]
            file_type = file_name.split('.')[-1]
            down_key = file["dwldUrl"]
            down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"

            request.urlretrieve(down_url, f"{folder_path}/10.{file_name}")


    def filedown_32_ex1(self,y,m):
        file_num = "32"
        ex_file_num = "1"
        print(f"{file_num}.이용상황별 지가변동률 , 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        page = self.try_request('https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=LFR')
        soup = bs(page.text, "html.parser")
        soup.select('tr>td>a.nttInfoBtn')

        # 날짜
        y = f"{y}년"
        m = f"{m}월"

        # 파일다운로드
        for a in soup.select('tr>td>a.nttInfoBtn'):
            if (y in a.text) and (m in a.text):
                post_id = a["data-id"]
                down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보세스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=LFR&listCo=10&searchType=sj&searchValue="

                files = self.try_request(down_file_response).json()["nttFileList"]
                for file in files:
                    if "지가변동률" in file["fileNm"]:
                        file_name = file["fileNm"]
                        file_type = file_name.split('.')[-1]
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url,f"{folder_path}/{ex_file_num}.{file_type}")

    def filedown_33_51_ex2_20(self,y,m):
        file_num="33-51"
        ex_file_num = "2-20"
        print(f"{file_num}.공동주택 통합 매매 실거래가격지수~연립 다세대 매매 평균가격 , 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        page = self.try_request('https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=TSPIA')
        soup = bs(page.text, "html.parser")

        # 날짜
        y = f"{y}년"
        m = f"{m}월"

        # 파일다운로드
        for a in soup.select('tr>td>a.nttInfoBtn'):
            if (y in a.text) and (m in a.text) :
                post_id = a["data-id"]
                down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보시스템&currPage=1&bbsId=1106&nttSn={post_id}&mi=9509&selectType=&cnrsBbsUseAt=&searchCate=TSPIA&listCo=10&searchType=sj&searchValue="

                files = self.try_request(down_file_response).json()["nttFileList"]
                for file in files:
                    if "공동주택 실거래가격지수 통계표" in file["fileNm"]:
                        # file_name = file["fileNm"]
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url,f"{folder_path}/{ex_file_num}.xlsm")

        # # 시트나누기
        # row_down_path = f"{folder_path}/{file_name}"
        # pd.read_excel(row_down_path, sheet_name='매매_공동주택').to_csv(f"{folder_path}/2.csv",index=False,encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매_공동주택_계절조정').to_csv(f"{folder_path}/3.csv", index=False,encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매_아파트').to_csv(f"{folder_path}/4.csv", index=False,encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 전세_아파트').to_csv(f"{folder_path}/5.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='분기별_매매 증감률_아파트').to_csv(f"{folder_path}/6.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매_아파트').to_csv(f"{folder_path}/7.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='전세_아파트').to_csv(f"{folder_path}/8.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매 중위_아파트').to_csv(f"{folder_path}/9.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매 평균_아파트').to_csv(f"{folder_path}/10.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매 중위_아파트').to_csv(f"{folder_path}/11.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매 평균_아파트').to_csv(f"{folder_path}/12.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='전세 중위_아파트').to_csv(f"{folder_path}/13.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='전세 평균_아파트').to_csv(f"{folder_path}/14.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매_연립다세대').to_csv(f"{folder_path}/15.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매_연립다세대').to_csv(f"{folder_path}/16.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매 중위_연립 다세대').to_csv(f"{folder_path}/17.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='규모별 매매 평균_연립 다세대').to_csv(f"{folder_path}/18.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매 중위_연립 다세대').to_csv(f"{folder_path}/19.csv", index=False, encoding='cp949')
        # pd.read_excel(row_down_path, sheet_name='매매 평균_연립 다세대').to_csv(f"{folder_path}/20.csv", index=False, encoding='cp949')

    def filedown_52_ex21(self):
        file_num = "52"
        ex_file_num = "21"
        print(f"{file_num}.경기종합지수, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_1C8015&orgId=101&listId=J1_1&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path,ex_file_num)

    def filedown_53_ex22(self):
        file_num = "53"
        ex_file_num = "22"
        print(f"{file_num}.품목별 소비자물가지수, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_1J20112&orgId=101&listId=P2_6&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1,browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 시도별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_1"]').click)
        self.delay_after_func(1, Select(browser.find_element(By.ID, "fancytree_1CheckOption")).select_by_value, ("allLowLevel",)) #하위전체선택
        self.delay_after_func(1, browser.switch_to.alert.accept)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li/span/span[@class="fancytree-checkbox"]').click)
        self.delay_after_func(1, browser.switch_to.alert.accept)

        # 품목별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_2"]').click)
        # self.delay_after_func(1, Select(browser.find_element(By.ID, "fancytree_2CheckOption")).select_by_value, ("allLowLevel",))
        # self.delay_after_func(1, browser.switch_to.alert.accept)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fancytree_2Btn"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[1]/span/span[@class="fancytree-checkbox"]').click) # 총지수
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/span/span[@class="fancytree-expander"]').click) # 집세내리기1
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/ul/li/span/span[@class="fancytree-expander"]').click)  # 집세내리기2
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/ul/li/span/span[@class="fancytree-checkbox"]').click)  # 집세체크
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/ul/li/ul/li[1]/span/span[@class="fancytree-checkbox"]').click)  # 전세체크
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/ul/li/ul/li[2]/span/span[@class="fancytree-checkbox"]').click)  # 전세체크

        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_54_ex23(self):
        file_num = "54"
        ex_file_num = "23"
        print(f"{file_num}.생산자물가지수(품목별), 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_404Y016&orgId=301&listId=P2_301002&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 계정코드별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_1"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fancytree_1Btn"]').click) #전체해제

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li[@class="fancytree-lastsib"]/span/span[@class="fancytree-expander"]').click) #창내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li[@class="fancytree-lastsib"]/ul/li[@class="fancytree-lastsib"]/span/span[@class="fancytree-expander"]').click) #창내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li[@class="fancytree-lastsib"]/ul/li[@class="fancytree-lastsib"]/ul/li[5]/span/span[@class="fancytree-expander"]').click) #창내리기

        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/span/span[@class="fancytree-checkbox"]').click)  #총지수 체크
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[@class="fancytree-lastsib"]/ul/li[5]/ul/li[2]/span/span[@class="fancytree-checkbox"]').click) #비주거건물인대
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[@class="fancytree-lastsib"]/ul/li[5]/ul/li[4]/span/span[@class="fancytree-checkbox"]').click) #비주거용부동산관리
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_55_ex24(self):
        file_num = "55"
        ex_file_num = "24"
        print(f"{file_num}.면적별 건축물 현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_540&orgId=116&listId=M1_5&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)  # 시점 탭 선택
        year_size = len(browser.find_elements(By.XPATH, '//*[@id="selectStrtTimeY"]/option'))
        self.delay_after_func(1, browser.find_element(By.XPATH, f'//*[@id="selectStrtTimeY"]/option[{year_size}]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_56_ex25(self):
        file_num = "56"
        ex_file_num = "25"
        print(f"{file_num}.용도별 건축물 현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_522&orgId=116&listId=M1_5&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 시점탭 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, f'//*[@id="selectStrtTimeY"]/option[2]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_57_ex26(self):
        file_num = "57"
        ex_file_num = "26"
        print(f"{file_num}.층수별 건축물 현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_524&orgId=116&listId=M1_5&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 시점탭 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, f'//*[@id="selectStrtTimeY"]/option[2]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_58_ex27(self):
        file_num = "58"
        ex_file_num = "27"
        print(f"{file_num}.동수별 연면적별 건축착공현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_6905&orgId=116&listId=M1_6&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 계정코드별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-1"]/li/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]/span[@class="fancytree-checkbox"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)  # 시점 탭 선택
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAllM"]').click)  # 날짜 전체선택 해재
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-5"]/li/span[@class="fancytree-node fancytree-exp-n fancytree-ico-c"]/span[@class="fancytree-checkbox"]').click)  # 최신날짜선택
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_59_ex28(self):
        file_num = "59"
        ex_file_num = "28"
        print(f"59.동수별 연면적별 건축허가현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(30,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_6906&orgId=116&listId=M1_6&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 계정코드별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-1"]/li/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]/span[@class="fancytree-checkbox"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)  # 시점 탭 선택
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAllM"]').click)  # 날짜 전체선택 해재
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-5"]/li/span[@class="fancytree-node fancytree-exp-n fancytree-ico-c"]/span[@class="fancytree-checkbox"]').click)  # 최신날짜선택
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_60_ex29(self):
        file_num = "60"
        ex_file_num = "29"
        print(f"{file_num}.시도별 건축물착공현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(25,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_2200&orgId=116&listId=M1_6&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_1"]').click)  # 시도명탭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li/span').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_2"]').click) # 용도별 탭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAll2"]').click)  # 전체선택
        self.delay_after_func(1, browser.switch_to.alert.accept)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li/span').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_3"]').click)  # 용도별(상세) 탭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAll3Span"]').click)  # 전체선택
        self.delay_after_func(1, browser.switch_to.alert.accept)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-4"]/li/span').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_4"]').click)  # 구분
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAll4"]').click)  # 전체선택

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_5"]').click)  # 레벨
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAll5"]').click) # 전체선택

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabTimeText"]').click)  # 시점 탭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAllM"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li/span').click)
        self.delay_after_func(1, browser.switch_to.alert.accept)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabItemText"]').click)  # 항목탭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="treeCheckAll0"]').click)

        # 항목 하나씩 선택
        L = ["콘크리트","철골","철골철근콘크리트","조적","목조","기타"]
        for i in range(1,7):
            print(L[i-1])
            if i!=1:
                self.delay_after_func(5, browser.find_element(By.XPATH, f'//*[@id="ft-id-1"]/li[{i}]/span').click)
            self.delay_after_func(5, browser.find_element(By.XPATH, f'//*[@id="ft-id-1"]/li[{i+1}]/span').click)
            self.delay_after_func(5, browser.switch_to.alert.accept)

            self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="searchImg2"]').click)
            mk_time = time.time()
            self.delay_after_func(30, browser.find_element(By.XPATH, '//*[@id="downLargeBtn"]').click)

            if file_check_func(folder_path,mk_time):
                change_last_file(folder_path, f"{ex_file_num}_{L[i-1]}")
            else:
                print("실패!")

            self.delay_after_func(4, browser.find_element(By.XPATH, '//*[@id="pop_downglarge2"]/div[@class="pop_top"]/span[@class="closeBtn"]').click)  # 취소
            self.delay_after_func(4, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click) #설정창열기

    def filedown_61_ex30(self):
        file_num = "61"
        ex_file_num = "30"
        print(f"61.연도별 건축허가현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_6920&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_62_ex31(self):
        file_num = "62"
        ex_file_num = "31"
        print(f"{file_num}.시도별 재건축사업 현황 누계, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_6192&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_65_ex34(self):
        file_num = "65"
        ex_file_num = "34"
        print(f"{file_num}.부문별 주택건설 인허가실적(월별누계), 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_1946&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        # 행렬교체
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le2"]').click) # 시점선택
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시도별상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le3"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_67_ex36(self):
        file_num = "67"
        ex_file_num = "36"
        print(f"{file_num}.주택규모별 주택건설 인허가실적(월별누계), 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_1952&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le3"]').click) #시점 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 규모상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le3"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 조회설정
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="tabClassText_4"]/span[@class="ui-accordion-header-icon ui-icon ui-icon-triangle-1-e"]').click) # 시도별 리스트 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-5"]/li[3]/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]').click)  # 시도별 리스트 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-5"]/li[6]/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]').click)  # 시도별 리스트 내리기
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 시점설정
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btn_time"]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="timePopListMBtn"]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li[1]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnTimeAccept"]').click)

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def filedown_69_ex38(self):
        file_num = '69'
        ex_file_num = "38"
        print(f"{file_num}.공사완료후 미분양현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=32&hFormId=5328&hDivEng=&month_yn=',))

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]/option[3]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="main"]/div/div[@class="search-wrap"]/div[@class="search-form unfold"]/div[@class="search-item-detail"]/div[@class="search-item-group"]/div[@class="search-form-item"]/div[@class="mu-item-group"]/button').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        change_last_file(folder_path, ex_file_num)

    def filedown_70_ex39(self):
        file_num = "70"
        ex_file_num = "39"
        print(f"70.규모별 미분양현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=32&hFormId=2080&hDivEng=&month_yn=',))

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]/option[3]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="main"]/div/div[@class="search-wrap"]/div[@class="search-form unfold"]/div[@class="search-item-detail"]/div[@class="search-item-group"]/div[@class="search-form-item"]/div[@class="mu-item-group"]/button').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        change_last_file(folder_path, ex_file_num)

    def filedown_39_kosis(self):
        file_num = "39"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_2080&orgId=116&listId=I1_2&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le2"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 부문상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri2"]').click)  # 규모상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 다운로드
        self.kosis_download(browser,"csv")
        change_last_file(folder_path, file_num)

    def filedown_72_ex41(self):
        file_num="72"
        ex_file_num = "41"
        print(f"{file_num}.시군구별 미분양현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=32&hFormId=2082&hDivEng=&month_yn=',))

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]/option[3]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="main"]/div/div[@class="search-wrap"]/div[@class="search-form unfold"]/div[@class="search-item-detail"]/div[@class="search-item-group"]/div[@class="search-form-item"]/div[@class="mu-item-group"]/button').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(10, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        change_last_file(folder_path, ex_file_num)

    def filedown_41_kosis(self):
        file_num = "41"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_2082&orgId=116&listId=I1_2&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le3"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 다운로드
        self.kosis_download(browser, "csv")
        change_last_file(folder_path, file_num)

    def filedown_73_ex42(self):
        file_num = "73"
        ex_file_num = "42"
        print(f"{file_num}.공동주택현황, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=419&hFormId=5882&hDivEng=&month_yn=',))

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="sStart"]/option[3]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="main"]/div/div[@class="search-wrap"]/div[@class="search-form unfold"]/div[@class="search-item-detail"]/div[@class="search-item-group"]/div[@class="search-form-item"]/div[@class="mu-item-group"]/button').click)

        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH,'//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        change_last_file(folder_path, ex_file_num)

    def filedown_74_ex43(self):
        file_num = "74"
        ex_file_num = "43"
        print(f"74.주택유형별 주택준공실적_ 다가구구분, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(45,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_5373&orgId=116&listId=116_11626_003&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 대분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click) # 중분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click) # 소분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 조회설정
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_2"]').click) # 대분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[1]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[2]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 시점
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="btn_time"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li[2]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li[3]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li[4]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnTimeAccept"]').click) #적용

        # 다운로드
        self.kosis_download(browser, "xlsx")
        change_last_file(folder_path, ex_file_num)

    def filedown_75_ex44(self):
        file_num = "75"
        ex_file_num = "44"
        print(f"{file_num}.주택유형별 착공실적다가구 구분, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(45,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_5387&orgId=116&listId=116_11626_002&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 대분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click) # 중분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 조회설정
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_2"]').click) # 대분류
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[1]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[2]').click)
        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드(시점후 바로 다운로드창으로)
        self.kosis_download(browser, "xlsx")
        change_last_file(folder_path, ex_file_num)

    def filedown_76_80_ex45_49(self):
        file_num = "76-80"
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        index_to_file_num={2:"45", 3:"46",6:"47",4:"48",5:"49"}
        index_to_file_name={2:"76.부동산시장 소비심리지수",
                            3:"77.주택시장 소비심리지수",
                            4:"78.토지시장 소비심리지수",
                            5:"79.주택매매시장 소비심리지수",
                            6:"80.주택전세시장 소비심리지수"}

        folder_path = f"{self.path}\\{day_folder_name}\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://kremap.krihs.re.kr/grid/grid?jisu=167',))

        # 45~49 하나씩 다운로드
        for i in range(2,7):
            ex_file_num = index_to_file_num[i]
            print(f"{index_to_file_name[i]}, 외부통계 번호 : {ex_file_num}")

            self.delay_after_func(3, browser.find_element(By.XPATH, f'//*[@id="Middle_ContentPlaceHolder2_Data_kind2"]/option[{i}]').click)  # 조회클릭

            self.kremap_move_back_year(browser)

            # 엑셀다운로드
            button = browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_excel_down"]')
            self.delay_after_func(3, ActionChains(browser).move_to_element(button).click(button).perform)
            change_last_file(folder_path, ex_file_num)

        # browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_Data_kind2"]/option[4]').click()  # n번째
        # button = browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_Data_kind2"]')
        # self.delay_after_func(3, ActionChains(browser).move_to_element(button).click(button).perform)  # 조회클릭
        #
        # button = browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_Data_kind2"]/option[3]')
        # self.delay_after_func(1, ActionChains(browser).move_to_element(button).click(button).perform)

        # browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_Data_kind2"]/option[3]').click()

    def filedown_45_kosis(self):
        file_num = "45"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_39002_01&orgId=390&listId=I2_3&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, file_num)

    def filedown_46_kosis(self):
        file_num = "46"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_39002_02&orgId=390&listId=I2_3&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, file_num)

    def filedown_47_kosis(self):
        file_num = "47"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_39002_03&orgId=390&listId=I2_3&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, file_num)

    def filedown_48_kosis(self):
        file_num = "48"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_39002_04&orgId=390&listId=I2_3&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, file_num)

    def filedown_49_kosis(self):
        file_num = "49"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_39002_05&orgId=390&listId=I2_3&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, file_num)

    def filedown_81_ex50(self):
        '''
         2월 : (작년)4분기
         5월 : 1분기
         8월 : 2분기
        11월 : 3분기
        '''
        file_num = "81"
        ex_file_num = "50"
        print(f"81.국토부 상가수익률, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        month_to_quater = {"2": 4, "5": 1, "8": 2, "11": 3}
        page = self.try_request('https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=RCS')
        soup = bs(page.text, "html.parser")

        # 파일다운로드
        for a in soup.select('tr>td>a.nttInfoBtn'):
            if "상업용부동산 임대동향조사" in a.text and f"{month_to_quater[self.m]}분기" in a.text:
                post_id = a["data-id"]
                down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보시스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&searchCate=LFR&listCo=10&searchType=sj"
                files = self.try_request(down_file_response).json()["nttFileList"]
                for file in files:
                    if "상업용부동산 임대동향조사 통계표(공표용)" in file["fileNm"]:
                        file_name = file["fileNm"]
                        file_type = file_name.split('.')[-1]
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url, f"{folder_path}/{ex_file_num}.{file_name}")
                break


    def filedown_82_ex51(self):
        file_num = "82"
        ex_file_num = "51"
        print(f"{file_num}.K-REMAP지수, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]

        ua = UserAgent()
        headers = {
            'User-Agent': ua.random,
        }

        # [전국, 수도권, 비수도권 값]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://kremap.krihs.re.kr/grid/grid?jisu=166',))

        self.kremap_move_back_year(browser)

        # 엑셀다운로드
        button = browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_excel_down"]')
        self.delay_after_func(3, ActionChains(browser).move_to_element(button).click(button).perform)
        file_name = "51.KREMAP_refer"
        change_last_file(folder_path, file_name)

        kremap_refer = pd.read_excel(f"{folder_path}/{file_name}.xlsx")

        # [시도구]
        sido_cd = ['11', '26', '27', '28', '29', '30', '31', '36', '41', '42', '43', '44', '45', '46', '47', '48', '50']

        df = pd.DataFrame({'기준년월': [], '지역명': [], '기상도': [], '진단지수': [], '전월대비': []})

        i=2
        while(1):
            yyyymm = (self.d - relativedelta(months=i)).strftime('%Y%m')
            print(yyyymm)
            yyyy = yyyymm[:4]
            mm = yyyymm[4:]

            # 시도군
            for sido in sido_cd:
                URL = f'https://kremap.krihs.re.kr/menu4/SystemIntro?area_cd={sido}&item_cd=0&Gbn=MONTH&year={yyyy}&month={mm}'
                rq = requests.get(URL, headers=headers, verify=False)
                try:
                    df_tp = pd.read_html(rq.text, encoding='UTF-8')[5] # 마지막에 에러
                except:
                    break

                head = df_tp["지역명"][0]
                if head:
                    df_tp["지역명"][1:] = str(head) + df_tp["지역명"][1:].astype(str)

                df_tp["지역명"] = df_tp["지역명"].str.replace(" ","")
                df_tp.insert(0, '기준년월', yyyymm)

                df = pd.concat([df, df_tp], axis=0, ignore_index=True)

                time.sleep(0.5)

            # 전국, 수도권, 비수도권
            df_add = pd.DataFrame({"기준년월":[yyyymm,yyyymm,yyyymm],
                                    "지역명":["전국","수도권","비수도권"],
                                   "진단지수":[float(kremap_refer[kremap_refer["지역명"]=="전국"][f"{yyyy}-{mm}"]),
                                           float(kremap_refer[kremap_refer["지역명"]  == "수도권"][f"{yyyy}-{mm}"]),
                                           float(kremap_refer[kremap_refer["지역명"]  == "비수도권"][f"{yyyy}-{mm}"])]
                                   })
            df = pd.concat([df, df_add], axis=0, ignore_index=True)
            df.to_csv(f"{self.path}\\{day_folder_name}\\원천\\{ex_file_num}.KREMAP_CRW.csv", index=False, encoding='ANSI')

            if yyyymm=="201108":
                break
            else:
                i+=1

    def filedown_83_ex52(self):
        month_to_quater = {3:[self.last_y,"4"],
                           6:[self.y,"1"],
                           9:[self.y,"2"],
                           12:[self.y,"3"]}

        base_yy = month_to_quater[int(self.m)][0][2:]
        quater = month_to_quater[int(self.m)][1]

        file_num = "83"
        ex_file_num = "52"
        file_name = f"{file_num}.전국산업단지현황통계, 외부통계번호 : {ex_file_num}"
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        print(file_name)

        folder_path = f"{self.path}\\{day_folder_name}\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://www.kicox.or.kr/user/bbs/BD_selectBbsList.do?q_bbsCode=1036&q_clCode=2',))

        is_file = False
        for i,row in enumerate(browser.find_elements(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr')):
            if (f"전국산업단지현황통계 통계표({base_yy}.{quater}분기)" in row.text):
                self.delay_after_func(2, browser.find_element(By.XPATH, f'//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr[{i+1}]/td[@class="subject"]').click)
                # content_href = browser.find_element(By.XPATH, f'//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr[{i+1}]/td[@class="subject"]/a').get_attribute('href')
                # browser.get(content_href)
                is_file = True
                break

        if not is_file:
            print("파일없음")
            return

        title = browser.find_element(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="detail-area"]/div[@class="util"]/span[@class="file-download-list"]/span[1]/a').text
        down_url = browser.find_element(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="detail-area"]/div[@class="util"]/span[@class="file-download-list"]/span[1]/a').get_attribute('href')

        self.delay_after_func(5, browser.get, (down_url,))
        change_last_file(folder_path, f"{ex_file_num}. 산업단지현황조사_{base_yy}.{quater}분기")

    def filedown_84_ex53(self,y=None,m=None):
        file_num = "84"
        ex_file_num = "53"
        file_name = f"{file_num}.국가산업단지산업동향, 외부통계번호 : {ex_file_num}"
        print(file_name)
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://www.kicox.or.kr/user/bbs/BD_selectBbsList.do?q_bbsCode=1036&q_clCode=1',))

        if y and m:
            find_title = f"{y}.{m}월 주요 국가산업단지"
            for i,row in enumerate(browser.find_elements(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr')):
                if find_title in row.text:
                    self.delay_after_func(1, browser.find_element(By.XPATH,f'//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr[{i+1}]').click)
                    break
        else:
            print("날짜없음")
            return
            # self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="table"]/table/tbody/tr[1]').click)

        down_url = browser.find_element(By.XPATH, '//*[@id="contents"]/div[@class="cont-body"]/div[@class="detail-area"]/div[@class="util"]/span[@class="file-download-list"]/span[1]/a').get_attribute('href')
        self.delay_after_func(5, browser.get, (down_url,))
        change_last_file(folder_path, f"{ex_file_num}.주요 국가산업단지 산업동향({y[2:]}.{m}월 공시용)")

    def filedown_86_ex55(self, y, m):
        file_num = "86"
        ex_file_num = "55"
        print(f"{file_num}.이용상황별 지가지수, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}/{day_folder_name}/원천"

        page = self.try_request('https://www.reb.or.kr/r-one/na/ntt/selectNttList.do?mi=9509&bbsId=1106&searchCate=LFR')
        soup = bs(page.text, "html.parser")

        # 날짜
        y = f"{y}년"
        m = f"{m}월"

        # 파일다운로드
        for a in soup.select('tr>td>a.nttInfoBtn'):
            if (y in a.text) and (m in a.text):
                post_id = a["data-id"]
                down_file_response = f"https://www.reb.or.kr/r-one/na/ntt/fileDownChk.do?qt=&divId=r-one&sysName=부동산통계정보시스템&currPage=&bbsId=1106&nttSn={post_id}&mi=9509&searchCate=LFR&listCo=10&searchType=sj"
                files = self.try_request(down_file_response).json()["nttFileList"]
                for file in files:
                    if "지가지수" in file["fileNm"]:
                        file_name = file["fileNm"]
                        file_type = file_name.split('.')[-1]
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url, f"{folder_path}/{ex_file_num}.{file_type}")

    def filedown_87_ex56(self):
        file_num = "87"
        ex_file_num = "56"
        print(f"{file_num}.주요정책사업(혁신도시) 지가지수, 외부통계 번호 : {ex_file_num}")
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ("https://www.reb.or.kr/r-one/statistics/statisticsViewer.do?menuId=LFR_13200",))
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="S_FileBox"]').click)

        change_last_file(folder_path, ex_file_num)

    def filedown_88_ex57(self):
        file_num = "88"
        day_folder_name = self.RUN_SCHEDULE[file_num][2]
        folder_path = f"{self.path}\\{day_folder_name}\\원천"

        # 첫번째
        ex_file_num = "57_1"
        print(f"{file_num}.예금취급기관의 가계대출[주택담보대출+기타대출] 지역별(월별), 외부통계 번호 : {ex_file_num}")
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20, browser.get, ('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_151Y003&orgId=301&listId=S1_301006_003_006&dbUser=NSI.&language=ko',))

        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

        # 두번째
        ex_file_num = "57_2"
        print(f"{file_num}.예금취급기관의 가계대출[주택담보대출+기타대출] 지역별(월별), 외부통계 번호 : {ex_file_num}")
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_151Y006&orgId=301&listId=S1_301006_003_006&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 계정코드별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_1"]').click) #아래탭 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fancytree_1Btn"]').click) #전체해제
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li[@class="fancytree-lastsib"]/span/span[@class="fancytree-expander"]').click) #목록 보이기

        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/span/span[@class="fancytree-checkbox"]').click)  #맨위 체크박스 체크
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[1]/span/span[@class="fancytree-checkbox"]').click) #주택담보대출
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[2]/span/span[@class="fancytree-checkbox"]').click)  # 주택담보대출

        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click) # 조회

        # 다운로드
        self.kosis_download(browser)
        change_last_file(folder_path, ex_file_num)

    def kosis_init_broswer(self,folder_path):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("prefs", {
            'download.default_directory': folder_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        browser = webdriver.Chrome(chrome_options=chrome_options, executable_path="chromedriver.exe")
        return browser

    def kosis_download(self,browser,type="excel"):
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_download"]').click)
        if type=="csv":
            self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="csvradio"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="downDesc"]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="pop_downgrid2"]/div[@class="pop_content2"]/div[@class="btn_lay"]/span[@class="confirmBtn"]/a').click)

    def delay_after_func(self, delay_sec, func, args=None):
        if args:
            func(*args)
        else:
            func()

        time.sleep(delay_sec)

    def try_request(self,url,params={}):
        '''
        request를 수행한다. 실패시 30초 대기 후 다시 시도한다. 총 3회 시도한다
        '''
        for i in range(3):
            try:
                post_page = requests.get(url,params=params)
                return post_page
            except:
                if i == 2:
                    sys.exit("3회 시도 실패로 강제종료")
                print(f"{i + 1}번째 연결실패, 15초 후 재시도")
                time.sleep(15)

    def save_download(self,down_folder,down_path):
        '''
        crawling함수 작동 후 carling_list에 있는 정보를 이용하여 첨부파일을 저장한다
        '''

        xlsx_down_cols = ["식별 코드", "SEQ", "제공처", "고유번호", "파일명", "복지_URL", "첨부_URL", "확장자", "경로"]

        if not os.path.exists(down_folder):
            os.makedirs(down_folder)

        total_dict = {col:[] for col in xlsx_down_cols}
        for down_dict in self.crawling_list_down:
            for col in xlsx_down_cols:
                total_dict[col].append(down_dict.get(col, None))

                if col=="첨부_URL":
                    request.urlretrieve(down_dict[col], down_folder+"/"+down_dict["SEQ"]+"."+down_dict["파일명"].split(".")[-1])

        df = pd.DataFrame(total_dict)
        df.to_excel(down_path,index=False)


    def kremap_move_back_year(self,browser):
        # 날짜설정
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_BusinessSdate"]').click)
        for i in range(11):  # 11년 전
            self.delay_after_func(0.5, browser.find_element(By.XPATH, '//*[@id="changeLeft"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="content"]/table/tbody/tr[2]/td[3]').click)  # 7월
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="Middle_ContentPlaceHolder2_complete"]').click)  # 조회클릭