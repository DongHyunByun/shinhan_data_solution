import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import time
import pandas as pd
from urllib import request,parse
import os
import sys
import openpyxl
import csv
import xlrd
import shutil

# import asyncio
# from arsenic import get_session
# from arsenic.browsers import Chrome
# from arsenic.services import Chromedriver

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select


class FileDown:
    now_y = 0
    now_m = 0
    now_str_d = ""
    browser = None
    path = None

    def __init__(self,y,m, path):
        self.path = path
        self.now_y=y
        self.now_m=m
        self.now_str_d = str(self.now_y) + (str(self.now_m).zfill(2))

        # self.filedown_1(str(self.now_y), str(self.now_m-2))
        # self.filedown_2_20(str(self.now_y),str(self.now_m-3))
        # self.filedown_21()
        self.filedown_22()
        # self.filedown_23()
        # self.filedown_55(str(self.now_y),str(self.now_m-2))
        # self.filedown_57()

    def filedown_1(self,y,m):
        folder_path = f"{self.path}/20일/원천"
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

                request.urlretrieve(down_url,f"{folder_path}/1.{file_type}")
                print("1")

    def filedown_2_20(self,y,m):
        folder_path = f"{self.path}/20일/원천"
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
                        file_name = file["fileNm"]
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url,f"data/{self.now_str_d}/raw_data/{file_name}")

        # 시트나누기
        row_down_path = f"data/{self.now_str_d}/raw_data/{file_name}"
        pd.read_excel(row_down_path, sheet_name='매매_공동주택').to_csv(f"{folder_path}/2.csv",index=False,encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매_공동주택_계절조정').to_csv(f"{folder_path}/3.csv", index=False,encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매_아파트').to_csv(f"{folder_path}/4.csv", index=False,encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 전세_아파트').to_csv(f"{folder_path}/5.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='분기별_매매 증감률_아파트').to_csv(f"{folder_path}/6.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매_아파트').to_csv(f"{folder_path}/7.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='전세_아파트').to_csv(f"{folder_path}/8.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매 중위_아파트').to_csv(f"{folder_path}/9.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매 평균_아파트').to_csv(f"{folder_path}/10.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매 중위_아파트').to_csv(f"{folder_path}/11.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매 평균_아파트').to_csv(f"{folder_path}/12.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='전세 중위_아파트').to_csv(f"{folder_path}/13.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='전세 평균_아파트').to_csv(f"{folder_path}/14.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매_연립다세대').to_csv(f"{folder_path}/15.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매_연립다세대').to_csv(f"{folder_path}/16.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매 중위_연립 다세대').to_csv(f"{folder_path}/17.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='규모별 매매 평균_연립 다세대').to_csv(f"{folder_path}/18.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매 중위_연립 다세대').to_csv(f"{folder_path}/19.csv", index=False, encoding='cp949')
        pd.read_excel(row_down_path, sheet_name='매매 평균_연립 다세대').to_csv(f"{folder_path}/20.csv", index=False, encoding='cp949')
        print("2~20")

    def filedown_21(self):
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(3,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_1C8015&orgId=101&listId=J1_1&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        # 이름바꾸기
        self.change_last_file(folder_path,"21.xlsx")
        print(21)

    def filedown_22(self):
        print(22)
        folder_path = f"{self.path}\\20일\\원천"
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
        self.delay_after_func(1, Select(browser.find_element(By.ID, "fancytree_2CheckOption")).select_by_value, ("allLowLevel",))
        self.delay_after_func(1, browser.switch_to.alert.accept)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[1]/span/span[@class="fancytree-checkbox"]').click) # 전체
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ft-id-3"]/li[5]/span/span[@class="fancytree-checkbox"]').click) # 집세
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        self.change_last_file(folder_path, "22.xlsx")


    def filedown_23(self):
        browser = self.kosis_init_broswer()
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_404Y016&orgId=301&listId=P2_301002&dbUser=NSI.&language=ko',))

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
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)

    def filedown_38(self):
        #???
        pass

    def filedown_55(self, y, m):
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
                        down_key = file["dwldUrl"]
                        down_url = f"https://www.reb.or.kr/r-one/common/nttFileDownload.do?fileKey={down_key}"
                        break

                request.urlretrieve(down_url, f"data/{self.now_str_d}/raw_data/{file_name}")

    def filedown_57(self):
        browser = self.kosis_init_broswer()
        self.delay_after_func(10,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_151Y006&orgId=301&listId=S1_301006_003_006&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)

        # 계정코드별
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="tabClassText_1"]').click) #아래탭 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fancytree_1Btn"]').click) #전체해제
        self.delay_after_func(1,  browser.find_element(By.XPATH, '//*[@id="ft-id-2"]/li[@class="fancytree-lastsib"]/span/span[@class="fancytree-expander"]').click) #목록 보이기

        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/span/span[@class="fancytree-checkbox"]').click)  #맨위 체크박스 체크
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[1]/span/span[@class="fancytree-checkbox"]').click) #주택담보대출
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-2"]/li/ul/li[2]/span/span[@class="fancytree-checkbox"]').click)  # 주택담보대출

        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click) # 조회

        # 다운로드
        self.kosis_download(browser)

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

    def kosis_download(self,browser):
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_download"]').click)
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

    def change_last_file(self,folder_path, new_name):
        filename = max([folder_path + "\\" + f for f in os.listdir(folder_path)], key=os.path.getctime)
        shutil.move(filename, os.path.join(folder_path, new_name))