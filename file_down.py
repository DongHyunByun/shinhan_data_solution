import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime,timedelta
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
    str_d = None
    d = None
    browser = None
    path = None

    def __init__(self,str_d,path):
        self.path = path
        self.str_d = str_d
        self.d = datetime.strptime(str_d, '%Y%m')

        self.make_d_dir()

        # # 20일자
        # self.filedown_1(str((self.d - timedelta(days=30*2)).year), str((self.d - timedelta(days=30*2)).month))
        # self.filedown_2_20(str((self.d - timedelta(days=30*3)).year),str((self.d - timedelta(days=30*3)).month))
        # self.filedown_21()
        # self.filedown_22()
        # self.filedown_23()
        # self.filedown_38()
        # self.filedown_42()
        # self.filedown_55(str((self.d - timedelta(days=30*2)).year),str((self.d - timedelta(days=30*2)).month))
        # self.filedown_56()
        # self.filedown_57()
        #
        # # 말일자
        # self.filedown_27()
        # self.filedown_28()
        # # self.filedown_29() # 해야함
        # self.filedown_34()
        # self.filedown_36()
        # self.filedown_39()
        # self.filedown_41()
        # self.filedown_43()
        # self.filedown_44()

    def make_d_dir(self):
        '''
        폴더가 없으면 폴더를 만든다

        path/YYYYMM
          I
          I___20일
              I
              I__원천
          I
          I___말일
              I
              I__원천
          I
          I___kb단지
              I
              I__원천
        '''
        if not os.path.isdir(self.path):
            os.mkdir(self.path)

        for folder in ["20일","말일","kb단지"]:
            if not os.path.isdir(f"{self.path}\\{folder}"):
                os.mkdir(f"{self.path}\\{folder}")

            if not os.path.isdir(f"{self.path}\\{folder}\\원천"):
                os.mkdir(f"{self.path}\\{folder}\\원천")

    def filedown_1(self,y,m):
        print("1")
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

    def filedown_2_20(self,y,m):
        print("2~20")
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

                request.urlretrieve(down_url,f"{folder_path}/{file_name}")

        # 시트나누기
        row_down_path = f"{folder_path}/{file_name}"
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

    def filedown_21(self):
        file_num = "21"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(3,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_1C8015&orgId=101&listId=J1_1&dbUser=NSI.&language=ko',))

        # 설정창 열기
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')

        # 다운로드
        self.kosis_download(browser)
        self.change_last_file(folder_path,file_num)

    def filedown_22(self):
        file_num = "22"
        print(file_num)
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
        self.change_last_file(folder_path, file_num)

    def filedown_23(self):
        file_num = "23"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
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
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 다운로드
        self.kosis_download(browser)
        self.change_last_file(folder_path, file_num)

    def filedown_27(self):
        file_num = "27"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
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
        self.change_last_file(folder_path, file_num)

    def filedown_28(self):
        file_num = "28"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_6906&orgId=116&listId=M1_6&dbUser=NSI.&language=ko',))

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
        self.change_last_file(folder_path, file_num)

    def filedown_34(self):
        file_num = "34"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_1946&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        # 행렬교체
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le3"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 시도별상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le4"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 다운로드
        self.kosis_download(browser)
        self.change_last_file(folder_path, file_num)

    def filedown_36(self):
        file_num = "36"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(15,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_1952&orgId=116&listId=116_11626_001&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri0"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le4"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 시도별상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Le2"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[2]/a[1]').click)  # 위쪽으로

        self.delay_after_func(5, browser.find_element(By.XPATH, '//*[@id="btn_definite"]').click)  # 적용

        # 조회설정
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_querySetting"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="tabClassText_4"]/span[@class="ui-accordion-header-icon ui-icon ui-icon-triangle-1-e"]').click) # 시도별 리스트 내리기

        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-5"]/li[2]/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]').click)  # 시도별 리스트 내리기
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="ft-id-5"]/li[6]/span[@class="fancytree-node fancytree-selected fancytree-exp-n fancytree-ico-c"]').click)  # 시도별 리스트 내리기
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnSearch"]').click)

        # 시점설정
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btn_time"]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="timePopListMBtn"]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="ft-id-7"]/li[1]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="btnTimeAccept"]').click)

        # 다운로드
        self.kosis_download(browser)
        self.change_last_file(folder_path, file_num)

    def filedown_38(self):
        file_num = "38"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=32&hFormId=5328&hDivEng=&month_yn=',))
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH, '//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        self.change_last_file(folder_path, file_num)

    def filedown_39(self):
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
        self.change_last_file(folder_path, file_num)

    def filedown_41(self):
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
        self.change_last_file(folder_path, file_num)

    def filedown_42(self):
        file_num = "42"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ('https://stat.molit.go.kr/portal/cate/statView.do?hRsId=419&hFormId=5882&hDivEng=&month_yn=',))
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="fileDownBtn"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH,'//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-body"]/ul[@class="mu-check-list horizontal"]/li[2]').click)
        self.delay_after_func(3, browser.find_element(By.XPATH,'//*[@id="file-download-modal"]/div[@class="mu-dialog"]/div[@class="mu-dialog-foot"]/button').click)

        self.change_last_file(folder_path, file_num)

    def filedown_43(self):
        file_num = "43"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_5373&orgId=116&listId=116_11626_003&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri2"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri3"]').click)
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
        self.change_last_file(folder_path, file_num)

    def filedown_44(self):
        file_num = "44"
        print(file_num)
        folder_path = f"{self.path}\\말일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(20,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_MLTM_5387&orgId=116&listId=116_11626_002&dbUser=NSI.&language=ko',))

        # 행렬교체(상자옮기기)
        browser.switch_to.frame('iframe_rightMenu')
        browser.switch_to.frame('iframe_centerMenu1')
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="ico_swap"]').click)

        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri1"]').click)  # 시점상자 클릭
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri2"]').click)
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="rEmpty"]/div[1]/a[1]').click)  # 왼쪽으로
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="Ri3"]').click)
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
        self.change_last_file(folder_path, file_num)

    def filedown_55(self, y, m):
        file_num = "55"
        print(file_num)
        folder_path = f"{self.path}/20일/원천"
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

                request.urlretrieve(down_url, f"{folder_path}/{file_num}.{file_type}")

    def filedown_56(self):
        file_num = "56"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10, browser.get, ("https://www.reb.or.kr/r-one/statistics/statisticsViewer.do?menuId=LFR_13200",))
        self.delay_after_func(1, browser.find_element(By.XPATH, '//*[@id="S_FileBox"]').click)

        self.change_last_file(folder_path, file_num)

    def filedown_57(self):
        file_num = "57"
        print(file_num)
        folder_path = f"{self.path}\\20일\\원천"
        browser = self.kosis_init_broswer(folder_path)
        self.delay_after_func(10,browser.get,('https://kosis.kr/statHtml/statHtml.do?vwCd=MT_ZTITLE&tblId=DT_151Y006&orgId=301&listId=S1_301006_003_006&dbUser=NSI.&language=ko',))

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
        self.change_last_file(folder_path, file_num)

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

    def change_last_file(self,folder_path, new_name):
        filename = max([folder_path + "\\" + f for f in os.listdir(folder_path)], key=os.path.getctime)
        file_type = filename.split(".")[-1]
        shutil.move(filename, os.path.join(folder_path, f"{new_name}.{file_type}"))