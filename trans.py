import pandas as pd
from tabulate import tabulate as tb
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import os
from bs4 import BeautifulSoup as bs

class Trans:
    data_path = None
    path = None
    d = None
    today = datetime.now().strftime('%Y.%m월')
    refer = "refer"

    def __init__(self,data_path,d):
        self.data_path = data_path
        self.path = f"{data_path}\\{d}"
        self.refer_path = f"{data_path}\\refer"
        self.d = d

        self.make_d_dir()

        # 20일자
        # self.trans_1()
        # self.trans_2_20() # 15~20까지 추가예정
        # self.trans_21()
        # self.trans_22()
        # self.trans_23()
        # self.trans_38()
        # self.trans_42()
        # self.trans_55()
        # self.trans_56()
        # self.trans_57()

        # 말일자
        # self.trans_27()
        # self.trans_29()
        # self.trans_39()
        # self.trans_41()
        # self.trans_45_49()
        # 34, 36, 43, 44, 51, 53, 54는 코드없음(잘 안틀림)

    def make_d_dir(self):
        '''
        폴더가 없으면 폴더를 만든다 path/YYYYMM/n일/원천_처리후
          I
          I--20일
             I
             I--원천_처리후
          I
          I--말일
             I
             I--원천_처리후
          I
          I--kb단지
             I
             I--원천
        '''
        for folder in ["20일","말일","kb단지"]:
            if not os.path.isdir(f"{self.path}\\{folder}\\원천_처리후"):
                os.mkdir(f"{self.path}\\{folder}\\원천_처리후")

    def trans_1(self):
        file_path1 = f"{self.path}/20일/원천/1.xls"
        file_path3 = f"{self.path}/20일/원천_처리후"

        jiga = pd.read_excel(file_path1, header=3, dtype='str')

        # 필요한 컬럼만 추출
        jiga = jiga.iloc[1:, [0, 1, 12, 13, 14, 15, 16, 17, 18]]
        jiga.columns = ['CODE', '행정구역', '전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타']

        jiga.dropna(subset=['행정구역'], how='all', inplace=True)
        jiga.dropna(subset=['전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타'], how='all', inplace=True)

        jiga['행정구역'] = jiga['행정구역'].apply(lambda x: re.sub('[\W\d]', '', x))
        sido_list = ['전국', '서울특별시', '인천광역시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시',
                     '세종특별자치시', '경기도', '강원도', '충청북도', '충청남도', '전라북도', '전라남도', '경상북도', '경상남도', '제주자치도']
        jiga['시도'] = [sido if sido in sido_list else np.nan for sido in jiga['행정구역']]
        jiga['시도'].fillna(method='ffill', inplace=True)

        def del_nm(x):
            for item in sido_list:
                x = re.sub(item, '', x)
            return x

        jiga['행정구역'] = jiga['행정구역'].apply(lambda x: del_nm(x))
        jiga['시도시군구'] = jiga['시도'] + jiga['행정구역']
        jiga = jiga.loc[:, ['시도시군구', '전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타']]
        jiga.columns = ['시도시군구', '전', '답', '주거용(대)', '상업용(대)', '임야', '공장용지', '기타']

        jiga.fillna('', inplace=True)
        jiga.replace('-', '9999', inplace=True)

        # 형태에 맞춰주기 위해 Transpose 하기
        jiga.set_index('시도시군구', drop=True, inplace=True)
        jiga = jiga.stack()
        jiga = pd.DataFrame(jiga.reset_index())

        jiga.columns = ['시도시군구', '이용상황구분명', '값']

        # 코드값 불러와서 붙이기
        sido_df = pd.read_csv(f"{self.refer_path}/55_이용상황별 지가지수_시도시군구.dat",sep='|', encoding='ANSI')
        gubun = pd.read_csv(f"{self.refer_path}/1_이용상황별 지가변동률_구분명.dat",sep='|', dtype='str', encoding='ANSI')

        jiga = pd.merge(sido_df, jiga, how='left', on='시도시군구')
        jiga = pd.merge(jiga, gubun, how='left', on='이용상황구분명')

        # 필요한 컬럼만 추출
        jiga = jiga.loc[:, ['시군구CODE', '시군구명', '시도시군구', '이용상황구분', '이용상황구분명', '값']]
        # 정렬
        jiga.sort_values(['시군구CODE', '이용상황구분'], inplace=True)

        jiga['값'].replace('-', '', inplace=True)
        jiga.drop_duplicates(inplace=True)

        jiga.to_csv(f'{file_path3}/1.rtp_usecase_jg_yyyymmdd.dat', sep='|', index=False, encoding='ANSI')

    def trans_2_20(self):
        # 파일 경로 설정
        file_path1 = f"{self.path}/20일/원천/2-20.xlsm"
        file_path3 = f"{self.path}/20일/원천_처리후"

        no_list = [2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        sheets = [['매매_공동주택', '매매 증감률_공동주택'], '매매_공동주택_계절조정', '규모별 매매_아파트', '규모별 전세_아파트', ['매매_아파트', '매매 증감률_아파트'],
                  '전세_아파트', '규모별 매매 중위_아파트', '규모별 매매 평균_아파트', '매매 중위_아파트', '매매 평균_아파트', '전세 중위_아파트', '전세 평균_아파트',
                  '규모별 매매_연립다세대', '매매_연립다세대', '규모별 매매 중위_연립 다세대', '규모별 매매 평균_연립 다세대', '매매 중위_연립 다세대', '매매 평균_연립 다세대']

        # for i in range(len(no_list)):
        #     print(no_list[i], ' : ', sheets[i])

        jibang = pd.read_csv(f'{self.refer_path}/지방도.dat', dtype='str', sep='|', encoding='ANSI')

        # 2번
        print(2, ' : ', ['매매_공동주택', '매매 증감률_공동주택'])
        df1 = pd.read_excel(file_path1, dtype='str', header=1, sheet_name=sheets[0][0], engine='openpyxl')
        df1.columns = [re.sub('[^가-힣]', '', col) for col in df1.columns]
        df1 = df1.set_index('지역구분년월').stack(level=0).reset_index()
        df1.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df1['자료발표일자'] = df1['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df1['자료기준년월'] = '201711'

        df2 = pd.read_excel(file_path1, dtype='str', header=1, sheet_name=sheets[0][1], engine='openpyxl')
        df2.columns = [re.sub('[^가-힣]', '', col) for col in df2.columns]
        df2 = pd.DataFrame(df2.iloc[-1, :]).transpose()
        df2 = df2.set_index('지역구분년월').stack(level=0).reset_index()
        df2['지역구분년월'] = df2['지역구분년월'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m(잠정)') + relativedelta(months=1)).strftime('%Y%m%d'))
        df2.columns = ['자료발표일자', 'KED시도구분명', '잠정증감율']
        df2['자료기준년월'] = df2['자료발표일자'].apply(lambda x: x[:-2])

        df = pd.concat([df1, df2], ignore_index=True)
        df.fillna('0', inplace=True)
        df = df.merge(jibang, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '실거래가격지수값', '잠정증감율', '자료기준년월']].sort_values(by = ['자료발표일자', 'cd'], ascending = [False, True])

        df.to_csv(f'{file_path3}/2.rtp_gdhse_t_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 3번
        print(3, ' : ', '매매_공동주택_계절조정')
        df = pd.read_excel(file_path1, header=1, sheet_name=sheets[1], engine='openpyxl')
        df.columns = [re.sub('[^가-힣]', '', col) for col in df.columns]
        df = df.set_index('지역구분년월').stack(level=0).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(lambda x: (x + relativedelta(months=1)).strftime('%Y%m%d'))
        df['자료기준년월'] = '201711'
        df = df.merge(jibang, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '실거래가격지수값', '자료기준년월']].sort_values(by=['자료발표일자', 'cd'],
                                                                                ascending=[False, True])

        scale_cd = pd.read_csv(f'{self.refer_path}/규모시군구.dat', sep='|', dtype='str', encoding='ANSI')
        df.to_csv(f'{file_path3}/3.rtp_gdhse_sea_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 4번 데이터
        print(4, ' : ', '규모별 매매_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[2], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '규모', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df = df.merge(scale_cd, how='inner', left_on='KED시도구분명', right_on='key')
        df = df.merge(scale_cd, how='inner', left_on='규모', right_on='key')
        df['지수기준년월'] = '201711'
        df = df[['자료발표일자', 'cd_x', 'KED시도구분명', 'cd_y', 'cdnm_y', '실거래가격지수값', '지수기준년월']]
        df.sort_values(by=['자료발표일자', 'cd_x', 'cd_y'], ascending=[False, True, True], inplace=True)

        df.to_csv(f'{file_path3}/4.rtp_sz_apt_t_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 5번 데이터
        print(5, ' : ', sheets[3])
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[3], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '규모', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m%d'))
        df = df.merge(scale_cd, how='inner', left_on='KED시도구분명', right_on='key')
        df = df.merge(scale_cd, how='inner', left_on='규모', right_on='key')
        df['지수기준년월'] = '201711'
        df = df[['자료발표일자', 'cd_x', 'KED시도구분명', 'cd_y', 'cdnm_y', '실거래가격지수값', '지수기준년월']]
        df.sort_values(by=['자료발표일자', 'cd_x', 'cd_y'], ascending=[False, True, True], inplace=True)
        df.to_csv(f'{file_path3}/5.rtp_sz_apt_js_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        sido_cd = pd.read_csv(f'{self.refer_path}/시도.dat', sep='|', dtype='str', encoding='ANSI')

        # 7번 데이터
        print(7, ' : ', sheets[4])
        df1 = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[4][0], engine='openpyxl')
        dic = {}
        for i in df1.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df1.rename(columns=dic, inplace=True)
        strftime = []
        for item in df1.iloc[:, 0]:
            try:
                strftime.append(
                    (datetime.strptime(item, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m01'))
            except:
                strftime.append(np.nan)
        df1.iloc[:, 0] = strftime
        df1.set_index('지역구분년월', inplace=True)
        df1 = df1.stack(level=[0, 1]).reset_index()
        df1.dropna(subset=['지역구분년월'], inplace=True)
        df1.loc[df1['level_2'] == '', 'level_2'] = df1.loc[df1['level_2'] == '', 'level_1']
        df1 = df1[['지역구분년월', 'level_2', 0]]
        df1.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df1['지수기준년월'] = '201711'

        df2 = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[4][1],engine='openpyxl')
        df2 = pd.DataFrame(df2.iloc[-1, :]).transpose()
        dic = {}
        for i in df2.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df2.rename(columns=dic, inplace=True)
        df2 = df2.set_index('지역구분년월').stack(level=[0, 1]).reset_index()
        df2.loc[df2['level_2'] == '', 'level_2'] = df2.loc[df2['level_2'] == '', 'level_1']
        df2 = df2[['지역구분년월', 'level_2', 0]]
        df2.columns = ['자료발표일자', 'KED시도구분명', '잠정증감율']
        df2['자료발표일자'] = df2['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m(잠정)') + relativedelta(months=1)).strftime('%Y%m%d'))
        df2['지수기준년월'] = df2['자료발표일자'].apply(lambda x: x[:6])

        df = pd.concat([df1, df2], ignore_index=True)
        df.fillna('', inplace=True)
        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '실거래가격지수값', '잠정증감율', '지수기준년월']].sort_values(by=['자료발표일자', 'cd'],
                                                                                         ascending=[False, True])
        df.to_csv(f'{file_path3}/7.rtp_apt_t_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 8번 데이터
        print(8," : ",'전세_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[5], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        strftime = []
        for item in df.iloc[:, 0]:
            try:
                strftime.append(
                    (datetime.strptime(item, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m01'))
            except:
                strftime.append(np.nan)
        df.iloc[:, 0] = strftime
        df.set_index('지역구분년월', inplace=True)
        df = df.stack(level=[0, 1]).reset_index()
        df.dropna(subset=['지역구분년월'], inplace=True)
        df.loc[df['level_2'] == '', 'level_2'] = df.loc[df['level_2'] == '', 'level_1']
        df = df[['지역구분년월', 'level_2', 0]]
        df.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df['지수기준년월'] = '201711'
        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '실거래가격지수값', '지수기준년월']].sort_values(by=['자료발표일자', 'cd'], ascending=[False, True])
        df.to_csv(f'{file_path3}/8.rtp_apt_js_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 9번 데이터
        print(9," : ",'규모별 매매 중위_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[6], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '규모', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df = df.merge(scale_cd, how='inner', left_on='KED시도구분명', right_on='key')
        df = df.merge(scale_cd, how='inner', left_on='규모', right_on='key')
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])
        df = df[['자료발표일자', 'cd_x', 'KED시도구분명', 'cd_y', 'cdnm_y', '실거래가격지수값', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=2)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd_x', 'cd_y'], ascending=[False, True, True], inplace=True)

        df.to_csv(f'{file_path3}/9.rtp_apt_sz_mid_yyyymmdd.dat', sep = '|', encoding = 'ANSI', index = False)

        # 10번 데이터
        print(10," : ",'규모별 매매 평균_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[7], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '규모', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df = df.merge(scale_cd, how='inner', left_on='KED시도구분명', right_on='key')
        df = df.merge(scale_cd, how='inner', left_on='규모', right_on='key')
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])
        df = df[['자료발표일자', 'cd_x', 'KED시도구분명', 'cd_y', 'cdnm_y', '실거래가격지수값', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=2)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd_x', 'cd_y'], ascending=[False, True, True], inplace=True)

        df.to_csv(f'{file_path3}/10.rtp_apt_sz_avg_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 11번 데이터
        print(11, " : ",'매매 중위_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[8], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.loc[df['level_2'] == '', 'level_2'] = df.loc[df['level_2'] == '', 'level_1']

        df = df.iloc[:, [0, 2, 3]]
        df.columns = ['자료발표일자', 'KED시도구분명', '가격']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])

        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '가격', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=2)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd'], ascending=[False, True], inplace=True)

        df.to_csv(f'{file_path3}/11.rtp_apt_t_mid_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 12번 데이터
        print(12," : ",'매매 평균_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[9], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.loc[df['level_2'] == '', 'level_2'] = df.loc[df['level_2'] == '', 'level_1']

        df = df.iloc[:, [0, 2, 3]]
        df.columns = ['자료발표일자', 'KED시도구분명', '가격']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])
        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '가격', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=2)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd'], ascending=[False, True], inplace=True)

        df.to_csv(f'{file_path3}/12.rtp_apt_t_avg_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 13번 데이터
        print(13," : ",'전세 중위_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[10], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.loc[df['level_2'] == '', 'level_2'] = df.loc[df['level_2'] == '', 'level_1']

        df = df.iloc[:, [0, 2, 3]]
        df.columns = ['자료발표일자', 'KED시도구분명', '가격']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m%d'))
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])
        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '가격', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=3)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd'], ascending=[False, True], inplace=True)

        df.to_csv(f'{file_path3}/13.rtp_apt_js_mid_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 14번 데이터
        print(14," : ",'전세 평균_아파트')
        df = pd.read_excel(file_path1, dtype='str', header=[1, 2], sheet_name=sheets[11], engine='openpyxl')
        dic = {}
        for i in df.columns:
            for j in i:
                dic[j] = re.sub('[^가-힣]', '', j)

        df.rename(columns=dic, inplace=True)
        df = df.set_index(df.columns[0]).stack(level=[0, 1]).reset_index()
        df.loc[df['level_2'] == '', 'level_2'] = df.loc[df['level_2'] == '', 'level_1']

        df = df.iloc[:, [0, 2, 3]]
        df.columns = ['자료발표일자', 'KED시도구분명', '가격']
        df['자료발표일자'] = df['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m%d'))
        df['지수기준년월'] = df['자료발표일자'].apply(lambda x: x[:6])
        df = df.merge(sido_cd, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '가격', '지수기준년월']]
        df = df.loc[df['자료발표일자'] == (datetime.now() - relativedelta(months=3)).strftime('%Y%m01'), :]
        df.sort_values(by=['자료발표일자', 'cd'], ascending=[False, True], inplace=True)

        df.to_csv(f'{file_path3}/14.rtp_apt_js_avg_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

    def trans_21(self):
        file_name1 = f"{self.path}/20일/원천/21.xlsx"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        df = pd.read_excel(file_name1, dtype='str', engine='openpyxl', sheet_name='데이터')
        df.columns = [re.sub('[ p)]', '', x) for x in df.columns]
        df.set_index('지수별', inplace=True)
        df = df.T

        # 자료발표일자 (월 + 1) 만들어주기
        yyyymm_list = list(df.index)
        yyyymm_list = [(datetime.strptime(x, '%Y.%m') + relativedelta(months=1)) for x in yyyymm_list]
        yyyymm_list = [x.strftime('%Y%m%d') for x in yyyymm_list]
        df.insert(0, '자료발표일자', yyyymm_list)

        # 지수 기준일이 바뀌면 변경 필수 !!
        df['자료기준년월'] = '202012'

        kospi = [i for i in range(len(df.columns)) if '코스피' in df.columns[i]]
        if len(kospi) == 0:
            print(tb(df, headers='keys', tablefmt='pretty'))
            print(df.shape)
        else:
            df.drop([df.columns[i] for i in kospi], axis=1, inplace=True)
            print(tb(df, headers='keys', tablefmt='pretty'))
            print(df.shape)

        df.to_csv(f"{file_path2}/21.rtp_cei_inf_yyyymmdd.dat",sep='|', index=False, encoding='ANSI')

    def trans_22(self):
        pd.set_option('display.float_format', '{:, %g}'.format)

        file_path1 = f"{self.path}/20일/원천/22.xlsx"
        file_path2 = f"{self.path}/20일/원천_처리후"

        # 원천 파일 불러오기
        df = pd.read_excel(file_path1, sheet_name='데이터', engine='openpyxl')
        # NA값 제거 및 공백 제거
        df['시도별'].fillna(method='ffill', inplace=True)
        df['시도별'] = df['시도별'].apply(lambda x: re.sub('[\W]', '', x))
        df['품목별'] = df['품목별'].apply(lambda x: re.sub('[\W]', '', x))

        print(list(df.columns))
        col_list = list(df.columns)[:2]
        yyyymm = input('사용할 컬럼명을 작성해주세요((원천=DB-1)  ex)2022.05  : ')
        col_list += [yyyymm]
        yyyymm = datetime.strptime(yyyymm, '%Y.%m') + relativedelta(months=1)
        yyyymm = yyyymm.strftime('%Y%m%d')

        df = df.loc[:, col_list]

        out = pd.read_csv(f'{self.refer_path}/22_품목별 소비자물가지수_구분명.dat', dtype='str', sep='|', header=None, encoding='ANSI')

        out = pd.merge(out, df, how='left', left_on=[1, 4], right_on=['시도별', '품목별'])
        out.drop(['시도별', '품목별'], axis=1, inplace=True)
        out = pd.merge(out, df, how='left', left_on=[1, 5], right_on=['시도별', '품목별'])
        out.drop(['시도별', '품목별'], axis=1, inplace=True)
        out = pd.merge(out, df, how='left', left_on=[1, 3], right_on=['시도별', '품목별'])
        out.drop(['시도별', '품목별'], axis=1, inplace=True)
        out.drop([4, 5], axis=1, inplace=True)

        out.insert(0, '자료발표일자', yyyymm)
        # 지수 기준년월 수정 시 수정 필수
        out['자료기준년월'] = '202012'
        print(tb(out, headers='keys', tablefmt='pretty'))

        out.to_csv(f"{file_path2}/22.rtp_item_cpi1_inf_yyyymm.dat", index=False, sep='|', header=None, encoding='ANSI')

    def trans_23(self):
        '''
        파일 형태 참고 ( 전체 월 데이터 불러오는걸 추천 )
        계정코드별                   2021.11	2021.12
        총지수 (2015=100)	        113.23	113.21
        비주거용건물임대 (2015=100)	103.66	103.66
        비주거용부동산관리 (2015=100)	108.29	108.43
        '''
        file_path1 = f"{self.path}/20일/원천/23.xlsx"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        df = pd.read_excel(file_path1, sheet_name='데이터')
        df.set_index('계정코드별', inplace=True)
        df = df.T

        df.columns = [re.sub('[^가-힣]', '', i) for i in df.columns]

        yyyymm_list = list(df.index)
        yyyymm_list = [(datetime.strptime(x, '%Y.%m') + relativedelta(months=1)) for x in yyyymm_list]
        yyyymm_list = [x.strftime('%Y%m%d') for x in yyyymm_list]
        df.insert(0, '자료발표일자', yyyymm_list)
        df.sort_values(by='자료발표일자', ascending=False, inplace=True)

        df_tp = df.loc[:, ['비주거용건물임대', '비주거용부동산관리']].stack()
        df_tp = df_tp.reset_index()
        df_tp = df_tp.set_index('level_0')

        yyyymm_list = list(df_tp.index)
        yyyymm_list = [(datetime.strptime(x, '%Y.%m') + relativedelta(months=1)) for x in yyyymm_list]
        yyyymm_list = [x.strftime('%Y%m%d') for x in yyyymm_list]
        df_tp.insert(0, '자료발표일자', yyyymm_list)

        df = pd.merge(df.loc[:, ['자료발표일자', '총지수']], df_tp, how='left', on='자료발표일자')
        df = df.loc[:, ['자료발표일자', 'level_1', '총지수', 0]]

        code = [0 if '건물임대' in x else 1 for x in df['level_1']]
        df.insert(1, '비주거용건물구분', code)

        # *** 지수 기준일 수정시 수정 필수 ***
        df['자료기준년월'] = '201512'
        print(tb(df, headers='keys', tablefmt='pretty'))

        df.to_csv(file_path2 + '23.rtp_item_ppi_inf_yyyymmdd.dat',sep='|', index=False, encoding='ANSI')

    def trans_27(self):
        file_path1 = f"{self.path}/말일/원천/27.xlsx"
        file_path2 = f"{self.path}/말일/원천_처리후/"

        df = pd.read_excel(file_path1, dtype='str',sheet_name='데이터', engine='openpyxl')
        df.fillna(method='ffill', inplace=True)

        dic = {'조적': '조적조','교육및사회용': '교육및사회'}
        df.replace(dic, inplace=True)
        df.columns = ['필요없음', '레벨01(1)', '레벨02(1)', '항목'] + list(df.columns)[4:]

        df = df.loc[:,['필요없음', '레벨01(1)', '레벨02(1)', '항목'] + [(datetime.now() - relativedelta(months=2)).strftime('%Y.%m')]]
        df.columns = ['필요없음', '레벨01(1)', '레벨02(1)', '항목', '값']
        df.insert(1, '레벨', [item1 + '_' + item2 for item1, item2 in zip(df['레벨01(1)'], df['레벨02(1)'])])
        df.insert(0, '날짜', (datetime.now() - relativedelta(months=1)).strftime('%Y%m01'))
        df['기준년월'] = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')

        df = df[['날짜', '레벨', '항목', '값', '기준년월']]

        cd1 = pd.read_csv(f'{self.refer_path}/27_level.dat', sep='|', dtype='str', header=None, encoding='ANSI')
        cd1.columns = ['code1', '레벨']
        cd2 = pd.read_csv(f'{self.refer_path}/27_level2.dat', sep='|', dtype='str', header=None, encoding='ANSI')
        cd2.columns = ['code2', '항목']

        df = pd.merge(df, cd1, how='left', on='레벨')
        df = pd.merge(df, cd2, how='left', on='항목')
        df = df[['날짜', 'code1', 'code2', '레벨', '항목', '값', '기준년월']]
        print(tb(df, headers='keys', tablefmt='pretty'))

        df.to_csv(f'{file_path2}/27.rtp_d_alsqr_st_yyyymm.dat', sep='|', index=False, header=None, encoding='ANSI')

    def trans_29(self):
        to_path = f"{self.path}/말일/원천_처리후/"

        file_path0 = f'{self.path}/말일/원천/29_콘크리트.xls'
        file_path1 = f'{self.path}/말일/원천/29_철골.xls'
        file_path2 = f'{self.path}/말일/원천/29_조적.xls'
        file_path3 = f'{self.path}/말일/원천/29_철골철근콘크리트.xls'
        file_path4 = f'{self.path}/말일/원천/29_목조.xls'
        file_path5 = f'{self.path}/말일/원천/29_기타.xls'

        gubun = pd.read_csv(f'{self.refer_path}/29_구분코드.dat', sep='|', dtype='str', encoding='ANSI')
        level = pd.read_csv(f'{self.refer_path}/29_레벨코드.dat', sep='|', dtype='str', encoding='ANSI')
        sido = pd.read_csv(f'{self.refer_path}/29_시도코드.dat', sep='|', dtype='str', encoding='ANSI')
        yong_d = pd.read_csv(f'{self.refer_path}/29_용도상세코드.dat', sep='|', dtype='str', encoding='ANSI')
        yong = pd.read_csv(f'{self.refer_path}/29_용도코드.dat', sep='|', dtype='str', encoding='ANSI')
        hang = pd.read_csv(f'{self.refer_path}/29_항목코드.dat', sep='|', dtype='str', encoding='ANSI')

        def xls_to_df(file, n):
            text = open(file, 'r').read()
            bs_text = bs(text, 'html.parser')
            bs_text
            dic = {}
            col_list = []
            cnt = 0
            # bs_text.find_all('row') : .xls의 각 ROW
            for i in range(len(bs_text.find_all('row'))):
                row = bs_text.find_all('row')[i]

                # row.find_all('data') : 각 row의 데이터 값
                data = row.find_all('data')

                # row에 데이터가 n개 이하인 경우는 필요없는 값
                if len(data) < n:
                    pass
                else:
                    # 첫 로우는 HEADER여서 따로 리스트로 저장 & dic 키로 저장
                    if cnt == 0:
                        for j in range(len(data)):
                            col_list.append(data[j].text)
                            dic[data[j].text] = []
                    # 나머지 로우는 dic 안에 값으로 저장
                    else:
                        for k in range(len(data)):
                            dic[col_list[k]].append(data[k].text)
                    cnt += 1

            df = pd.DataFrame(dic)
            return df

        # 파일 모두 병합
        df = pd.concat([xls_to_df(file_path0, 7), xls_to_df(file_path1, 7), xls_to_df(file_path2, 7), xls_to_df(file_path3, 7),xls_to_df(file_path4, 7), xls_to_df(file_path5, 7)])

        # 코드값 조인
        df = df.merge(gubun, how='left').merge(level, how='left').merge(sido, how='left').merge(yong_d,how='left').merge(yong,how='left').merge(hang, how='left')

        # 작업월에 필요한 컬럼 값만 사용, 자료기준년월이 다르면 에러
        yyyymm = (datetime.now() - relativedelta(months=2)).strftime('%Y.%m 월')

        # 필요한 컬럼만 사용
        df_fin = df.loc[:,['시도코드', '시도명', '항목코드', '항목명', '용도코드', '용도상세코드', '용도명', '용도상세명', '구분코드', '구분', '레벨코드', '레벨01', yyyymm]]
        df_fin.insert(0, '자료기준년월', (datetime.now() - relativedelta(months=1)).strftime('%Y%m01'))
        df_fin['지수기준년월'] = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')

        # 코드 값 매핑이 안된 자료 제거
        df_fin.dropna(inplace=True)

        # 자료 저장
        df_fin.to_csv(f'{to_path}/29.rtp_sido_st_yyyymmdd.dat', sep='|', index=False, encoding='ANSI')

    def trans_34(self):
        file_path1 = f"{self.path}/말일/원천/29.csv"
        file_path2 = f"{self.path}/말일/원천_처리후/"

        df = pd.read_csv(file_path1, header=[0, 1], index_col=[0, 1, 2, 3, 4], dtype='str', encoding='ANSI')
        df.columns.names = ['월', '월2']
        df = df.stack(level=[0, 1]).reset_index()

        df.to_csv(file_path2 + 'sido.dat', sep='|', index=False, encoding='ANSI')

    def trans_38(self):
        file_path = f"{self.path}/20일/원천/38.csv"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        unsold = pd.read_csv(file_path, encoding='cp949')
        unsold.fillna('0', inplace=True)
        # 필요한 컬럼만 뽑기
        unsold = unsold.loc[unsold['규모'].apply(lambda x: x not in ('계', '소계')), ['구분', '시군구', '부문', '규모', '호', '월(Monthly)']]

        # 합계, 계 공백으로 변경
        unsold.replace({'합계': '', '계': ''}, inplace=True)

        # 필요한 컬럼으로 가공
        unsold['시도시군구'] = unsold['구분'] + unsold['시군구']
        unsold['부문_규모'] = unsold['부문'] + '_' + unsold['규모']

        # 기준년월 형식에 맞추기
        unsold['월'] = unsold['월(Monthly)'].apply(lambda x: datetime.strptime(x, '%Y-%m') + relativedelta(months=1))
        unsold['자료발표일자'] = unsold['월'].apply(lambda x: x.strftime('%Y%m%d'))
        unsold['자료기준년월'] = unsold['월'].apply(lambda x: x.strftime('%Y%m'))

        unsold = unsold.loc[:, ['자료발표일자', '시도시군구', '부문_규모', '호', '자료기준년월']]

        # 이번달 제공해야할 기준월 자료만 추출
        today = self.d
        today = datetime.strptime(today, '%Y%m') - relativedelta(months=2)
        today = today.strftime('%Y%m')
        print(today)
        unsold = unsold.loc[unsold['자료기준년월'] == today, :]

        # 코드값 붙일 파일 불러오기
        sido = pd.read_csv(f'{self.refer_path}/38_공사완료후_미분양현황_시도시군구.dat', sep='|', encoding='ANSI')
        scale = pd.read_csv(f'{self.refer_path}/38_공사완료후_미분양현황_부문규모.dat', sep='|', encoding='ANSI')

        unsold = pd.merge(sido, unsold, how='left', on='시도시군구')
        unsold = pd.merge(unsold, scale, how='left', on='부문_규모')

        unsold = unsold.loc[:, ['자료발표일자', '시군구CODE', '시군구명', 'CODE', '부문_규모', '호', '자료기준년월']]
        unsold.dropna(axis=0, subset=['부문_규모'], inplace=True)

        print(tb(unsold.head(10), headers='keys', tablefmt='pretty'))
        print(tb(unsold.tail(10), headers='keys', tablefmt='pretty'))

        unsold.to_csv(file_path2 + '38.rtp_gsat_us_' + unsold['자료발표일자'][0] + '.csv',sep='|', header=False, index=False, encoding='ANSI')

    def trans_39(self):
        file_path1 = f"{self.path}/말일/원천/39.csv"
        file_path3 = f"{self.path}/말일/원천_처리후/"

        # 원천 파일 불러오기
        unsold = pd.read_csv(file_path1, encoding='ANSI')

        # 사용할 월 컬럼명 입력
        print([i for i in unsold.iloc[:, 0].drop_duplicates()])
        unsold = unsold.loc[unsold.iloc[:, 0] == input('사용할 월을 입력해주세요 ex) 2022-02 : '), :]

        # 컬럼의 월(Month)를 월로 바꿔주기
        unsold.columns = [re.sub(r'\([^)]*\)', '', i) for i in unsold.columns]
        # 부문과 규모를 합친 값 만들기
        unsold['부문_규모'] = unsold['부문'] + '_' + unsold['규모']
        # 필요한 컬럼만 뽑아내기
        unsold = unsold.loc[unsold['규모'].apply(lambda x: x not in ('총합', '소계')), ['월', '시도', '부문_규모', '호']]

        # 부문_규모 데이터 만들기
        scale = {'부문_규모': ['민간부문_40∼60㎡', '민간부문_60∼85㎡', '민간부문_85㎡초과', '민간부문_40㎡이하', '공공부문_공공부문'],
                 '부문_규모_코드': ['2', '3', '4', '6', '7']
                 }
        scale = pd.DataFrame(scale)

        # 시도 코드 데이터 불러오기
        sido = pd.read_csv(f'{self.refer_path}/규모별_미분양현황_Ked.dat',
                           sep='|', encoding='ANSI', header=None, names=['시도', '시도_코드'])

        # 부문_규모 및 시도_코드 합치기
        unsold = pd.merge(unsold, sido, how='left', on='시도')
        unsold = pd.merge(unsold, scale, how='left', on='부문_규모')
        unsold['월'] = unsold['월'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m%d'))
        unsold = unsold[['월', '시도_코드', '시도', '부문_규모_코드', '부문_규모', '호']]
        unsold['기준년월'] = unsold['월'].apply(lambda x: x[:-2])
        print(tb(unsold.head(), headers='keys', tablefmt='pretty'))

        unsold.to_csv(file_path3 + '39.rtp_sz_us_yyyymmdd.dat',
                      index=False, sep='|', header=None, encoding='ANSI')

    def trans_41(self):
        # 파일 경로 설정
        file_path1 = f"{self.path}/말일/원천/41.csv"
        file_path3 = f"{self.path}/말일/원천_처리후"

        unsold = pd.read_csv(file_path1,dtype='str', encoding='ANSI')

        yyyymm = datetime.now() - relativedelta(months=2)  # 변경 필수
        yyyymm_bf = yyyymm.strftime('%Y-%m')

        unsold = unsold.loc[unsold.iloc[:, 0] == yyyymm_bf, :]
        unsold['시군구'].replace({'계': '', '세종시': ''}, inplace=True)

        unsold.columns = ['월', '구분', '시군구', '호']

        unsold['시도시군구'] = unsold['구분'] + unsold['시군구']
        unsold = unsold.loc[:, ['시도시군구', '호']]

        sido = pd.read_csv(f'{self.refer_path}/시군구별 미분양현황_sido.dat', sep='|', dtype='str', encoding='ANSI')
        sido.fillna('', inplace=True)
        sido['시도시군구'] = sido['시도'] + sido['시군구']
        sido = sido.loc[:, ['시군구CODE', '시군구명', '시도시군구']]

        unsold = pd.merge(sido, unsold, how='left', on='시도시군구')
        yyyymm_af = yyyymm + relativedelta(months=1)
        unsold['자료발표일자'] = yyyymm_af.strftime('%Y%m01')
        unsold['자료기준년월'] = yyyymm_af.strftime('%Y%m')

        unsold = unsold.loc[:, ['자료발표일자', '시군구CODE', '시군구명', '호', '자료기준년월']]
        unsold.drop_duplicates(inplace=True)

        print(tb(unsold, headers='keys', tablefmt='pretty'))

        unsold.to_csv(f'{file_path3}/41.rtp_sigungu_us_' + str(datetime.now().strftime('%Y%m')) + '.dat',  sep='|', index=False, encoding='ANSI')

    def trans_42(self):
        file_path1 = f"{self.path}/20일/원천/42.csv"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        # 원천 파일 불러오기
        df = pd.read_csv(file_path1, dtype='str', encoding='cp949')
        df = df.set_index(['월(Monthly)', '구분']).stack().reset_index()

        # 필요한 년월에 해당하는 파일 불러오기
        now = datetime.now()
        now = now - relativedelta(months=2)
        now = now.strftime('%Y-%m')
        df = df.loc[df['월(Monthly)'] == now, :]

        # 시도 값 수정
        df['구분'] = df['구분'].apply(lambda x: re.sub('특별자치도|특별자치시|특별시|광역시|도|청|라|상', '', x))

        # 코드값 파일 불러오기
        code = pd.read_csv(f"{self.refer_path}/42_공동주택현황_코드.dat", dtype='str', sep='|', encoding='ANSI')

        # 코드값에 붙여넣어서 작업파일과 유사하게 맞춰주기
        df = pd.merge(code, df, how='left', left_on=['시도명', '단지동호수구분명'], right_on=['구분', 'level_2'])

        # 자료월, 자료기준년월 맞춰주기
        df = df.loc[:, ['월(Monthly)', 'KED분류시도구분', '시도명', '단지동호수구분', '단지동호수구분명', 0]]
        df['자료기준년월'] = df['월(Monthly)'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m'))
        df['월(Monthly)'] = df['월(Monthly)'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m') + relativedelta(months=1)).strftime('%Y%m%d'))

        print(tb(df.head(10), headers='keys', tablefmt='pretty'))

        # 파일 저장
        df.to_csv(f"{file_path2}/42.rtp_gdhse_now_yyyymmdd.dat", sep='|', header=None, index=False, encoding='ANSI')

    def trans_45_49(self):
        '''
        ************************************
        파일명 수정 필수 ex)부동산시장소비심리지수, 주택매매시장소비심리지수
        ************************************
        '''

        # # 지난달 작업파일 찾는 쿼리
        # l_month = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')
        #
        # def find_name(list):
        #     answer = [i for i in list if '제공' in i]
        #     answer = [j for j in answer if l_month in j]
        #     return answer[0]
        #
        # def find_name2(list, nm):
        #     answer = [i for i in list if 'dat' in i]
        #     answer = [j for j in answer if nm in j]
        #     return answer[0]
        #
        # os.getcwd()
        # os.chdir(path='../')
        # dir_ = find_name(list(os.listdir()))
        #
        # # 저장한 폴더에 들어가기
        # os.chdir(path='./' + dir_)
        #
        # lfile_list = os.listdir()
        # lfile_path = os.getcwd()
        #
        # print(lfile_list)
        # print(lfile_path)


        # 파일 경로 설정
        last_month = (datetime.strptime(self.d,"%Y%m") - relativedelta(months=1)).strftime('%Y%m')
        last_month_path = f"{self.data_path}/{last_month}/말일/원천_처리후"

        file_path1 = f"{self.path}/말일/원천/"
        file_path3 = f"{self.path}/말일/원천_처리후/"

        filenm1 = ['45', '46', '47', '48', '49']
        filenm2 = ['45.rtp_re_csi_inf_', '46.rtp_hse_csi_inf_', '47.rtp_ld_csi_inf_', '48.rtp_hse_t_csi_inf_',
                   '49.rtp_hse_js_csi_inf_']

        # 원천 파일 읽기
        for fn_1, fn_2 in zip(filenm1, filenm2):
            #   file_name = input('원천 파일명을 입력해주세요. (.xlsx제외)  ex)' + item1 + '시장소비심리지수 : ')
            sy = pd.read_excel(f'{file_path1}/{fn_1}.xlsx', dtype='str')

            # 현재 년월을 기준으로 데이터 처리
            yyyymm = datetime.strptime(self.d,"%Y%m") - relativedelta(months=1)
            yyyymm_bf = yyyymm.strftime('%Y-%m')

            try:
                sy = sy.loc[:, ['지역명', yyyymm_bf]]
            except:
                yyyymm_bf = input('원천파일에 지난달에 해당하는 컬럼이 없습니다. 필요시 년월을 입력해주세요 ex)yyyy-mm : ')
                sy = sy.loc[:, ['지역명', yyyymm_bf]]

            yyyymm_af = datetime.strptime(yyyymm_bf, '%Y-%m') + relativedelta(months=1)

            code = pd.read_csv(f'{self.refer_path}/소비심리지수_sido.dat', sep='|', dtype='str', encoding='ANSI')

            sy_tp = pd.merge(code, sy, how='right', right_on='지역명', left_on='KED시장시도구분명')
            print(tb(sy_tp.loc[sy_tp['KED시장시도구분명'].isna(), ['지역명']], headers='keys', tablefmt='pretty'))
            if input('전국, 수도권, 비수도권을 제외한 새로운 지역명이 생겼을 경우, 코드수정 필요시 press n : ') == 'n':
                quit()

            sy = pd.merge(code, sy, how='left', right_on='지역명', left_on='KED시장시도구분명')
            sy['자료발표일'] = yyyymm_af.strftime('%Y%m01')
            sy['자료기준년월'] = yyyymm_af.strftime('%Y%m')
            sy = sy.loc[:, ['자료발표일', 'KED시장시도구분', 'KED시장시도구분명', yyyymm_bf, '자료기준년월']]
            sy.columns = ['자료발표일', 'KED시장시도구분', 'KED시장시도구분명', '소비심리지수값', '자료기준년월']

            print(tb(sy, headers='keys', tablefmt='pretty'))
            ldf = pd.read_csv(f"{last_month_path}/{fn_2}{last_month}30.dat", header=None, sep='|', dtype='str',encoding='ANSI')
            ldf.columns = ['자료발표일', 'KED시장시도구분', 'KED시장시도구분명', '소비심리지수값', '자료기준년월']
            sy = pd.concat([sy, ldf], axis=0, ignore_index=True)

            sy.to_csv(f'{file_path3}/{fn_2}yyyymmdd.dat', sep='|', index=False, encoding='ANSI')

    def trans_55(self):
        ## 이번달 작업 외 수행 시 아래 코드 사용
        ## yyyymm = '2022.09월' # (현재 년월)

        file_path1 = f"{self.path}/20일/원천/55.xls"
        file_path3 = f"{self.path}/20일/원천_처리후/"

        jiga = pd.read_excel(file_path1, header=4, dtype='str')

        # 필요한 컬럼만 추출
        jiga = jiga.iloc[:, [0, 1, 12, 13, 14, 15, 16, 17, 18]]
        jiga.columns = ['CODE', '행정구역', '전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타']

        # 필요없는 데이터 지우기
        jiga.dropna(subset=['전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타'], how='all', inplace=True)

        # 행정구역에 한글 제외하고 모두 삭제
        jiga['행정구역'] = jiga['행정구역'].apply(lambda x: re.sub('[\W\d]', '', x))
        sido_list = ['전국', '서울특별시', '인천광역시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시',
                     '세종특별자치시', '경기도', '강원도', '충청북도', '충청남도', '전라북도', '전라남도', '경상북도', '경상남도', '제주자치도']
        jiga['시도'] = [sido if sido in sido_list else np.nan for sido in jiga['행정구역']]
        jiga['시도'].fillna(method='ffill', inplace=True)

        # sido_list에 해당하는 행정구역 삭제하기 위한 함수 만들기
        def del_nm(x):
            for item in sido_list:
                x = re.sub(item, '', x)
            return x

        jiga['행정구역'] = jiga['행정구역'].apply(lambda x: del_nm(x))
        jiga['시도시군구'] = jiga['시도'] + jiga['행정구역']
        jiga = jiga.loc[:, ['시도시군구', '전', '답', '주거용_대', '상업용_대', '임야', '공장', '기타']]

        jiga.fillna('', inplace=True)

        # 형태에 맞춰주기 위해 Transpose 하기
        jiga.set_index('시도시군구', drop=True, inplace=True)
        jiga = jiga.stack()
        jiga = pd.DataFrame(jiga.reset_index())

        jiga.columns = ['시도시군구', '이용상황구분명', '값']

        # 코드값 불러와서 붙이기
        sido_df = pd.read_csv(f"{self.refer_path}/55_이용상황별 지가지수_시도시군구.dat" , sep='|', encoding='ANSI')
        gubun = pd.read_csv(f"{self.refer_path}/55_이용상황별 지가지수_구분명.dat", sep='|', dtype='str', encoding='ANSI')

        jiga = pd.merge(sido_df, jiga, how='left', on='시도시군구')
        jiga = pd.merge(jiga, gubun, how='left', on='이용상황구분명')
        # 필요한 컬럼만 추출
        jiga = jiga.loc[:, ['시군구CODE', '시군구명', '시도시군구', '이용상황구분', '이용상황구분명', '값']]
        # 정렬
        jiga.sort_values(['시군구CODE', '이용상황구분'], inplace=True)

        jiga['값'].replace('-', '', inplace=True)
        jiga.drop_duplicates(inplace=True)

        print(tb(jiga, headers='keys', tablefmt='pretty'))
        jiga.to_csv(f"{file_path3}/55.rtp_usecase_jg_index_inf_yyyymmdd.dat", sep='|', index=False, encoding='ANSI')


    def trans_56(self):
        ## 이번달 작업 외 수행 시 아래 코드 사용
        ## yyyymm = '2022.09월' # (현재 년월)

        file_path1 = f"{self.path}/20일/원천/56.xlsx"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        # 지역 추가시에 변경
        dic = {'부산': '0',
               '대구': '1',
               '울산': '2',
               '강원': '3',
               '충북': '4',
               '전북': '5',
               '전남': '6',
               '경북': '7',
               '경남': '8',
               '제주': '9'}

        df = pd.read_excel(file_path1, header=10, dtype='str', engine='openpyxl', sheet_name='Sheet1')

        col_nm = df.columns[0]
        df_fin = df.set_index(col_nm).stack().reset_index()
        df_fin.columns = ['지역', '자료기준년월', '값']

        df_fin['자료기준년월'] = df_fin['자료기준년월'].apply(lambda x: (datetime.strptime(x, '%Y년 %m월') + relativedelta(months=1)).strftime('%Y%m01'))
        df_fin['지역코드'] = df_fin['지역'].replace(dic)

        # 지수기준년월 변경 시 변경
        df_fin['지수기준년월'] = '202210'

        df_fin = df_fin[['자료기준년월', '지역코드', '지역', '값', '지수기준년월']]
        print(tb(df_fin, headers='keys', tablefmt='pretty'))

        df_fin.to_csv(f"{file_path2}/56.rtp_hyuksin_city_jg_index_inf_yyyymmdd.dat", sep='|', index=False,header=None, encoding='ANSI')

    def trans_57(self):
        file_1 = f"{self.path}/20일/원천/57_1.xlsx"
        file_2 = f"{self.path}/20일/원천/57_2.xlsx"
        file_path2 = f"{self.path}/20일/원천_처리후/"

        # file_name3 = '42_공동주택현황_코드'

        df1 = pd.read_excel(file_1,sheet_name='데이터', engine='openpyxl')
        df1.fillna(method='ffill', inplace=True)

        df2 = pd.read_excel(file_2,sheet_name='데이터', engine='openpyxl')
        df2.fillna(method='ffill', inplace=True)

        code_nm = []
        for i in df1['계정항목별']:
            if '주택담보대출' in i:
                code_nm.append('주택담보대출')
            elif '기타대출' in i:
                code_nm.append('기타대출')
            else:
                code_nm.append('예금취급기관')
        df1['계정항목별'] = code_nm
        code_nm = []
        for i in df2['계정항목별']:
            if '주택담보대출' in i:
                code_nm.append('주택담보대출')
            elif '기타대출' in i:
                code_nm.append('기타대출')
            elif '비은행예금취급기관' in i:
                code_nm.append('예금취급기관')
            else:
                code_nm.append('')
        df2['계정항목별'] = code_nm
        print(list(df1.columns))
        yyyymm = input('사용할 컬럼명을 입력해주세요 ex)2022.03 (작업월-3) : ')
        df1 = df1.loc[df1['계정항목별'].apply(lambda x: x in ['예금취급기관', '주택담보대출', '기타대출']), ['계정항목별', '지역코드별'] + [yyyymm]]
        print(list(df2.columns))
        yyyymm = input('사용할 컬럼명을 입력해주세요 ex)2022.03 (작업월-3) : ')
        df2 = df2.loc[df2['계정항목별'].apply(lambda x: x in ['예금취급기관', '주택담보대출', '기타대출']), ['계정항목별', '지역코드별'] + [yyyymm]]

        df = pd.merge(df1, df2, how='inner', on=['계정항목별', '지역코드별'])
        df['값'] = df[yyyymm + '_x'] + df[yyyymm + '_y']
        df['값'] = df['값'].apply(lambda x: round(x, 1))
        gubun = pd.read_csv(f"{self.refer_path}/57_예금취급기관의 가계대출_구분값.dat", sep='|', dtype='str', header=None, encoding='ANSI')
        gubun.columns = ['code', '계정항목별']
        sido = pd.read_csv(f"{self.refer_path}/57_예금취급기관의 가계대출_시도.dat", sep='|', dtype='str', encoding='ANSI')
        df = pd.merge(df, gubun, how='inner', on='계정항목별')
        df = pd.merge(df, sido, how='inner', on='지역코드별')
        df = df.loc[:, ['sido_code', '지역코드별', 'code', '계정항목별', '값']]
        yyyymmdd = (datetime.strptime(yyyymm, '%Y.%m') + relativedelta(months=1)).strftime('%Y%m%d')
        yyyymm = yyyymmdd[:-2]
        df.insert(0, '자료발표일자', yyyymmdd)
        df['자료기준년월'] = yyyymm

        print('하나라도 다른게 있다면 확인 必必必必')
        print(df1.shape[0], ' / ', df2.shape[0], ' / ', df.shape[0], sep='')

        df.to_csv(f"{file_path2}/57.rtp_householdloan_yyyymmdd.dat", sep='|', index=False, header=False, encoding='ANSI')

if __name__ == "__main__":
    str_d = "202305"
    trans = Trans(f'C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data',str_d)
