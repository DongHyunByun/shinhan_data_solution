import pandas as pd
from tabulate import tabulate as tb
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

## 이번달 작업 외 수행 시 아래 코드 사용
## yyyymm = '2022.09월' # (현재 년월)

class Trans:
    path = None
    d = None
    today = datetime.now().strftime('%Y.%m월')
    refer = "refer"

    def __init__(self,data_path,d):
        self.path = f"{data_path}\\{d}"
        self.refer_path = f"{data_path}\\refer"
        self.d = d

        # 20일자
        self.trans_1()
        # self.trans_2_20()  # ipy로부터 작업 필요
        # self.trans_21()
        # self.trans_22()
        # self.trans_23()
        # self.trans_38()
        # self.trans_42()
        # self.trans_55()
        # self.trans_56()
        # self.trans_57()

        # 말일자




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
        today = datetime.now().strftime('%Y.%m월')
        # 이번달 작업 외 수행 시 아래 코드 사용
        # today = '2022.11월' # (현재 년월)

        # 파일 경로 설정
        file_path1 = '../' + today + '/원천/'
        file_path2 = '../파이썬코드/'
        file_path3 = '../' + today + '/수기작업/'

        file_name = input('파일명을 입력해주세요.  ex)23.1월 공동주택 실거래가격지수 통계표 : ')

        no_list = [2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        sheets = [['매매_공동주택', '매매 증감률_공동주택'], '매매_공동주택_계절조정', '규모별 매매_아파트', '규모별 전세_아파트', ['매매_아파트', '매매 증감률_아파트'],
                  '전세_아파트', '규모별 매매 중위_아파트', '규모별 매매 평균_아파트', '매매 중위_아파트', '매매 평균_아파트', '전세 중위_아파트', '전세 평균_아파트',
                  '규모별 매매_연립다세대', '매매_연립다세대', '규모별 매매 중위_연립 다세대', '규모별 매매 평균_연립 다세대', '매매 중위_연립 다세대', '매매 평균_연립 다세대']

        for i in range(len(no_list)):
            print(no_list[i], ' : ', sheets[i])

        jibang = pd.read_csv(file_path2 + '지방도.dat', dtype='str', sep='|', encoding='ANSI')

        # 2번
        df1 = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=1, sheet_name=sheets[0][0],
                            engine='openpyxl')
        df1.columns = [re.sub('[^가-힣]', '', col) for col in df1.columns]
        df1 = df1.set_index('지역구분년월').stack(level=0).reset_index()
        df1.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df1['자료발표일자'] = df1['자료발표일자'].apply(
            lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + relativedelta(months=1)).strftime('%Y%m%d'))
        df1['자료기준년월'] = '201711'

        df2 = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=1, sheet_name=sheets[0][1],
                            engine='openpyxl')
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

        df.to_csv(file_path3 + '2.rtp_gdhse_t_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)
        # 3번
        df = pd.read_excel(file_path1 + file_name + '.xlsm', header=1, sheet_name=sheets[1], engine='openpyxl')
        df.columns = [re.sub('[^가-힣]', '', col) for col in df.columns]
        df = df.set_index('지역구분년월').stack(level=0).reset_index()
        df.columns = ['자료발표일자', 'KED시도구분명', '실거래가격지수값']
        df['자료발표일자'] = df['자료발표일자'].apply(lambda x: (x + relativedelta(months=1)).strftime('%Y%m%d'))
        df['자료기준년월'] = '201711'
        df = df.merge(jibang, how='inner', left_on='KED시도구분명', right_on='cdnm')
        df = df[['자료발표일자', 'cd', 'KED시도구분명', '실거래가격지수값', '자료기준년월']].sort_values(by=['자료발표일자', 'cd'],
                                                                                ascending=[False, True])

        scale_cd = pd.read_csv(file_path2 + '규모시군구.dat', sep='|', dtype='str', encoding='ANSI')
        df.to_csv(file_path3 + '3.rtp_gdhse_sea_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 4번 데이터
        df = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=[1, 2], sheet_name=sheets[2],
                           engine='openpyxl')
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

        # 5번 데이터
        df = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=[1, 2], sheet_name=sheets[3],
                           engine='openpyxl')
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
        df.to_csv(file_path3 + '5.rtp_sz_apt_js_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        sido_cd = pd.read_csv(file_path2 + '시도.dat', sep='|', dtype='str', encoding='ANSI')
        sido_cd.tail()

        # 7번 데이터
        df1 = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=[1, 2], sheet_name=sheets[4][0],
                            engine='openpyxl')
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

        df2 = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=[1, 2], sheet_name=sheets[4][1],
                            engine='openpyxl')
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
        df.to_csv(file_path3 + '7.rtp_apt_t_inf_yyyymmdd.dat', sep='|', encoding='ANSI', index=False)

        # 8번 데이터
        df = pd.read_excel(file_path1 + file_name + '.xlsm', dtype='str', header=[1, 2], sheet_name=sheets[5],
                           engine='openpyxl')
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
