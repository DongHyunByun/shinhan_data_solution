import pandas as pd
from tabulate import tabulate as tb
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

## 이번달 작업 외 수행 시 아래 코드 사용
## yyyymm = '2022.09월' # (현재 년월)

class Trans:
    path = None
    d = None
    today = datetime.now().strftime('%Y.%m월')
    refer = "refer"

    def __init__(self,path,d):
        self.path = path
        self.d = d

        # self.trans_21()
        # self.trans_22()
        # self.trans_23()
        self.trans_38()

    def trans_21(self):
        file_name1 = f"{self.path}/20일/원천/21.csv"
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

        out = pd.read_csv('refer/22_품목별 소비자물가지수_구분명.dat', dtype='str', sep='|', header=None, encoding='ANSI')

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
        file_path3 = 'refer/'

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
        today = datetime.now().strftime('%Y%m')
        today = datetime.strptime(today, '%Y%m') - relativedelta(months=2)
        print(today)
        today = today.strftime('%Y%m')
        print(today)
        unsold = unsold.loc[unsold['자료기준년월'] == today, :]

        # 코드값 붙일 파일 불러오기
        sido = pd.read_csv(file_path3 + '38_공사완료후_미분양현황_시도시군구.dat', sep='|', encoding='ANSI')
        scale = pd.read_csv(file_path3 + '38_공사완료후_미분양현황_부문규모.dat', sep='|', encoding='ANSI')

        unsold = pd.merge(sido, unsold, how='left', on='시도시군구')
        unsold = pd.merge(unsold, scale, how='left', on='부문_규모')

        unsold = unsold.loc[:, ['자료발표일자', '시군구CODE', '시군구명', 'CODE', '부문_규모', '호', '자료기준년월']]
        unsold.dropna(axis=0, subset=['부문_규모'], inplace=True)

        print(tb(unsold.head(10), headers='keys', tablefmt='pretty'))
        print(tb(unsold.tail(10), headers='keys', tablefmt='pretty'))

        unsold.to_csv(file_path2 + '38.rtp_gsat_us_' + unsold['자료발표일자'][0] + '.csv',sep='|', header=False, index=False, encoding='ANSI')

if __name__ == "__main__":
    trans = Trans(f'C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data\\202304',"202304")
