import pandas as pd
from tabulate import tabulate as tb
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

# today = datetime.now().strftime('%Y.%m월')
## 이번달 작업 외 수행 시 아래 코드 사용
yyyymm = '2023.02월' # (현재 년월)

file_path1 = ''
file_name1 = input('파일명을 입력해주세요 (.xlsx 제외)  ex) 경기종합지수_2015100___10차__20220614171810  : ')
file_path2 = ''

df = pd.read_excel(file_path1 + file_name1 + '.xlsx', dtype='str', engine='openpyxl', sheet_name='데이터')
df.columns = [re.sub('[ p)]', '', x) for x in df.columns]
df.set_index('지수별', inplace=True)
df = df.T

# 자료발표일자 (월 + 1) 만들어주기
yyyymm_list = list(df.index)
yyyymm_list = [(datetime.strptime(x, '%Y.%m') + relativedelta(months=1)) for x in yyyymm_list]
yyyymm_list = [x.strftime('%Y%m%d') for x in yyyymm_list]
df.insert(0, '자료발표일자', yyyymm_list)

# 지수 기준일이 바뀌면 변경 필수 !!
df['자료기준년월'] = '201512'

kospi = [i for i in range(len(df.columns)) if '코스피' in df.columns[i]]
if len(kospi) == 0:
    print(tb(df, headers='keys', tablefmt='pretty'))
    print(df.shape)
else:
    df.drop([df.columns[i] for i in kospi], axis=1, inplace=True)
    print(tb(df, headers='keys', tablefmt='pretty'))
    print(df.shape)

yn = input('저장하시겠습니까?  y/n : ')
if yn != 'n':
        df.to_csv(file_path2 + '21.rtp_cei_inf_yyyymmdd.dat',
                  sep='|', index=False, encoding='ANSI')