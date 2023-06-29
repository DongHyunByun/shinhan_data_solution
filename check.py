# 데이터를 검증한다
# todo. 추가적으로 검증할 사항들
# 1. 이전달꺼를 누적해서 보내야 하는 데이터들이 있는지
# 2. 원천데이터들의 날짜가 맞는지
# 3. 원천의 기준년월이 달라지지 않았는지


import time
import datetime
import base_val

from base_val import BaseVal
import os
import pandas as pd

from common import *
from datetime_func import *

class Check:
    path = None

    FINAL_FILE_NAME_DICT=None
    EX_KEY_DICT=None

    def __init__(self,path,str_d,base_v):
        self.path = path
        self.str_d = str_d
        self.d = datetime.strptime(str_d, '%Y%m')

        self.FINAL_FILE_NAME_DICT = base_v.FINAL_FILE_NAME_DICT
        self.EX_KEY_DICT = base_v.EX_KEY_DICT
        self.RUN_SCHEDULE = base_v.RUN_SCHEDULE
        self.MONTH_DIFF = base_v.MONTH_DIFF
        self.INCREASE_AMOUNT = base_v.INCREASE_AMOUNT

        # 키 중복 확인
        self.check_files_unique_key(self.path)

        # 최신날짜 확인
        self.check_files_last_dt(self.path)

        # 최신데이터 개수 확인
        self.check_increase_val(self.path)

        # 값 중복확인
        # self.check_duple(self.path)

    def check_increase_val(self,file_path):
        '''
        추가된 값의 개수 확인
        '''
        print("========================추가된 값의 개수 확인========================")
        files = os.listdir(file_path)
        for file in files:
            print(file, end=" ")
            df = pd.read_csv(f"{self.path}/{file}", encoding="CP949", header=None, sep='|', low_memory=False)

            # 실제 데이터 가장최근값의 개수
            file_num = get_key_by_val(self.FINAL_FILE_NAME_DICT, file)
            if not self.INCREASE_AMOUNT[file_num]:
                print("수기확인필요")
                continue
            amount = self.INCREASE_AMOUNT[file_num]

            # 가장 최근값의 개수
            latest_d = df[0].sort_values(ascending=False)[0]
            latest_cnt = len(df[df[0] == latest_d])

            print(amount, latest_cnt)
            # assert latest_cnt==amount

    def check_files_last_dt(self,file_path):
        '''
        file_paths에 있는 파일들의 가장 최근값 확인
        '''
        print("========================최근값확인========================")
        files = os.listdir(file_path)
        for file in files:
            print(file, end=" ")

            # 실제데이터 최근날짜
            df = pd.read_csv(f"{self.path}/{file}", encoding="CP949", header=None, sep='|', low_memory=False)
            latest_d = str(df[0].sort_values(ascending=False)[0])
            latest_d_yyyymm = latest_d[:6]

            # 기준이되는 최근날짜
            file_num = get_key_by_val(self.FINAL_FILE_NAME_DICT,file)
            # file_num = file_num_conv.get(file_num, file_num)
            if not self.MONTH_DIFF[file_num]:
                print("수기확인필요")
                continue
            if file_num in ("84"): # 원천과 똑같다
                before_month_n = self.MONTH_DIFF[file_num]
            else:
                before_month_n = self.MONTH_DIFF[file_num] - 1

            yyyy,m = return_y_m_before_n_v2(self.d,before_month_n)
            mm = m.zfill(2)
            yyyymm = yyyy+mm

            print(latest_d_yyyymm,yyyymm)
            assert latest_d_yyyymm==yyyymm

    def check_files_unique_key(self,file_path):
        '''
        file_paths에 있는 파일들의 키중복이 있는지 확인
        '''
        print("========================키중복 확인========================")
        files = os.listdir(file_path)
        for file in files:
            print(file, end=" ")
            is_check = False
            for set_num,file_name in self.FINAL_FILE_NAME_DICT.items():
                if (file==file_name) and (set_num in self.EX_KEY_DICT):
                    df = pd.read_csv(f"{self.path}/{file}",encoding="CP949",header=None,sep='|', low_memory=False)
                    index_list = self.EX_KEY_DICT[set_num]

                    print(df.duplicated(index_list).sum())
                    assert df.duplicated(index_list).sum() == 0
                    is_check = True
                    continue
            if not is_check:
                print("키 정보(혹은 파일정보) 없음")

    def check_duple(self, file_path):
        print("========================값 중복 확인========================")
        files = os.listdir(file_path)
        for file in files:
            print(file, end=" ")
            df = pd.read_csv(f"{self.path}/{file}", encoding="CP949", header=None, sep='|', low_memory=False)

            unique_dates = df[0].unique()
            total_L = []

            print(df)
            val_col_index = int(input("값이 있는 컬럼 번호 입력 : "))

            for d in unique_dates:
                print(d)
                L = list(df[df[0] == d][val_col_index])

                # if L in total_L:
                #     print("중복값존재")
                #     break
                # else:
                #     total_L.append(L)

                assert (L in total_L)
                total_L.append(L)

if __name__ == "__main__":
    str_d = "202306"
    base_v = BaseVal(str_d)
    Check(f"C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data\\{str_d}\\말일\\원천_처리후\\check",str_d,base_v)
