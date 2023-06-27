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

        # 키 중복 확인
        self.check_files_unique_key(self.path)

        # 최신날짜 확인
        self.check_files_last_dt(self.path)

    def check_files_last_dt(self,file_path):
        '''
        file_paths에 있는 파일들의 가장 최근값 확인
        '''
        files = os.listdir(file_path)
        for file in files:
            df = pd.read_csv(f"{self.path}/{file}", encoding="CP949", header=None, sep='|')
            df[0]

            file_num = get_key_by_val(self.FINAL_FILE_NAME_DICT,file)
            before_month_n = self.RUN_SCHEDULE[file_num][3] + 1
            yyyy,m = return_y_m_before_n_v2(self.d,before_month_n)
            mm = m.zfill(2)


    def check_files_unique_key(self,file_path):
        '''
        file_paths에 있는 파일들의 키중복이 있는지 확인
        '''
        files = os.listdir(file_path)
        for file in files:
            for set_num,file_name in self.FINAL_FILE_NAME_DICT.items():
                if (file==file_name) and (set_num in self.EX_KEY_DICT):
                    print(set_num,file_name,end=" ")
                    df = pd.read_csv(f"{self.path}/{file}",encoding="CP949",header=None,sep='|')
                    index_list = self.EX_KEY_DICT[set_num]

                    print(df.duplicated(index_list).sum())
                    assert df.duplicated(index_list).sum() == 0
                    continue


if __name__ == "__main__":
    str_d = "202306"
    base_v = BaseVal(str_d)
    Check(f"C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data\\202306\\말일\\원천_처리후",str_d,base_v)
