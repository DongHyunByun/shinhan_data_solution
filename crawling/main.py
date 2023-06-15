from file_down import FileDown
from datetime import datetime

import os
import argparse

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--d", type=str, default=datetime.today().strftime("%Y%m"),
                      help="크롤링을 시작할 날짜. YYYYMM")
    args.add_argument("--path", type=str, default=f'C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data',
                      help="날짜별 폴더가 저장될 폴더")
    args.add_argument("--work_day", type=str, default='all',
                      help="작업대상 일자(5일, 20일, 말일)")
    config = args.parse_args()

    str_d = config.d
    path = config.path
    work_day = config.work_day

    FileDown(str_d,path,work_day)