from file_down import FileDown
from datetime import datetime

import os
import argparse

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--d", type=str, default=datetime.today().strftime("%Y%m"),
                      help="크롤링을 시작할 날짜. YYYYMM")
    args.add_argument("--path", type=str, default=f'C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\data\\{datetime.today().strftime("%Y%m")}',
                      help="20일, kb단지, 말일 폴더가 들어가있는 폴더")
    config = args.parse_args()

    d = config.d
    path = config.path

    path = path[:-6]+config.d

    y = int(d[:4])
    m = int(d[4:6])

    FileDown(y,m,path)