from file_down import FileDown
from datetime import datetime
from trans import Trans
from run_month import RUN_SCHEDULE

import argparse

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--d", type=str, default=datetime.today().strftime("%Y%m"),
                      help="크롤링을 시작할 날짜. YYYYMM")
    args.add_argument("--path", type=str, default=f'C:\\Users\\KODATA\\Desktop\\project\\shinhan_data',
                      help="프로젝트 폴더")
    args.add_argument("--work_day", type=str, default='all',
                      help="작업대상 일자(5, 20, 말일)")
    config = args.parse_args()

    path = config.path
    str_d = config.d
    work_day = config.work_day

    # 크롤링
    FileDown(path, str_d, work_day, RUN_SCHEDULE)

    # 데이터 전처리
    # Trans(path, str_d, work_day, RUN_SCHEDULE)