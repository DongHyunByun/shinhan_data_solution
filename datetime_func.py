# 시간관련 모듈들

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

import pandas as pd

def return_y_m_before_n(d, n):
    '''
    d일(date type)에서 n월 전 값의 년,월을 반환한다.
    '''
    return (str((d + timedelta(days=15) - timedelta(days=30 * n)).year),
            str((d + timedelta(days=15) - timedelta(days=30 * n)).month))

def return_y_m_before_n_v2(d, n):
    '''
    d일(date type)에서 n월 전 값의 년,월을 반환한다.
    '''
    n_month_before_d = (d - relativedelta(months=n))
    return (str(n_month_before_d.year),str(n_month_before_d.month))

def return_last_day_of_yyyymm(yyyy,mm):
    input_dt = datetime(int(yyyy), int(mm),1)
    res = calendar.monthrange(input_dt.year, input_dt.month)
    return res[1]