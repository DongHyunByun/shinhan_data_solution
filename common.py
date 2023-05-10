from datetime import datetime, timedelta

def return_y_m_before_n(d, n):
    '''
    d일(date type)에서 n월 전 값의 년,월을 반환한다.
    '''
    return (str((d + timedelta(days=15) - timedelta(days=30 * n)).year),
            str((d + timedelta(days=15) - timedelta(days=30 * n)).month))