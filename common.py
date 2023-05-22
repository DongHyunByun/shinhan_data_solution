from datetime import datetime, timedelta
import pandas as pd

def round_digit(path, column, digit):
    '''
    path의 column컬럼의 소숫점을  digit까지 표시한다.
    '''
    def my_round(a):
        return round(a,digit)

    file_type = path.split(".")[-1]
    if file_type=="xlsx":
        df = pd.read_excel(path,sep='|',encoding='CP949',dtype=str)
    else:
        df = pd.read_csv(path,sep='|',encoding='CP949',dtype=str)

    df[column] = df[column].astype('float')
    df[column] = df[column].apply(my_round)
    # print(df)

    df.to_csv(path,encoding="CP949",sep='|',index=False)

if __name__ == "__main__":
    path = "C:\\Users\\KODATA\\Desktop\\project\\신한은행\\5월\\20일\\원천_처리후"

    file_name = "8.rtp_apt_js_inf_yyyymmdd.dat" #!!!!!!!
    digit_col = "실거래가격지수값" #!!!!!!!

    round_digit(f"{path}\\{file_name}",digit_col,10)