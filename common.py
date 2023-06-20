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

def fill_row(file_path, columns):
    '''
    file_path파일의 column컬럼중 none값인 것을 위에서 부터 채운다
    61.연도별 건축허가현황, 외부통계 번호 : 30 참고
    '''
    df_origin = pd.read_excel(file_path, dtype='str', engine='openpyxl')
    size = len(df_origin)

    for column in columns:
        data_type = df_origin[column][0]

        for i in range(1, size):
            if df_origin[column][i] != df_origin[column][i]:
                df_origin[column][i] = data_type
            else:
                data_type = df_origin[column][i]

    return df_origin

def khapi_gangwon_change():
    '''
    오피스텔 전체 데이터 강원도 코드 변경
    '''
    path = "C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\final\\202306\\20일자\\오피스텔, 주택매매\\rtp_khpi_inf_202306.txt"
    to_path = "C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\final\\202306\\20일자\\오피스텔, 주택매매\\rtp_khpi_inf_202306_new.txt"
    df_origin = pd.read_csv(path, encoding='CP949', header=None, sep='|',dtype='str')


    df = pd.DataFrame()
    days = sorted(df_origin[0].unique(),reverse=True)

    for d in days:
        # d일의 강원도->강원특별자치도로 바꾼 데이터
        new_gangwon_df = df_origin[(df_origin[0] == str(d))&(df_origin[2]=="강원도")]
        new_gangwon_df[1] = "51" + new_gangwon_df[1].str[2:]
        # new_gangwon_df[2].loc[(new_gangwon_df[2] == "강원도")] = "강원특별자치도"
        new_gangwon_df[2] = ["강원특별자치도" for _ in range(len(new_gangwon_df))]

        # d일의 강원도 + 강원자치도
        d_df = df_origin[(df_origin[0] == str(d))]
        d_df = pd.concat([d_df,new_gangwon_df]).drop_duplicates().sort_values(by=1)

        # 전체 concoat
        df = pd.concat([df, d_df])

    df.to_csv(to_path , sep='|', index=False, header=False, encoding='ANSI')

def ofpi_gangwon_change():
    '''
    주택매매 전체 데이터 강원도 코드 변경
    '''

    path = "C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\final\\202306\\20일자\\오피스텔, 주택매매\\rtp_ofpi_inf_202306.txt"
    to_path = "C:\\Users\\KODATA\\Desktop\\project\\shinhan_data\\final\\202306\\20일자\\오피스텔, 주택매매\\rtp_ofpi_inf_202306_new.txt"
    df_origin = pd.read_csv(path, encoding='CP949', header=None, sep='|',dtype='str')


    df = pd.DataFrame()
    days = sorted(df_origin[0].unique())

    for d in days:
        # d일의 강원도->강원특별자치도로 바꾼 데이터
        new_gangwon_df = df_origin[(df_origin[0] == str(d))&(df_origin[2]=="강원도")]
        new_gangwon_df[1] = "51" + new_gangwon_df[1].str[2:]
        # new_gangwon_df[2].loc[(new_gangwon_df[2] == "강원도")] = "강원특별자치도"
        new_gangwon_df[2] = ["강원특별자치도" for _ in range(len(new_gangwon_df))]

        # d일의 강원도 + 강원자치도
        d_df = df_origin[(df_origin[0] == str(d))]
        d_df = pd.concat([d_df,new_gangwon_df]).drop_duplicates().sort_values(by=1)

        # 전체 concoat
        df = pd.concat([df, d_df])

    df.to_csv(to_path , sep='|', index=False, header=False, encoding='ANSI')

if __name__ == "__main__":
    # path = "C:\\Users\\KODATA\\Desktop\\project\\신한은행\\5월\\20일\\원천_처리후"
    # file_name = "8.rtp_apt_js_inf_yyyymmdd.dat" #!!!!!!!
    # digit_col = "실거래가격지수값" #!!!!!!!
    #
    # round_digit(f"{path}\\{file_name}",digit_col,10)

    # khapi_gangwon_change()
    ofpi_gangwon_change()