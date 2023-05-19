import pandas as pd
import os

# 파일 시스템 함수를 호출하는 모듈 들
# print(os.listdir("C:\\Users\\KODATA\\Desktop\\project\\신한은행\\5월\\20일\\원천_처리후"))

file_num_name={1:"rtp_usecase_jg",2:"rtp_gdhse_t_inf",3:"rtp_gdhse_sea_inf",4:"rtp_sz_apt_t_in",5:"rtp_sz_apt_js_inf",
               7:"rtp_apt_t_inf",8:"rtp_apt_js_inf", 9:"rtp_apt_sz_mid", 10:"rtp_apt_sz_avg", 11:"rtp_apt_t_mid",
               12:"rtp_apt_t_avg",13:"rtp_apt_js_mid",14:"rtp_apt_js_avg", 15:"rtp_sz_yd_s_inf", 16:"rtp_yd_t_inf",
               17:"rtp_yd_sz_mid",18:"rtp_yd_sz_avg",19:"rtp_yd_t_mid",20:"rtp_yd_t_avg",21:"rtp_cei_inf",
               22:"rtp_item_cpi1_inf",23:"rtp_item_ppi_inf",38:"rtp_gsat_us",42:"rtp_gdhse_now", 55:"rtp_usecase_jg_index_inf",
               56:"rtp_hyuksin_city_jg_index_inf",57:"rtp_householdloan"}

def add_num_20(path):
    '''
    디렉토리에 있는 20일자 파일들을 넘버링 한다.
    '''
    files = os.listdir(path)
    for file_name in files:
        common_size = 0
        fin_num=None
        for num,file_name_head in file_num_name.items():
            if file_name_head in file_name:
                if common_size < len(file_name_head): # 공통으로 겹치는 제목이 더 많은 번호를 줌
                    common_size = len(file_name_head)
                    fin_num = num

        os.rename(f'{path}/{file_name}',f'{path}/{fin_num}.{file_name}')

if __name__ == "__main__":
    path = "C:\\Users\\KODATA\\Desktop\\project\\신한은행\\5월\\20일\\db"
    add_num_20(path)



