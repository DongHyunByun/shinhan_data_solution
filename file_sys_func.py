# 파일 시스템 함수를 사용하는 모듈들
import pandas as pd
import os
import shutil
import time

def nubmering(path):
    '''
    PATH에 있는 외부통계 파일들을 넘버링한다.
    '''
    file_num_name = {1: "rtp_usecase_jg",
                     2: "rtp_gdhse_t_inf",
                     3: "rtp_gdhse_sea_inf",
                     4: "rtp_sz_apt_t_in",
                     5: "rtp_sz_apt_js_inf",
                     7: "rtp_apt_t_inf",
                     8: "rtp_apt_js_inf",
                     9: "rtp_apt_sz_mid",
                     10: "rtp_apt_sz_avg",
                     11: "rtp_apt_t_mid",
                     12: "rtp_apt_t_avg",
                     13: "rtp_apt_js_mid",
                     14: "rtp_apt_js_avg",
                     15: "rtp_sz_yd_s_inf",
                     16: "rtp_yd_t_inf",
                     17: "rtp_yd_sz_mid",
                     18: "rtp_yd_sz_avg",
                     19: "rtp_yd_t_mid",
                     20: "rtp_yd_t_avg",
                     21: "rtp_cei_inf",
                     22: "rtp_item_cpi1_inf",
                     23: "rtp_item_ppi_inf",
                     27: "rtp_d_alsqr_st",
                     28: "rtp_d_alsqr_pm",
                     29: "rtp_sido_st",
                     34: "rtp_field_hse_pm_m",
                     36: "rtp_hse_sz_pm",
                     38: "rtp_gsat_us",
                     39: "rtp_sz_us",
                     41: "rtp_sigungu_us",
                     42: "rtp_gdhse_now",
                     43: "rtp_hse_ut_m",
                     44: "rtp_hse_st_m",
                     45: "rtp_re_csi_inf",
                     46: "rtp_hse_csi_inf",
                     47: "rtp_ld_csi_inf",
                     48: "rtp_hse_t_csi_inf",
                     49: "rtp_hse_js_csi_inf",
                     50: "rtp_sg_rtrate",
                     51: "rtp_k_remap",
                     55: "rtp_usecase_jg_index_inf",
                     53: "ked_sandan_in",
                     56: "rtp_hyuksin_city_jg_index_inf",
                     57: "rtp_householdloan"}

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

def mkdir_dfs(path, dir_dict):
    '''
    빈디렉토리를 재귀적으로 만든다
    root_path : 디렉토리를 만들기 시작할 경로
    sub_list : root_path를 시작으로 하위폴더들을 만든다
    ex:
    sub_list = {"8.전국주택 매매가격지수" : {"단독":None, "아파트":None, "연립":None, "종합":None}}
    '''

    if not dir_dict:
        return

    for d in dir_dict:
        new_path = f"{path}/{d}"
        if not os.path.isdir(new_path):
            os.mkdir(new_path)
        mkdir_dfs(new_path, dir_dict[d])

def change_last_file(folder_path, new_name, file_type=None):
    '''
    수정일자가 가장 최근인 파일의 이름을 변경하는 함수
    가장 최근 다운로드된 파일이름을 변경하는데 사용한다.
    '''
    filename = max([folder_path + "\\" + f for f in os.listdir(folder_path)], key=os.path.getctime)
    if not file_type:
        file_type = filename.split(".")[-1]
    shutil.move(filename, os.path.join(folder_path, f"{new_name}.{file_type}"))

def file_check_func(folder_path, mk_time):
    '''
    folder_path 경로에 가장 최근 파일 생성시간이 mk_time이후인지 체크
    없으면 10초간 기다린 후 다시 확인한다.
    최대 3번 반복한다.
    '''
    for i in range(3):
        path = max([folder_path + "\\" + f for f in os.listdir(folder_path)], key=os.path.getctime)
        if mk_time < os.path.getctime(path):
            return True
        time.sleep(10)
    return False

if __name__ == "__main__":
    path = "C:\\Users\\KODATA\\Desktop\\데이터운영부_20230620"
    nubmering(path)



