import pandas
import numpy
import calc_utility
import data_manipulation_helper
import data_loader
import concurrent.futures
import functools
from multiprocessing import Process, Manager
from config import *
import csv
import glob
import os,sys
def main():
    manager = multiprocessing.Manager()
    shared_list = manager.list()
    lock = manager.Lock()
    wl = data_loader.load_all_wordlists()
    sd = data_loader.SapData(
        data_loader.path_to_sap_data, data_loader.tangro_om, wl,0.9)
    ag_counts = sd.__train_dict['headers']['SOLD_TO'].value_counts()
    mat_counts = sd.__train_dict['items']['MATERIAL'].value_counts()
    filter_list = [['headers','SOLD_TO',list(ag_counts.index)],['items','MATERIAL',list(mat_counts.index)]]
    
    for sd_set in filter_list:
        for count in sd_set[2]:
            name_appendix = sd_set[1] + '_' + str(count)
            filtered_sets, filtered_wl = data_manipulation_helper.filter_sdDicts(
                sd.__train_dict, sd_set[0], sd_set[1], count, wl)
            series = filtered_sets['headers']['DOC_NUMBER']    
            print(len(series))
            sliced_series_list = data_manipulation_helper.equipartition(
                series, cores_to_use)
            frozen_wl_correlator = functools.partial(
                calc_utility.correlate_docs, wl)
            with concurrent.futures.ProcessPoolExecutor(max_workers=cores_to_use) as executor:
                executor.map(frozen_wl_correlator, sliced_series_list)
            all_files = glob.glob(os.path.join(temporary_save_dir, "*.csv"))
            total_file = pandas.concat((pandas.read_csv(f, engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE) for f in all_files))
            total_file.to_csv(os.path.join(save_dir,f'data_{name_appendix}.csv'))
            os.remove(f for f in all_files)
    
if __name__ == '__main__':
    main()


