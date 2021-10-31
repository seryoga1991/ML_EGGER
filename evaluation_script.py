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


def main():
    manager = multiprocessing.Manager()
    shared_list = manager.list()
    lock = manager.Lock()
    wl = data_loader.load_all_wordlists()
    sd = data_loader.SapData(
        data_loader.path_to_sap_data, data_loader.tangro_om, wl,0.9)
    ag_counts = sd.__train_dict['headers']['SOLD_TO'].value_counts()
    mat_counts = sd.__train_dict['items']['MATERIAL'].value_counts()
    test = ag_counts.index
    filter_list = [['headers','SOLD_TO',list(ag_counts.index)],['items','MATERIAL',list(mat_counts.index)]]
    for sd_set in filter_list:
        for count in sd_set[2]:
            filtered_sets, filtered_wl = data_manipulation_helper.filter_sdDicts(
                sd.__train_dict, sd_set[0], sd_set[1], count, wl)
            series = filtered_sets['headers']['DOC_NUMBER']    
            sliced_series_list = data_manipulation_helper.equipartition(
                series, cores_to_use)
            frozen_wl_correlator = functools.partial(
                calc_utility.correlate_docs, wl, series, shared_list, lock)
            with concurrent.futures.ProcessPoolExecutor(max_workers=cores_to_use) as executor:
                results = executor.map(frozen_wl_correlator, sliced_series_list)
            total_result = []
            for result in results:
                total_result.extend(result)
            total_result = data_manipulation_helper.transpose_list(total_result)
            item_length = len(total_result[0])
            with open(f'OneDrive\Desktop\DATA\data_{sd_set[1]}_{count}.csv', 'w') as test_file:
                file_writer = csv.writer(test_file)
                for i in range(item_length):
                    file_writer.writerow([x[i] for x in total_result])
        


if __name__ == '__main__':
    main()


