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
import os
import sys

apply_multithread = True


def set_multithread(series):
    global apply_multithread
    # ab einem Punkt ist Multithreading nicht notwendig
    if len(series) > cores_to_use and cores_to_use > 1:
        apply_multithread = True
    else:
        apply_multithread = False


def save_data(file_name: str):
    all_files = glob.glob(os.path.join(
        temporary_save_dir, "*.csv"))
    if all_files:
        total_file = pandas.concat((pandas.read_csv(
            f, engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE) for f in all_files), ignore_index=True)
        total_file.to_csv(os.path.join(
            save_dir, f'data_{file_name}.csv'))


def clean_tmp_data():
    all_files = glob.glob(os.path.join(
        temporary_save_dir, "*.csv"))
    for f in all_files:
        os.remove(f)


def prepare_sets(dictionary: dict, frame: str, category: str, value: str, wl: pandas.DataFrame):
    filtered_sets, filtered_wl = data_manipulation_helper.filter_sdDicts(
        dictionary, frame, category, value, wl)
    series = filtered_sets['headers']['DOC_NUMBER']
    return filtered_sets, filtered_wl, series


def add_target_values(series):
    new_series = [series, series, pandas.Series([]), pandas.Series([])]
    return new_series


def calculate_series_correlation(filtered_wl: pandas.DataFrame, series):
    if apply_multithread:
        if cores_to_use in core_check:
            frozen_wl_correlator = functools.partial(
                calc_utility.correlate_docs, filtered_wl)
            sliced_series_list = data_manipulation_helper.binomial_equipartition(
                series, cores_to_use)
            with concurrent.futures.ProcessPoolExecutor(max_workers=cores_to_use) as executor:
                executor.map(frozen_wl_correlator, sliced_series_list)
        else:
            raise InvalidCoreCount(
                f'Nicht vorgesehen CPU Kernangabe: {cpu_count} liegen vor und {cores_to_use} zugewiesen.')
    else:
        new_series = add_target_values(series)
        calc_utility.correlate_docs(filtered_wl, new_series)


def main():
    wl = data_loader.load_all_wordlists()
    sd = data_loader.SapData(
        data_loader.path_to_sap_data, data_loader.tangro_om, wl, 0.7)

    ag_counts = sd.__train_dict['headers']['SOLD_TO'].value_counts()
    mat_counts = sd.__train_dict['items']['MATERIAL'].value_counts()
    eval_filter_list = [[
        'headers', 'SOLD_TO', list(ag_counts.index)], ['items', 'MATERIAL', list(mat_counts.index)]]

    for sd_set in eval_filter_list:
        frame = sd_set[0]
        category = sd_set[1]
        values = sd_set[2]
        for count in values:
            filtered_sets, filtered_wl, series = prepare_sets(
                sd.__train_dict, frame, category, count, wl)
            set_multithread(series)
            calculate_series_correlation(filtered_wl, series)
            file_name = str(category) + '_' + str(count)
            save_data(file_name)
            clean_tmp_data()


if __name__ == '__main__':
    main()
