import pandas
import numpy
import calc_utility
import data_manipulation_helper
import data_loader
import concurrent.futures
import functools
from multiprocessing import Process, Manager
import config as cfg
from file_mover import move_spam
import csv
import glob
import os
import sys

apply_multithread = True


def set_multithread(series):
    global apply_multithread
    # ab einem Punkt ist Multithreading nicht notwendig
    if len(series) > cfg.cores_to_use and cfg.cores_to_use > 1:
        apply_multithread = True
    else:
        apply_multithread = False


def save_data(file_name: str):
    all_files = glob.glob(os.path.join(
        cfg.temporary_save_dir, "*.csv"))
    if all_files:
        total_file = pandas.concat((pandas.read_csv(
            f, engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE) for f in all_files), ignore_index=True)
        total_file.to_csv(os.path.join(
            cfg.save_dir, f'data_{file_name}.csv'))


def clean_tmp_data():
    all_files = glob.glob(os.path.join(
        cfg.temporary_save_dir, "*.csv"))
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
        if cfg.cores_to_use in cfg.core_check:
            frozen_wl_correlator = functools.partial(
                calc_utility.correlate_docs, filtered_wl)
            sliced_series_list = data_manipulation_helper.binomial_equipartition(
                series, cfg.cores_to_use)
            with concurrent.futures.ProcessPoolExecutor(max_workers=cfg.cores_to_use) as executor:
                executor.map(frozen_wl_correlator, sliced_series_list)
        else:
            raise cfg.InvalidCoreCount(
                f'Nicht vorgesehene CPU Kernangabe: {cfg.cpu_count} liegen vor und {cfg.cores_to_use} zugewiesen.')
    else:
        new_series = add_target_values(series)
        calc_utility.correlate_docs(filtered_wl, new_series)


def caclculate_correlation():
    wl = data_loader.load_wordlists()
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


def classify_docs():
    classified_dict = data_manipulation_helper.classify_spam(
        data_manipulation_helper.FilterMethod.debitor)
    return classified_dict


def prepare_structs_for_comparisson(debitor_dict, debitor, filter_dict, wordlist):
    exemplary_non_spam_doc = debitor_dict[debitor][1][0]
    filtered_dict = data_manipulation_helper.filter_sdDicts(
        filter_dict, 'headers', 'SOLD_TO', debitor, wordlist)
    docs_for_debitor = filtered_dict.get('headers')[cfg.sd_key_val_pair.get(
        'headers')]
    compare_to_doc = pandas.Series(exemplary_non_spam_doc)
    docs_for_correlate = [docs_for_debitor,
                          compare_to_doc, pandas.Series([]), pandas.Series([])]
    wl = wordlist[wordlist['DOC_NUMBER'].isin(docs_for_correlate[0])]
    return docs_to_compare, wl


def correlacte_docs(wordlist, docs_to_compare):
    calc_utility.correlate_docs(wordlist, docs_to_compare)
    file_path = glob.glob(os.path.join(cfg.temporary_save_dir, cfg.temp_name))
    file = pandas.read_csv(file_path[0])
    return file


def determine_all_spam(classified_dict):
    wl = data_loader.load_wordlists()
    sd = data_loader.SapData(cfg.path_to_sap_data,
                             data_loader.tangro_om, wl, 0.)
    spam_list = []
    non_spam_list = []
    cfg.name_appendix = 'temp_corr'
    for key in classified_dict:
        docs_to_compare, wordlist = prepare_structs_for_comparisson(
            debitor_dict=classified_dict, debitor=key, filter_dict=sd.__train_dict, wordlist=wl)
        file = correlacte_docs(wordlist, docs_to_compare)
        temp_spam_list, temp_non_spam_list = data_manipulation_helper.get_spam_by_correlation_scores(
            file, 0.8)
        spam_list.append(temp_spam_list)
        non_spam_list.append(temp_non_spam_list)

    spam_list = [spam for sublist in spam_list for spam in sublist]
    return spam_list


def get_rid_of_spam(data_dir=cfg.path_to_wordlists):
    spam_list = []
    classified_dict = classify_docs()
    spam_list = determine_all_spam(classified_dict)
    move_spam(spam_list)
    clean_tmp_data()


if __name__ == '__main__':
    get_rid_of_spam()
