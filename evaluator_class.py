import concurrent.futures
import functools
import config as cfg
import glob
import global_config as glb
from file_mover import move_spam
import cust_types as cst
from wordlist_class import WordList
from sapdata_class import SapData
from data_manipulation_helper import binomial_equipartition, filter_sdDicts, flatten_list
from doc_classifier_methods import get_spam_by_correlation_scores, get_spam_by_modul_specs
import calc_utility
from doc_classifier_methods import classify_docs
import os
import utils
import pandas


class Evaluator:
    def __init__(self, path_to_wordlists: str = cfg.path_to_wordlists, path_to_sap_data: str = cfg.path_to_sap_data, tangro_modul=cfg.tangro_om, files_subset=[]):
        initialize_members(self, path_to_wordlists,
                           path_to_sap_data, tangro_modul, files_subset)

    def calculate_bi_correlation_scores(self):
        calc_corr(self)

    @classify_docs
    def classify_spam(self, wordlist_class, tangro_modul, classifier_method, filter_method, classified_dict):
        self.sapdata.set_test_train_dict(ratio=0.)
        all_data = self.sapdata.get_train_dict()
        self.spam_list = determine_all_spam(
            modul=self.tangro_modul, classified_dict=classified_dict, wordlist_class=self.wordlist, all_docs=all_data)

    def move(self, data_type: cst.ClassifierMethod):
        if data_type == cst.ClassifierMethod.spam:
            try:
                move_spam(self.spam_list)
            except:
                print('Attribut Spamliste nicht vorhanden')


def initialize_members(self, path_to_wordlists, path_to_sap_data, tangro_modul, files_subset):
    self.tangro_modul = tangro_modul
    self.wordlist = WordList(
        path_to_wordlists=cfg.path_to_wordlists, subset=files_subset)
    self.sapdata = SapData(wordlist_filter=self.wordlist.get_wordlist(),
                           path_to_sap_data=path_to_sap_data, tangro_modul=tangro_modul)
    self.spam_list = []


def set_multithread(series):

    # ab einem Punkt ist Multithreading nicht notwendig
    if len(series) > cfg.cores_to_use and cfg.cores_to_use > 1:
        glb.apply_multithread = True
    else:
        glb.apply_multithread = False


def save_data(file_name: str):
    all_files = glob.glob(os.path.join(
        cfg.temporary_save_dir, "*.csv"))
    if all_files:
        total_file = pandas.concat((pandas.read_csv(
            f, engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE) for f in all_files), ignore_index=True)
        total_file.to_csv(os.path.join(
            cfg.save_dir, f'data_{file_name}.csv'))


def prepare_sets(dictionary: dict, frame: str, category: str, value: str, wl: pandas.DataFrame):
    filtered_sets, filtered_wl = filter_sdDicts(
        dictionary, frame, category, value, wl)
    series = filtered_sets['headers']['DOC_NUMBER']
    return filtered_sets, filtered_wl, series


def add_target_values(series):
    new_series = [series, series, pandas.Series([]), pandas.Series([])]
    return new_series


def calculate_series_correlation(filtered_wl: pandas.DataFrame, series):
    if glb.apply_multithread:
        if cfg.cores_to_use in cfg.core_check:
            frozen_wl_correlator = functools.partial(
                calc_utility.correlate_docs, filtered_wl)
            sliced_series_list = binomial_equipartition(
                series, cfg.cores_to_use)
            with concurrent.futures.ProcessPoolExecutor(max_workers=cfg.cores_to_use) as executor:
                executor.map(frozen_wl_correlator, sliced_series_list)
        else:
            raise cfg.InvalidCoreCount(
                f'Nicht vorgesehene CPU Kernangabe: {cfg.cpu_count} liegen vor und {cfg.cores_to_use} zugewiesen.')
    else:
        new_series = add_target_values(series)
        calc_utility.correlate_docs(filtered_wl, new_series)


def create_evaluation_list(modul):
    if modul == cfg.tangro_om:
        ag_counts = self.sapdata._train_dict['headers']['SOLD_TO'].value_counts(
        )
        mat_counts = self.sapdata._train_dict['items']['MATERIAL'].value_counts(
        )
        eval_filter_list = [[
            'headers', 'SOLD_TO', list(ag_counts.index)], ['items', 'MATERIAL', list(mat_counts.index)]]
    else:
        pass
    return eval_filter_list


def calc_corr(self):
    eval_filter_list = create_evaluation_list(self.tangro_modul)
    for eval_set in eval_filter_list:
        frame = eval_set[0]
        category = eval_set[1]
        values = eval_set[2]
        for count in values:
            filtered_sets, filtered_wl, series = prepare_sets(
                self.sapdata.get_train_dict(), frame, category, count, self.wordlist.get_wordlist())
            set_multithread(series)
            calculate_series_correlation(filtered_wl, series)
            file_name = str(category) + '_' + str(count)
            save_data(file_name)
            utils.clean_tmp_data()


def prepare_structs_for_comparisson(partner_dict, partner, filter_dict, wordlist):
    try:
        filtered_dict, wl = filter_sdDicts(
            filter_dict, 'headers', 'SOLD_TO', partner, wordlist)
        docs_for_partner = filtered_dict.get('headers')[cfg.sd_key_val_pair.get(
            'headers')]
        examplary_non_spam_doc = partner_dict[partner][1][0]
        compare_to_doc = pandas.Series(examplary_non_spam_doc)
        docs_for_correlation = [docs_for_partner,
                                compare_to_doc, pandas.Series([], dtype=float), pandas.Series([], dtype=float)]
    except IndexError as idx:
        docs_for_correlation = cst.pass_docs
        wl = cst.pass_docs
    return docs_for_correlation, wl, docs_for_partner


def correlacte_docs(wordlist, docs_to_compare):
    calc_utility.correlate_docs(wordlist, docs_to_compare)
    file_path = glob.glob(os.path.join(cfg.temporary_save_dir, cfg.temp_name))
    file = pandas.read_csv(file_path[0])
    return file


def get_filenames_for_partner_docs(wordlist_class, partner_docs):
    all_partner_files = wordlist_class.get_wordlist(
        doc_numbers=partner_docs.tolist())
    all_partner_files = all_partner_files['FILE_NAME'].unique().tolist(
    )
    return all_partner_files


def determine_all_spam(modul, classified_dict: dict,  wordlist_class, all_docs: cst.train_dict):
    spam_list = []
    non_spam_list = []
    definetly_spam = get_spam_by_modul_specs(
        modul=cfg.tangro_om, wordlist_class=wordlist_class)
    for key in classified_dict:
        cfg.name_appendix = key
        docs_to_compare, wordlist, partner_docs = prepare_structs_for_comparisson(
            partner_dict=classified_dict, partner=key, filter_dict=all_docs, wordlist=wordlist_class.get_wordlist())
        if docs_to_compare != cst.kill_all_docs:
            file = correlacte_docs(wordlist, docs_to_compare)
            temp_spam_list, temp_non_spam_list = get_spam_by_correlation_scores(
                file, 0.8, definetly_spam=definetly_spam)
            spam_list.append(temp_spam_list)
        elif docs_to_compare == cst.kill_all_docs:
            spam_list.append(get_filenames_for_partner_docs(
                wordlist_class, partner_docs))
    return flatten_list(spam_list)
