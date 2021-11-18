
from data_loader import load_wordlists
import os
import sys
import pandas as pd
import numpy as np
import glob
from zlib import crc32
from config import *
from enum import Enum


# Types
page = int
first_margin = int
second_margin = int
margin_setup = list[list[page, list[first_margin, second_margin]]]


class FilterMethod(Enum):
    debitor = 1


def transpose_list(liste):
    liste = list(map(list, zip(*liste)))
    return liste


def binomial_equipartition(series: pd.Series, parts_count: int) -> list[pd.Series]:

    remainder = len(series) % parts_count
    slice_size = int((len(series) - remainder)/parts_count)
    sliced_series_list = [[series[slice_size*i:slice_size *
                                  i + slice_size], series[slice_size*i:]]for i in range(parts_count - 1)]
    sliced_series_list.append(
        [series[slice_size*(parts_count - 1):], series[slice_size*(parts_count - 1):], pd.Series([]), pd.Series([])])
    mean_val = len(sliced_series_list[int(parts_count/2 - 1)][1])
    if slice_size > cores_to_use and cores_to_use > 2:
        for j in range(int(parts_count/2)):
            reduced_series_len = len(series) - j * slice_size
            equi_load_step = int(
                slice_size*(reduced_series_len - mean_val - 1)/reduced_series_len)
            diff_step = int(slice_size - equi_load_step)
            reverse_idx = int(parts_count - 2 - j)
            if j < (parts_count/2 - 1):
                sliced_series_list[j] = [series[j*slice_size:j*slice_size + diff_step],
                                         series[slice_size*j:],
                                         pd.Series([]),
                                         pd.Series([])]
                sliced_series_list[reverse_idx] = [*sliced_series_list[reverse_idx],
                                                   series[j*slice_size + diff_step: j *
                                                          slice_size + slice_size],
                                                   sliced_series_list[j][1][j*slice_size + diff_step:]]
            else:
                sliced_series_list[j] = [
                    *sliced_series_list[j], pd.Series([]), pd.Series([])]

    return sliced_series_list


def get_debitor_list(correlation_dir):
    only_debitors = '*SOLD_TO*'
    all_debitor_files = glob.glob(os.path.join(correlation_dir, only_debitors))
    def get_debitor_from_filename(x): return x.split('_')[3][:-4]
    debitor_list = [get_debitor_from_filename(f) for f in all_debitor_files]
    return debitor_list


def get_docs_above_threshold(filtered_file, attachment, threshold):
    ab_doc1 = filtered_file[(filtered_file['DOC1_ATTNO'] == attachment) & (
        filtered_file['Distance'] >= threshold)]
    ab_doc2 = filtered_file[(filtered_file['DOC2_ATTNO'] == attachment) & (
        filtered_file['Distance'] >= threshold)]
    above_threshold = pd.concat([ab_doc1, ab_doc2], ignore_index=True)
    return above_threshold


def get_docs_below_threshold(filtered_file, attachment, threshold):
    bl_doc1 = below_threshold = filtered_file[(filtered_file['DOC1_ATTNO'] == attachment) & (
        filtered_file['Distance'] < threshold) & (
        filtered_file['Distance'] > 0.0)]
    bl_doc2 = filtered_file[(filtered_file['DOC2_ATTNO'] == attachment) & (
        filtered_file['Distance'] < threshold) & (
        filtered_file['Distance'] > 0.0)]
    below_threshold = pd.concat([bl_doc1, bl_doc2], ignore_index=True)
    return below_threshold


def get_spam_by_correlation_scores(file: pd.DataFrame, threshold: np.float64):
    spam_list = []
    not_spam_list = []
    unique_doc_list = file['DOC1']
    unique_doc_list = unique_doc_list.append(file['DOC2'])
    unique_doc_list = unique_doc_list.unique()
    for doc in unique_doc_list:
        filtered_file = file[(file['DOC1'] == doc) | (file['DOC2'] == doc)]
        unique_attachments = filtered_file[filtered_file['DOC1']
                                           == doc]['DOC1_ATTNO']
        unique_attachments.append(
            filtered_file[filtered_file['DOC2'] == doc]['DOC2_ATTNO'])
        unique_attachments = unique_attachments.unique()

        for attachment in unique_attachments:
            document = str(doc) + '_' + str(attachment)
            modul_specific_spam = get_spam_by_modul_specs(
                modul='sd', doc=document)
            if document != modul_specific_spam:
                above_threshold = get_docs_above_threshold(
                    filtered_file, attachment, threshold)
                below_threshold = get_docs_below_threshold(
                    filtered_file, attachment, threshold)
                if not above_threshold.empty:
                    passed_failed_ratio = len(
                        below_threshold.index)/len(above_threshold.index)
                else:
                    passed_failed_ratio = 2
                if passed_failed_ratio < 1:
                    spam_list.append(document)
                elif passed_failed_ratio >= 1:
                    not_spam_list.append(document)
            else:
                spam_list.append(document)
    return spam_list, not_spam_list


def get_spam_by_modul_specs(modul, doc):
    if modul == 'sd':
        wordlist = load_wordlists(
            PATH_TO_FILES=path_to_wordlists, subset=[doc])
        if not wordlist.empty:
            filtered_wl = wordlist[wordlist['WORT'] == doc]
            if not filtered_wl.empty:
                return doc
            else:
                return None
        else:
            return None


def get_spam(filter_value: str, correlation_dir, threshold, modul: str = ''):
    filter_by = '*_' + filter_value + '.csv'
    file_path = glob.glob(os.path.join(correlation_dir, filter_by))
    file = pd.read_csv(file_path[0])
    spam_list, not_spam_list = get_spam_by_correlation_scores(file, threshold)
    print(f'Done:{filter_value}')
    return [spam_list, not_spam_list]


def classify_spam(filter_method: FilterMethod, correlation_dir=save_dir, threshold=0.8) -> list[int]:
    classific_dict = {}
    if filter_method == FilterMethod.debitor:
        debitors = get_debitor_list(correlation_dir)
        for debitor in debitors:
            classific_dict[debitor] = get_spam(
                debitor, correlation_dir, threshold, 'sd')
    else:
        print("Eine gültige Methode für den Spam-Filter angeben.")
    return classific_dict


def filter_sdDicts(object: dict, filter_set: sd_key_val_pair.keys, filter_column, filter_value, wordlist=None):

    conv_filter_val = int(filter_value)
    if filter_set not in list(sd_key_val_pair.keys()):
        raise ValueError(
            "Die Parameter filter_set und object müssen in %r liegen" % sd_key_val_pair.keys())
    else:
        new_dict = {}
        Set = object.get(filter_set)
        Set = Set[Set[filter_column] == conv_filter_val]
        new_dict[filter_set] = Set
        if wordlist is not None:
            filtered_wordlist = wordlist[wordlist['DOC_NUMBER'].isin(
                Set[sd_key_val_pair.get(filter_set)])]
        for key, item in object.items():
            if key is not filter_set:
                filtered_item = item[item[sd_key_val_pair.get(key)].isin(
                    Set[sd_key_val_pair.get(filter_set)])]
                new_dict[key] = filtered_item
            else:
                pass
    if wordlist is None:
        return new_dict
    else:
        return new_dict, filtered_wordlist


def sort_margins(left_margins, up_margins):
    sorted_left = [[item[0], [item[1][0], item[1][1]].sort()]
                   for item in left_margins]
    sorted_up = [[item[0], [item[1][0], item[1][1]].sort()]
                 for item in up_margins]
    return sorted_left, sorted_up


def normalize_margins(left_margins, up_margins):
    normalized_left = left_margins
    normalized_up = up_margins
    for idx, item in enumerate(normalized_left):

        if item[1][1] > left_max_dpi:
            normalized_left[idx][1][0] = int(item[1][0]/item[1][1])
            normalized_left[idx][1][1] = left_max_dpi
    for idx, item in enumerate(normalized_up):
        if item[1][1] > up_max_dpi:
            normalized_up[idx][1][0] = int(item[1][0]/item[1][1])
            normalized_up[idx][1][1] = up_max_dpi

    return normalized_left, normalized_up


def filter_wl_by_left_coord(wordlist, normalized_left):
    for margin in normalized_left:
        seite = margin[0]
        links_1 = margin[1][0]
        links_2 = margin[1][1]
        if not seite == None and seite > 0:
            filtered_wl = wordlist[wordlist['SEITE'] == seite].between(
                links_1, links_2, inclusive=True)
            filtered_wl = pd.concat(
                filtered_wl, wordlist[wordlist['SEITE'] != seite], ignore_index=True)
    return filtered_wl


def filter_wl_by_up_coord(wordlist, normalized_up):
    for margin in normalized_up:
        seite = margin[0]
        up_1 = margin[1][0]
        up_2 = margin[1][1]
        if not seite == None and seite > 0:
            filtered_wl = wordlist[wordlist['SEITE'] ==
                                   seite].between(up_1, up_2, inclusive=True)
            filtered_wl = pd.concat(
                filtered_wl, wordlist[wordlist['SEITE'] != seite], ignore_index=True)
    return filtered_wl


def sort_wl_by_coord(wordlist: pd.DataFrame):
    sorted_wl = wordlist.sort_values(by=['SEITE', 'LINKS', 'OBEN'])
    return sorted_wl


def sort_wl_by_neighbors(wordlist: pd.DataFrame):
    sorted_wl = wordlist.sort_values(by=['SEITE', 'LINKS', 'OBEN'])
    return sorted_wl


def filter_sort_wl_by_coord(wordlist: pd.DataFrame, left_margins: margin_setup, up_margins: margin_setup):
    sorted_left, sorted_up = sort_margins(left_margins,  up_margins)
    normalized_left, normalized_up = normalize_margins(sorted_left, sorted_up)
    filtered_wl_left = filter_wl_by_left_coord(wordlist, normalized_left)
    filtered_wl_up = filter_wl_by_up_coord(filtered_wl_left, normalized_up)
    sorted_filtered_wl = sort_wl_by_coord(filtered_wl_up)
    return sorted_filtered_wl
