import pandas as pd
import numpy as np
from custom_transformer import hot_encode
import csv
import os
from math import sqrt
from config import *


def aggregate_by_docno(wordlist: pd.DataFrame, hot_encoded_wl: np.ndarray) -> ([int], [np.ndarray]):
    filtered_wl = wordlist.drop_duplicates(subset=['DOC_NUMBER'])
    wordlist_count = len(filtered_wl.index)
    if wordlist_count == 2:
        index_set = [wordlist.index[wordlist['DOC_NUMBER'] ==
                                    docno].tolist() for docno in filtered_wl['DOC_NUMBER']]
        array = [hot_encoded_wl[x[0]:x[-1], :].toarray() for x in index_set]
        aggregation = [np.sum(x, axis=0) for x in array]
        aggregation = [np.divide(x, x, out=np.zeros_like(
            x), where=x != 0) for x in aggregation]
        return ([docno for docno in filtered_wl['DOC_NUMBER']], aggregation)
    elif wordlist_count == 1:
        diag_entry = filtered_wl.at[0, 'DOC_NUMBER']
        return ([diag_entry, diag_entry], [np.zeros(1), np.zeros(1)])


def correlate(vec: ([int], [np.ndarray])):
    normalized_difference = np.linalg.norm((vec[1][0]-vec[1][1]),1)/len(vec[1][1])
    return [vec[0][0], vec[0][1], normalized_difference]


def correlate_docs(wordlist: pd.DataFrame, doc_range: pd.Series = None) -> [((int, int), np.float64)]:
    for count in range(2):
        docs = doc_range[2*count + 1].copy()
        docs_count = len(docs)
        docs_to_correlate = doc_range[2*count].copy()
        doc_corr_count = len(docs_to_correlate)
        if not docs.empty:
            with open(os.path.join(temporary_save_dir, f'data_{name_appendix}_{doc_corr_count}_{docs_count}.csv'), 'a+', newline='') as temp_file:
                file_writer = csv.writer(temp_file, delimiter=',')
                print('Start:', doc_corr_count, docs_count)
                file_writer.writerow(['DOC1', 'DOC2', 'Distance'])
                for idx, i in enumerate(docs):
                    for j in docs_to_correlate:
                        concat_wl = pd.concat(
                            [wordlist[wordlist['DOC_NUMBER'] == i], wordlist[wordlist['DOC_NUMBER'] == j]], ignore_index=True)
                        two_doc_corr = correlate(aggregate_by_docno(
                            concat_wl, hot_encode(concat_wl)))
                        file_writer.writerow([x for x in two_doc_corr])
                    docs = docs[docs != i]
                print('Finish:', doc_corr_count, docs_count)


def correlate_docs_single_thread(wordlist: pd.DataFrame, doc_range: pd.Series = None) -> [((int, int), np.float64)]:
    docs = doc_range.copy()
    docs_count = len(docs)
    if not docs.empty:
        with open(os.path.join(temporary_save_dir, f'data_{name_appendix}_{docs_count}.csv'), 'a+', newline='') as temp_file:
            file_writer = csv.writer(temp_file, delimiter=',')
            file_writer.writerow(['DOC1', 'DOC2', 'Distance'])
            for idx, i in enumerate(docs):
                for j in docs:
                    concat_wl = pd.concat(
                        [wordlist[wordlist['DOC_NUMBER'] == i], wordlist[wordlist['DOC_NUMBER'] == j]], ignore_index=True)
                    two_doc_corr = correlate(aggregate_by_docno(
                        concat_wl, hot_encode(concat_wl)))
                    file_writer.writerow([x for x in two_doc_corr])
                docs = docs[docs != i]
