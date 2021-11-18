import pandas as pd
import numpy as np
from custom_transformer import hot_encode
import csv
import os
from math import sqrt
import config as cfg
import re
# Types
document_number = int
aggregated_array = np.ndarray
two_doc_aggregation = tuple[list[document_number,
                                 document_number], list[aggregated_array, aggregated_array]]
doc_doc_correlation = list[tuple[tuple[document_number,
                                       document_number], np.float64]]


class CorrelationError(Exception):
    pass


def aggregate_by_docno(wordlist: pd.DataFrame, hot_encoded_wl: np.ndarray) -> two_doc_aggregation:
    filtered_wl = wordlist.drop_duplicates(
        subset=['DOC_NUMBER', 'ATTACH_NO'], keep='last')
    how_many_wl = len(filtered_wl.index)
    if how_many_wl == 2:
        split_at_index = filtered_wl.index[0]
        last_index = filtered_wl.index[1]
        array = [hot_encoded_wl[i * split_at_index: (1-i)*split_at_index + i * last_index, :].toarray()
                 for i in range(2)]
        simple_aggregation = [np.sum(x, axis=0) for x in array]
        word_hits = [np.divide(x, x, out=np.zeros_like(
            x), where=x != 0) for x in simple_aggregation]
        return ([docno for docno in filtered_wl['DOC_NUMBER']], word_hits)
    elif how_many_wl == 1:
        diag_entry = filtered_wl.at[filtered_wl.index[0], 'DOC_NUMBER']
        return ([diag_entry, diag_entry], [np.zeros(1), np.zeros(1)])
    else:
        raise CorrelationError(
            f'Es dürfen nur 2 Wortlisten mit einadner korreliert werden. Es wurden aber {how_many_wl} übergeben.')


def correlate(vec: two_doc_aggregation, first_attachment, second_attachment):
    normalized_difference = np.linalg.norm(
        (vec[1][0]-vec[1][1]), 1)/len(vec[1][1])
    return [vec[0][0], vec[0][1], first_attachment, second_attachment, normalized_difference]


def attachno(f: str):
    try:
        attachnumb = f.split('_')[1]
        doc_no = int(f.split('_')[0])
    except:
        doc_no = f
        attachnumb = None
    return attachnumb, doc_no


def get_unique_wl_attachment(wordlist: pd.DataFrame, doc_no):
    attachnumber, doc_number = attachno(doc_no)
    if attachnumber != None:
        filtered_wl = wordlist[(wordlist['DOC_NUMBER'] == doc_number) & (
            wordlist['ATTACH_NO'] == attachnumber)]
    elif attachnumber == None:
        filtered_wl = wordlist[wordlist['DOC_NUMBER'] == doc_no]
    unique_vals = filtered_wl['ATTACH_NO'].unique()
    """     if len(unique_vals) > 5:  # höchstens  5 Attachments sonst wird Rechenzeit zu hoch
        unique_vals = unique_vals[0:5] """
    return unique_vals, filtered_wl


def starts_with_1(wordlist: pd.DataFrame):
    if wordlist[wordlist['SEITE'] == 1].empty:
        return False
    else:
        return True


def equalize_pages(first_wl: pd.DataFrame, second_wl: pd.DataFrame):
    no_of_pages = first_wl['SEITE'].unique().tolist()
    reduced_second_wl = second_wl[second_wl['SEITE'].isin(
        no_of_pages)]
    no_of_pages = second_wl['SEITE'].unique().tolist()
    reduced_first_wl = first_wl[first_wl['SEITE'].isin(
        no_of_pages)]
    return reduced_first_wl, reduced_second_wl


def prepare_docs_data(doc_range, count):
    docs = doc_range[2*count + 1].copy()
    docs_count = len(docs)
    docs_to_correlate = doc_range[2*count].copy()
    doc_corr_count = len(docs_to_correlate)
    return docs, docs_to_correlate, docs_count, doc_corr_count


def prepare_writer(file):
    file_writer = csv.writer(file, delimiter=',')
    file_writer.writerow(
        ['DOC1', 'DOC2', 'DOC1_ATTNO', 'DOC2_ATTNO', 'Distance'])
    return file_writer


def prepare_data_for_correlation(tot_first_wl, first_attachment, tot_second_wl, second_attachment):
    filtered_first_wl = tot_first_wl[tot_first_wl['ATTACH_NO']
                                     == first_attachment]
    filtered_second_wl = tot_second_wl[tot_second_wl['ATTACH_NO']
                                       == second_attachment]
    tot_first_wl, tot_second_wl = equalize_pages(
        filtered_first_wl, filtered_second_wl)
    return [filtered_first_wl, filtered_second_wl, tot_first_wl, tot_second_wl]


def correlate_error_docs(doc1, doc2, first_attachment, second_attachment):
    document_i = attachno(doc1)[1]
    document_j = attachno(doc2)[1]
    two_doc_corr = [
        document_i, document_j, first_attachment, second_attachment, 1]
    return two_doc_corr


def correlate_proper_docs(tot_first_wl, tot_second_wl, first_attachment, second_attachment):
    concat_wl = pd.concat(
        [tot_first_wl, tot_second_wl], ignore_index=True)
    two_doc_corr = correlate(aggregate_by_docno(
        concat_wl, hot_encode(concat_wl)), first_attachment, second_attachment)
    return two_doc_corr


def proper_doc(data_list):
    if starts_with_1(data_list[0]) and starts_with_1(data_list[1]) and not data_list[2].empty and not data_list[3].empty:
        return True
    else:
        False


def correlate_docs(wordlist: pd.DataFrame, doc_range: pd.Series = None) -> doc_doc_correlation:
    for count in range(2):
        docs, docs_to_correlate, docs_count, doc_corr_count = prepare_docs_data(
            doc_range=doc_range, count=count)
        if not docs.empty:
            cfg.temp_name = f'data_{cfg.name_appendix}_{doc_corr_count}_{docs_count}.csv'
            print(cfg.temp_name)
            with open(os.path.join(cfg.temporary_save_dir, cfg.temp_name), 'a+', newline='') as temp_file:
                file_writer = prepare_writer(temp_file)
                for idx, i in enumerate(docs):
                    for j in docs_to_correlate:
                        first_attachments, tot_first_wl = get_unique_wl_attachment(
                            wordlist, i)
                        second_attachments, tot_second_wl = get_unique_wl_attachment(
                            wordlist, j)
                        for first_attachment in first_attachments:
                            for second_attachment in second_attachments:
                                data = prepare_data_for_correlation(
                                    tot_first_wl, first_attachment, tot_second_wl, second_attachment)
                                if not proper_doc(data_list=data):
                                    two_doc_corr = correlate_error_docs(
                                        i, j, first_attachment, second_attachment)
                                else:
                                    two_doc_corr = correlate_proper_docs(
                                        data[2], data[3], first_attachment, second_attachment)
                                file_writer.writerow([x for x in two_doc_corr])
                                if two_doc_corr[0] == two_doc_corr[1]:
                                    second_attachments = second_attachments[second_attachments !=
                                                                            first_attachment]
                    docs = docs[docs != i]
