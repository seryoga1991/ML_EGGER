import glob
import os
import cust_types as cst
import config as cfg
import utils
import pandas as pd
import numpy as np
from calc_utility import get_unique_wl_attachment

def classify_docs(func):
    def wrapper(*args, **kwargs):
        try:
            tangro_modul = kwargs['tangro_modul']
            classifier_method = kwargs['classifier_method']
            filter_method = kwargs['filter_method']
            wordlist_class = kwargs['wordlist_class']
        except:
            raise KeyError('Eines der Key-Argumente wurde nicht übergeben')
        data_dir = kwargs.get('data_dir', cfg.save_dir)
        threshold = kwargs.get('threshold', 0.8)
        sapdata_class = kwargs.get('sapdata_class')
        utils.clean_tmp_data()
        classified_dict = classify(tangro_modul, classifier_method, filter_method, wordlist_class,
                                   data_dir, threshold,  sapdata_class)
        kwargs['classified_dict'] = classified_dict
        try:
            classify_main_directory = func(*args, **kwargs)
        finally:
            utils.clean_tmp_data()
        return classify_main_directory
    return wrapper


""" def classify_docs(tangro_modul: str, classifier_method:cst.ClassifierMethod, wordlist_class=None, sapdata_class=None):
    if tangro_modul == cfg.tangro_om:
        classified_dict = classify_spam(cst.FilterMethod.debitor, wordlist_class)
    else:
        pass
    return classified_dict """


def get_docs_above_threshold(filtered_file, attachment, threshold):
    ab_doc1 = filtered_file[(filtered_file['DOC1_ATTNO'] == attachment) & (
        filtered_file['Distance'] >= threshold)]
    ab_doc2 = filtered_file[(filtered_file['DOC2_ATTNO'] == attachment) & (
        filtered_file['Distance'] >= threshold)]
    above_threshold = pd.concat([ab_doc1, ab_doc2], ignore_index=True)
    return above_threshold


def get_docs_below_threshold(filtered_file, attachment, threshold):
    bl_doc1 = filtered_file[(filtered_file['DOC1_ATTNO'] == attachment) & (
        filtered_file['Distance'] < threshold) & (
        filtered_file['Distance'] > 0.0)]
    bl_doc2 = filtered_file[(filtered_file['DOC2_ATTNO'] == attachment) & (
        filtered_file['Distance'] < threshold) & (
        filtered_file['Distance'] > 0.0)]
    below_threshold = pd.concat([bl_doc1, bl_doc2], ignore_index=True)
    return below_threshold


def get_partner_list(correlation_dir: str, tangro_modul):
    only_partners = (cfg.sap_modul_keys.get(tangro_modul)).get('partner')
    all_partner_files = glob.glob(os.path.join(
        correlation_dir, f'*{only_partners}*'))

    def get_partner_from_filename(x): return x.split('_')[3][:-4]
    partner_list = [get_partner_from_filename(f) for f in all_partner_files]
    return partner_list


def get_unique_doc_list(correlation_file: pd.DataFrame) -> cst.docs_list:
    unique_doc_list = correlation_file['DOC1']
    unique_doc_list = unique_doc_list.append(correlation_file['DOC2'])
    unique_doc_list = unique_doc_list.unique()
    return unique_doc_list


def get_unique_attachments(correlation_file: pd.DataFrame, document_no: cst.docs):
    doc = document_no
    filtered_file = correlation_file[(correlation_file['DOC1'] == doc) | (
        correlation_file['DOC2'] == doc)]
    unique_attachments_1 = filtered_file[filtered_file['DOC1']
                                         == doc]['DOC1_ATTNO']
    unique_attachments_2 = filtered_file[filtered_file['DOC2']
                                         == doc]['DOC2_ATTNO']
    union_unique_attachments = pd.concat(
        [unique_attachments_1, unique_attachments_2])
    unique_attachments = union_unique_attachments.unique()
    return unique_attachments, filtered_file


def determine_pass_fail_ratio(filtered_file, attachment, threshold):
    above_threshold = get_docs_above_threshold(
        filtered_file, attachment, threshold)
    below_threshold = get_docs_below_threshold(
        filtered_file, attachment, threshold)
    if not above_threshold.empty:
        passed_failed_ratio = len(
            below_threshold.index)/len(above_threshold.index)
    else:
        passed_failed_ratio = 2


def get_spam_by_correlation_scores(file: pd.DataFrame, threshold: np.float64, definetly_spam=[]):
    spam_list = []
    not_spam_list = []
    unique_doc_list = get_unique_doc_list(correlation_file=file)
    for doc in unique_doc_list:
        unique_attachments, filtered_file = get_unique_attachments(
            correlation_file=file,document_no = doc)
        number_of_attachments = len(unique_attachments)
        for attachment in unique_attachments:
            document = str(doc) + '_' + str(attachment)
            if document not in definetly_spam.tolist() or number_of_attachments == 1:
                passed_failed_ratio = determine_pass_fail_ratio(
                    filtered_file, attachment, threshold)
                if passed_failed_ratio < 1:
                    spam_list.append(document)
                elif passed_failed_ratio >= 1:
                    not_spam_list.append(document)
            else:
                spam_list.append(document)
    return spam_list, not_spam_list




def get_spam_by_modul_specs(modul, wordlist_class,sapdata_class):
    definetly_spam = []
    if modul == cfg.tangro_om:
        wordlist = wordlist_class.get_wordlist()
        if sapdata_class == None:
            
            wordlist = wordlist[wordlist['WORT'] == wordlist['DOC_NUMBER']]
            if not wordlist.empty:
                definetly_spam = wordlist['FILE_NAME']
                return definetly_spam
            else:
                return None
        else:
            unique_doc_list = wordlist['FILE_NAME'].unique() 
            total_length = len(unique_doc_list)
            for idx,doc in enumerate(unique_doc_list):
                wortliste = wordlist[wordlist['FILE_NAME'] == doc]
                doc_no = str(wortliste['DOC_NUMBER'].iloc[0])
                concat_words = "".join(wortliste['WORT'].astype(str))
                if str(sapdata_class.headers[sapdata_class.headers['DOC_NUMBER'] == doc_no]['PURCH_NO'].iloc[0]) != 'nan':
                    customer_orderno = sapdata_class.headers[sapdata_class.headers['DOC_NUMBER'] == doc_no]['PURCH_NO'].iloc[0]
                    customer_orderno = customer_orderno.upper()
                else: 
                    customer_orderno = ""
                concat_words = concat_words.upper()
                if customer_orderno  and customer_orderno in concat_words:
                    pass
                else: 
                    filename = wortliste['FILE_NAME'].iloc[0]
                    definetly_spam.append(filename)
                print(idx/total_length)
            return definetly_spam
            




def get_spam(filter_value: str, correlation_dir, threshold, definetly_spam=[]):
    filter_by = '*_' + filter_value + '.csv'
    file_path = glob.glob(os.path.join(correlation_dir, filter_by))
    file = pd.read_csv(file_path[0])
    spam_list, not_spam_list = get_spam_by_correlation_scores(
        file, threshold, definetly_spam=definetly_spam)
    print(f'Done:{filter_value}')
    return [spam_list, not_spam_list]


def classify_spam(tangro_modul, wordlist_class, partner_list, correlation_dir, threshold,sapdata_class):
    classific_dict = {}
    modul_specific_spam = get_spam_by_modul_specs(
        modul=tangro_modul, wordlist_class=wordlist_class, sapdata_class = sapdata_class)
    for partner in partner_list:
        classific_dict[partner] = get_spam(
            partner, correlation_dir, threshold, definetly_spam=modul_specific_spam)
    return classific_dict


def classify(tangro_modul, classifier_method: cst.ClassifierMethod, filter_method: cst.FilterMethod, wordlist_class, correlation_dir=cfg.save_dir, threshold=0.8, sapdata_class=None):
    partner_list = get_partner_list(correlation_dir, tangro_modul)
    if filter_method == cst.FilterMethod.debitor:
        if classifier_method == cst.ClassifierMethod.spam:
            if sapdata_class == None:
                return classify_spam(tangro_modul, wordlist_class, partner_list, correlation_dir, threshold)
            else:
                return get_spam_by_modul_specs(tangro_modul,wordlist_class,sapdata_class)
    else:
        print("Eine gültige Methode für den Spam-Filter angeben.")
"""     return classific_dict """
