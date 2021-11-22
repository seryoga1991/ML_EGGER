import glob
import os
import cust_types as cst
import config as cfg
import utils
import pandas as pd
import numpy as np


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
    bl_doc1 = below_threshold = filtered_file[(filtered_file['DOC1_ATTNO'] == attachment) & (
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

# TODO der der obersete Block ist vll zu unnötig --> Refactoring + Optimization


def get_spam_by_correlation_scores(file: pd.DataFrame, threshold: np.float64, definetly_spam=[]):
    spam_list = []
    not_spam_list = []
    unique_doc_list = file['DOC1']
    unique_doc_list = unique_doc_list.append(file['DOC2'])
    unique_doc_list = unique_doc_list.unique()
    for doc in unique_doc_list:
        filtered_file = file[(file['DOC1'] == doc) | (file['DOC2'] == doc)]
        unique_attachments_1 = filtered_file[filtered_file['DOC1']
                                             == doc]['DOC1_ATTNO']
        unique_attachments_2 = filtered_file[filtered_file['DOC2']
                                             == doc]['DOC2_ATTNO']
        union_unique_attachments = pd.concat(
            [unique_attachments_1, unique_attachments_2])
        unique_attachments = union_unique_attachments.unique()
        number_of_attachments = len(unique_attachments)
        for attachment in unique_attachments:
            document = str(doc) + '_' + str(attachment)

            if document not in definetly_spam.tolist() or number_of_attachments == 1:
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


def get_spam_by_modul_specs(modul, wordlist_class):
    if modul == cfg.tangro_om:
        wordlist = wordlist_class.get_wordlist()
        wordlist = wordlist[wordlist['WORT'] == wordlist['DOC_NUMBER']]
        if not wordlist.empty:
            definetly_spam = wordlist['FILE_NAME']
            return definetly_spam
        else:
            return None


def get_spam(filter_value: str, correlation_dir, threshold, definetly_spam=[]):
    filter_by = '*_' + filter_value + '.csv'
    file_path = glob.glob(os.path.join(correlation_dir, filter_by))
    file = pd.read_csv(file_path[0])
    spam_list, not_spam_list = get_spam_by_correlation_scores(
        file, threshold, definetly_spam=definetly_spam)
    print(f'Done:{filter_value}')
    return [spam_list, not_spam_list]


def classify_spam(tangro_modul, wordlist_class, partner_list, correlation_dir, threshold):
    classific_dict = {}
    modul_specific_spam = get_spam_by_modul_specs(
        modul=tangro_modul, wordlist_class=wordlist_class)
    for partner in partner_list:
        classific_dict[partner] = get_spam(
            partner, correlation_dir, threshold, definetly_spam=modul_specific_spam)
    return classific_dict


def classify(tangro_modul, classifier_method: cst.ClassifierMethod, filter_method: cst.FilterMethod, wordlist_class, correlation_dir=cfg.save_dir, threshold=0.8, sapdata_class=None):
    partner_list = get_partner_list(correlation_dir, tangro_modul)
    if filter_method == cst.FilterMethod.debitor:
        if classifier_method == cst.ClassifierMethod.spam:
            return classify_spam(tangro_modul, wordlist_class, partner_list, correlation_dir, threshold)
    else:
        print("Eine gültige Methode für den Spam-Filter angeben.")
    return classific_dict
