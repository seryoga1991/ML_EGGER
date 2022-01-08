import pandas as pd
import glob
import os
import csv
import re
import config as cfg
import cust_types as cst
from pre_proc_sap_data import preproc_sap_data, additional_processing

class NoData(Exception):
    pass

def load_wordlists(PATH_TO_FILES=cfg.path_to_wordlists, subset: cst.files_list = []):
    path_length = len(PATH_TO_FILES) + 1
    if subset == []:
        all_files = glob.glob(os.path.join(PATH_TO_FILES, "*.csv"))
    else:
        all_files = [os.path.join(PATH_TO_FILES, doc + '.csv')
                     for doc in subset]

    if len(all_files) == 1:
        single_file = True
    else:
        single_file = False
    try:
        total_file = read_wordlists(
            all_files=all_files, single_file=single_file, path_length=path_length)
    except FileNotFoundError as e:
        print('Datei nicht vorhanden')

    return total_file


def attachno(f: str):
    try:
        attachnumb = str(f.split('_')[1][:-4])
        if re.search('[a-zA-Z]', attachnumb):
            attachnumb = '1'
    except:
        attachnumb = None
    return attachnumb


def docno(f: str, path_length: int):
    doc_number = str(f.split('_')[0][path_length: path_length + 10])
    return doc_number


def filename(f: str, path_length) -> cst.file_name:
    file_name = str(f[path_length: -4])
    return file_name


def read_wordlists(all_files, single_file: bool = False, path_length=0) -> cst.wordlist:
    if not single_file:
        total_file = pd.concat((pd.read_csv(f, sep=';', engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE).
                                assign(DOC_NUMBER=lambda x: docno(f, path_length), ATTACH_NO=lambda x: attachno(f), FILE_NAME=lambda x: filename(f, path_length)) for f in all_files))
    else:
        total_file = [pd.read_csv(f, sep=';', engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE).assign(
            DOC_NUMBER=lambda x: docno(f, path_length), ATTACH_NO=lambda x: attachno(f), FILE_NAME=lambda x: filename(f, path_length)) for f in all_files]
        total_file = total_file[0]
    return total_file


def load_sap_data(file_name, tangro_modul, path_to_data=cfg.path_to_sap_data):
    name_pattern = file_name.split('.')[0] + "*.csv"
    all_files = glob.glob(os.path.join(path_to_data, name_pattern))
    if len(all_files) == 0:
        raise NoData("Dateien nicht vorhanden") 
    else:
        concat_sap_file = pd.concat(
            (get_sap_data(f, tangro_modul) for f in all_files))
    additional_processing(concat_sap_file)
    return concat_sap_file


def get_sap_data(path_to_file, tangro_modul):
    file = preproc_sap_data(path_to_file, tangro_modul)
    return file
