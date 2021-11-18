import pandas as pd
import glob
import os
import sys
import csv
import re
from config import *
from pre_proc_sap_data import preproc_sap_data, additional_processing
from calc_utility import correlate_docs

docs = str


def test_set_check(identifier, test_ratio):
    return crc32(np.int64(identifier)) & 0xffffffff < test_ratio * 2**32


def split_train_test_by_id(data, test_ratio, id_column):
    ids = data[id_column]
    in_test_set = ids.apply(lambda id_: test_set_check(id_, test_ratio))
    return data.loc[~in_test_set], data.loc[in_test_set]


class SapData:
    def __init__(self, path_to_file: str, tangro_modul, wordlist_filter: pd.DataFrame = None, create_test_train_set=0.2):
        intialize_data_structs(
            self, path_to_file, tangro_modul)
        if wordlist_filter is not None:
            filter_by_wordlist(self, wordlist_filter)
        if create_test_train_set < 1.:
            create_test_train_dict(self, create_test_train_set)


def create_test_train_dict(self, test_ratio=0.2):
    for attr in dir(self):
        if not attr.startswith('__') and not callable(getattr(self, attr)):
            key_col = sd_key_val_pair.get(attr)
            if test_ratio != 0.:
                train_set, test_set = split_train_test_by_id(
                    getattr(self, attr), test_ratio, key_col)
            elif test_ratio == 0.:
                train_set = getattr(self, attr)
                test_set = None
            self.__train_dict[attr] = train_set
            self.__test_dict[attr] = test_set


def filter_by_wordlist(self, wordlist: pd.DataFrame):
    for attr in dir(self):
        if not attr.startswith('__') and not callable(getattr(self, attr)):
            wl = wordlist.drop_duplicates(subset=['DOC_NUMBER'])
            dataset = getattr(self, attr)
            filtered_dataset = dataset[dataset[
                sd_key_val_pair.get(attr)].isin(wl['DOC_NUMBER'])]
            setattr(self, attr, filtered_dataset)


class WordList:
    def __init__(self, path_to_file=path_to_wordlists):
        intialize_wl_dataframe(self, path_to_file)


def intialize_wl_dataframe(self, path_to_file):
    self.wordlist = load_wordlists(path_to_file)
    self.filtered_wl


def intialize_data_structs(self, path_to_file, tangro_modul):
    # Member Variablen die nicht gefiltert  werden sollen, fangen mit __ an
    self.__tangro_preproc = tangro_preproc[tangro_modul]
    self.headers = load_sap_data(
        sd_headers, self.__tangro_preproc, path_to_file)
    self.items = load_sap_data(sd_items, self.__tangro_preproc, path_to_file)
    self.schedules = load_sap_data(
        sd_schedules, self.__tangro_preproc, path_to_file)
    self.partners = load_sap_data(
        sd_partners, self.__tangro_preproc, path_to_file)
    self.cuins = load_sap_data(sd_cuins, self.__tangro_preproc, path_to_file)
    self.cucfg = load_sap_data(sd_cucfg, self.__tangro_preproc, path_to_file)
    self.cuval = load_sap_data(sd_cuval, self.__tangro_preproc, path_to_file)
    self.cuprt = load_sap_data(sd_cuprt, self.__tangro_preproc, path_to_file)
    self.__train_dict = {}
    self.__test_dict = {}


def load_wordlists(PATH_TO_FILES=path_to_wordlists, subset: list[docs] = None):
    try:
        if subset == None:
            all_files = glob.glob(os.path.join(PATH_TO_FILES, "*.csv"))
        else:
            all_files = [os.path.join(PATH_TO_FILES, doc + '.csv')
                         for doc in subset]
        path_length = len(PATH_TO_FILES) + 1
        if len(all_files) == 1:
            single_file = True
        else:
            single_file = False

        if not single_file:
            total_file = pd.concat((pd.read_csv(f, sep=';', engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE).
                                    assign(DOC_NUMBER=lambda x: docno(f, path_length), ATTACH_NO=lambda x: attachno(f)) for f in all_files))
        else:
            try:
                total_file = [pd.read_csv(f, sep=';', engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE).assign(
                    DOC_NUMBER=lambda x: docno(f, path_length), ATTACH_NO=lambda x: attachno(f)) for f in all_files]
                total_file = total_file[0]
            except FileNotFoundError as e:
                ext_f = all_files[0].replace('_1.csv', '_Ext*.csv')
                all_files = glob.glob(ext_f)
                total_file = [pd.read_csv(f, sep=';', engine='python', on_bad_lines='warn', quoting=csv.QUOTE_NONE).assign(
                    DOC_NUMBER=lambda x: docno(f, path_length), ATTACH_NO=lambda x: attachno(f)) for f in all_files]
                total_file = total_file[0]
    except pd.errors.ParserError:
        print("Beim Einlesen der Gesamtwortliste ist ein Parser-Fehler aufgetreten: " + sys.exc_info())
    return total_file


def attachno(f: str):
    try:
        attachnumb = f.split('_')[1][:-4]
        if re.search('[a-zA-Z]', attachnumb):
            attachnumb = '1'
    except:
        attachnumb = None
    return attachnumb


def docno(f: str, path_length: int):
    doc_number = int(f.split('_')[0][path_length: path_length + 10])
    return doc_number


def load_sap_data(file_name, tangro_modul, path_to_data=path_to_sap_data):
    name_pattern = file_name.split('.')[0] + "*.csv"
    all_files = glob.glob(os.path.join(path_to_data, name_pattern))
    if len(all_files) == 0:
        print("Dateien nicht vorhanden")  # Raise Error
    else:
        concat_sap_file = pd.concat(
            (get_sap_data(f, tangro_modul) for f in all_files))
    additional_processing(concat_sap_file)
    return concat_sap_file


def get_sap_data(path_to_file, tangro_modul):
    file = preproc_sap_data(path_to_file, tangro_modul)
    return file
