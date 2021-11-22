import cust_types as cst
import config as cfg
import numpy as np
import pandas as pd
from zlib import crc32
from data_loader import load_sap_data


def test_set_check(identifier, test_ratio):
    return crc32(np.int64(identifier)) & 0xffffffff < test_ratio * 2**32


def split_train_test_by_id(data, test_ratio, id_column):
    ids = data[id_column]
    in_test_set = ids.apply(lambda id_: test_set_check(id_, test_ratio))
    return data.loc[~in_test_set], data.loc[in_test_set]


def create_test_train_dict(self, test_ratio=0.2):
    for attr in dir(self):
        if not attr.startswith('_') and not callable(getattr(self, attr)):
            key_col = cfg.sd_key_val_pair.get(attr)
            if test_ratio != 0.:
                train_set, test_set = split_train_test_by_id(
                    getattr(self, attr), test_ratio, key_col)
            elif test_ratio == 0.:
                train_set = getattr(self, attr)
                test_set = None
            self._train_dict[attr] = train_set
            self._test_dict[attr] = test_set


def filter_by_wordlist(self, wordlist: cst.wordlist):
    for attr in dir(self):
        if not attr.startswith('_') and not callable(getattr(self, attr)):
            wl = wordlist.drop_duplicates(subset=['DOC_NUMBER'])
            dataset = getattr(self, attr)
            filtered_dataset = dataset[dataset[
                cfg.sd_key_val_pair.get(attr)].isin(wl['DOC_NUMBER'])]
            setattr(self, attr, filtered_dataset)


def intialize_data_structs(self, path_to_sap_data, tangro_modul):
    # Member Variablen die nicht gefiltert  werden sollen, fangen mit __ an
    self.__tangro_preproc = cfg.tangro_preproc[tangro_modul]
    self.headers = load_sap_data(
        cfg.sd_headers, self.__tangro_preproc, path_to_sap_data)
    self.items = load_sap_data(
        cfg.sd_items, self.__tangro_preproc, path_to_sap_data)
    self.schedules = load_sap_data(
        cfg.sd_schedules, self.__tangro_preproc, path_to_sap_data)
    self.partners = load_sap_data(
        cfg.sd_partners, self.__tangro_preproc, path_to_sap_data)
    self.cuins = load_sap_data(
        cfg.sd_cuins, self.__tangro_preproc, path_to_sap_data)
    self.cucfg = load_sap_data(
        cfg.sd_cucfg, self.__tangro_preproc, path_to_sap_data)
    self.cuval = load_sap_data(
        cfg.sd_cuval, self.__tangro_preproc, path_to_sap_data)
    self.cuprt = load_sap_data(
        cfg.sd_cuprt, self.__tangro_preproc, path_to_sap_data)
    self._train_dict = {}
    self._test_dict = {}


class SapData:
    def __init__(self, path_to_sap_data: str = cfg.path_to_sap_data, tangro_modul=cfg.tangro_om, wordlist_filter: pd.DataFrame = None, create_test_train_set=0.2):
        intialize_data_structs(
            self, path_to_sap_data, tangro_modul)
        if wordlist_filter is not None:
            filter_by_wordlist(self, wordlist_filter)
        if create_test_train_set < 1.:
            create_test_train_dict(self, create_test_train_set)

    def set_test_train_dict(self, ratio: float = 0.2):
        create_test_train_dict(self, ratio)

    def get_train_dict(self):
        return self._train_dict
