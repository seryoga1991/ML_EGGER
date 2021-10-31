import os
import sys
import pandas as pd
import numpy as np
import glob
from zlib import crc32
from config import *


def transpose_list(liste):
    liste = list(map(list, zip(*liste)))
    return liste


def equipartition(series: pd.Series, parts_count: int) -> [pd.Series]:
    remainder = len(series) % parts_count
    slice_size = int((len(series) - remainder)/parts_count)
    sliced_series_list = [series[slice_size*i:slice_size *
                                 i + slice_size] for i in range(parts_count - 1)]
    sliced_series_list.append(series[slice_size*(parts_count - 1):])
    return sliced_series_list


def swap_header_by_row(dataset, row=0):
    dataset_new = dataset.copy()
    dataset_new.columns = dataset.iloc[row]
    dataset_new.drop([row])
    return dataset_new


def test_set_check(identifier, test_ratio):
    return crc32(np.int64(identifier)) & 0xffffffff < test_ratio * 2**32


def split_train_test_by_id(data, test_ratio, id_column):
    ids = data[id_column]
    in_test_set = ids.apply(lambda id_: test_set_check(id_, test_ratio))
    return data.loc[~in_test_set], data.loc[in_test_set]


""" def create_dict_from_trained_sets(object):
    trainedSet_dict = {}
    for attr in dir(object):
        if attr.startswith('train_') and not callable(getattr(object, attr)):
            new_name = attr[6:]
            trainedSet_dict[new_name] = getattr(object, attr)
    return trainedSet_dict """


def filter_sdDicts(object, filter_set, filter_column, filter_value, wordlist=None):

    if filter_set not in list(sd_key_val_pair.keys()):
        raise ValueError(
            "Die Parameter filter_set und object müssen in %r liegen" % sd_key_val_pair.keys())
    else:
        new_dict = {}
        Set = object.get(filter_set)
        Set = Set[Set[filter_column] == filter_value]
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
