import pandas as pd
import config as cfg
import global_config as glb
import cust_types as cst
from utils import printProgressBar
from utils import Parallelize_Task


def transpose_list(liste):
    liste = list(map(list, zip(*liste)))
    return liste


def flatten_list(some_list: list):
    return [item for sublist in some_list for item in sublist]

#TODO: Refactorn? 
def binomial_equipartition(series: pd.Series, parts_count: int) -> list[pd.Series]:

    remainder = len(series) % parts_count
    slice_size = int((len(series) - remainder)/parts_count)
    sliced_series_list = [[series[slice_size*i:slice_size *
                                  i + slice_size], series[slice_size*i:]]for i in range(parts_count - 1)]
    sliced_series_list.append(
        [series[slice_size*(parts_count - 1):], series[slice_size*(parts_count - 1):], pd.Series([]), pd.Series([])])
    mean_val = len(sliced_series_list[int(parts_count/2 - 1)][1])
    if slice_size > cfg.cores_to_use and cfg.cores_to_use > 2:
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


def filter_sdDicts(object: dict, filter_set: cfg.sd_key_val_pair.keys, filter_column, filter_value, wordlist=None):

    conv_filter_val = int(filter_value)
    if filter_set not in list(cfg.sd_key_val_pair.keys()):
        raise ValueError(
            "Die Parameter filter_set und object müssen in %r liegen" % cfg.sd_key_val_pair.keys())
    else:
        new_dict = {}
        Set = object.get(filter_set)
        Set = Set[Set[filter_column] == conv_filter_val]
        new_dict[filter_set] = Set
        if wordlist is not None:
            filtered_wordlist = wordlist[wordlist['DOC_NUMBER'].isin(
                Set[cfg.sd_key_val_pair.get(filter_set)])]
        for key, item in object.items():
            if key is not filter_set:
                filtered_item = item[item[cfg.sd_key_val_pair.get(key)].isin(
                    Set[cfg.sd_key_val_pair.get(filter_set)])]
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

        if item[1][1] > cfg.left_max_dpi:
            normalized_left[idx][1][0] = int(item[1][0]/item[1][1])
            normalized_left[idx][1][1] = cfg.left_max_dpi
    for idx, item in enumerate(normalized_up):
        if item[1][1] > cfg.up_max_dpi:
            normalized_up[idx][1][0] = int(item[1][0]/item[1][1])
            normalized_up[idx][1][1] = cfg.up_max_dpi

    return normalized_left, normalized_up


def filter_wl_by_left_coord(wordlist: cst.wordlist, normalized_left):
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


def sort_wl_by_coord(wordlist: cst.wordlist) -> cst.wordlist:
    sorted_wl = wordlist.sort_values(by=['SEITE', 'LINKS', 'OBEN'])
    return sorted_wl

def get_central_word(df: pd.DataFrame, tolerance_row,tolerance_col):
    central_word = df.iloc[0]
    central_word['RECHTS'] = central_word['RECHTS'] + tolerance_row
    central_word['LINKS'] =  central_word['LINKS'] - tolerance_row
    central_word['OBEN'] =  central_word['OBEN'] - tolerance_col
    central_word['UNTEN'] =  central_word['UNTEN'] + tolerance_col 
    return central_word   


#TODO Refactorn
def _group_wordlocks(tolerance_row = 50,tolerance_col = 60, wordlist: pd.DataFrame = pd.DataFrame()):
    wordlist = wordlist.assign(WORDBLOCK_ID = lambda x: 0)
    columm_loc = wordlist.columns.get_loc('WORDBLOCK_ID') 
    unique_docids = wordlist['DOC_NUMBER'].unique()
    for idx,doc in enumerate(unique_docids):
        wl = wordlist[wordlist['DOC_NUMBER'] == doc]
        unique_filenames = wl['FILE_NAME'].unique()
        for filename in unique_filenames:        
            counter = 0
            end_of_wordlist = False
            wl = wordlist[wordlist['FILE_NAME'] == filename]
            while( not end_of_wordlist):
                df = wl[wl['WORDBLOCK_ID'] == 0]
                if not df.empty:
                    central_word = get_central_word(df,tolerance_row,tolerance_col)
                    neighboring_words = wl[(((wl['LINKS'].between(central_word['LINKS'],central_word['RECHTS'])) & (wl['OBEN'].between(central_word['OBEN'],central_word['UNTEN'])))
                                        | ((wl['RECHTS'].between(central_word['LINKS'],central_word['RECHTS'])) & (wl['OBEN'].between(central_word['OBEN'],central_word['UNTEN'])))) 
                                        & (wl['SEITE'] == central_word['SEITE'])                                        
                                        ]
                    index = neighboring_words.index
                    df = neighboring_words[neighboring_words['WORDBLOCK_ID'] != 0]
                    if df.empty:
                        counter = counter + 1
                        id = counter 
                    else:
                        id = df.iloc[0]['WORDBLOCK_ID']  
                    wl.iloc[index[neighboring_words['WORDBLOCK_ID'] == 0].to_list(), columm_loc] = id 
                else:
                    end_of_wordlist = True
        wordlist.loc[wordlist['DOC_NUMBER'] == doc,'WORDBLOCK_ID'] = wl['WORDBLOCK_ID']
        if not glb.apply_multithread:
            percent = (idx + 1 ) / len(unique_docids)
            printProgressBar(percentage=percent) 
        
    return wordlist['WORDBLOCK_ID'] 
            

group_wordlocks = Parallelize_Task(_group_wordlocks,task_classifier=cst.parallelize_tasks.wordlist_task,slice_size=100)
    

    

def filter_sort_wl_by_coord(wordlist: pd.DataFrame, left_margins: cst.margin_setup, up_margins: cst.margin_setup):
    sorted_left, sorted_up = sort_margins(left_margins,  up_margins)
    normalized_left, normalized_up = normalize_margins(sorted_left, sorted_up)
    filtered_wl_left = filter_wl_by_left_coord(wordlist, normalized_left)
    filtered_wl_up = filter_wl_by_up_coord(filtered_wl_left, normalized_up)
    sorted_filtered_wl = sort_wl_by_coord(filtered_wl_up)
    return sorted_filtered_wl


""" def small_gap_wl_clustering(wordlist: pd.DataFrame, row_tolerance = 10, col_tolerance = 3):
    wl = sort_wl_by_coord(wordlist)
    end_of_list = False
    df = wl
    while not end_of_list:
        pass """