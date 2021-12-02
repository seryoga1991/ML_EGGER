import pandas as pd
import config as cfg
import cust_types as cst


def transpose_list(liste):
    liste = list(map(list, zip(*liste)))
    return liste


def flatten_list(some_list: list):
    return [item for sublist in some_list for item in sublist]


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
        if item[1][1] > up_max_dpi:
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


def sort_wl_by_neighbors(wordlist: pd.DataFrame):
    sorted_wl = wordlist.sort_values(by=['SEITE', 'LINKS', 'OBEN'])
    return sorted_wl


def filter_sort_wl_by_coord(wordlist: pd.DataFrame, left_margins: cst.margin_setup, up_margins: cst.margin_setup):
    sorted_left, sorted_up = sort_margins(left_margins,  up_margins)
    normalized_left, normalized_up = normalize_margins(sorted_left, sorted_up)
    filtered_wl_left = filter_wl_by_left_coord(wordlist, normalized_left)
    filtered_wl_up = filter_wl_by_up_coord(filtered_wl_left, normalized_up)
    sorted_filtered_wl = sort_wl_by_coord(filtered_wl_up)
    return sorted_filtered_wl
