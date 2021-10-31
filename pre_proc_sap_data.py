import pandas as pd
import glob
import os
import sys
import numpy as np
from data_manipulation_helper import swap_header_by_row 

def preproc_sap_data(path_to_file, tangro_modul):
    try:
        file = pd.read_csv(path_to_file, sep=',')
        return file
    except pd.errors.ParserError:
        file = __handle_bad_lines(path_to_file, tangro_modul)
        file = swap_header_by_row(file)
        return file
    except OSError as err:
        print("Ungültiger Pfad.")
    return file


def findOccurrences(s, ch):
    return [i for i, letter in enumerate(s) if letter == ch]


def replace_char_at_index(org_str, index, replacement):
    new_str = org_str
    if index < len(org_str):
        new_str = org_str[0:index] + replacement + org_str[index + 1:]
    return new_str


def __get_column_numb_to_assign(path_to_file, string, tangro_modul):
    column_pos = __determine_col_pos_from_header(
        path_to_file, string, tangro_modul)
    return column_pos


def __get_total_columnnumber(string):
    x = string.split(',')
    return len(x)


def __determine_col_pos_from_header(path_to_file, string, tangro_list):
    for item in tangro_list:
        if item[0] in path_to_file:
            x = string.split(',')
            column_pos = x.index(item[1]) + 1
            break
        elif item[0] == tangro_list[-1][0] and item[0] not in path_to_file:
            print("Datei nicht in der Übersichtsliste der SAP Dateien.")
    return column_pos


# Alles soweit verarbeitet --> entferne die Hilfsspalten
def __add_auxillary_cols(file,):
    new_column = 'A1'
    file.at[0, 0] = file.at[0, 0] + ',' + new_column


def __handle_bad_lines(path_to_file, tangro_modul):
    file = pd.read_csv(path_to_file, header=None, sep='\n')
    copied_file = file.copy()
    __add_auxillary_cols(copied_file)
    copied_file = copied_file[0].str.split(',', expand=True)
    new_header = copied_file.iloc[0]
    copied_file = copied_file[1:]
    copied_file.columns = new_header
    list_of_bad_indices = copied_file[copied_file['A1'].notnull()].index
    assigned_column_numb = __get_column_numb_to_assign(
        path_to_file, file.iloc[0].at[0], tangro_modul)
    total_column_number_original = __get_total_columnnumber(file.iloc[0].at[0])
    commas_to_expect = total_column_number_original - 1
    for index in list_of_bad_indices:
        number_of_commas = file.loc[index].at[0].count(',')
        separators_to_delete = number_of_commas - commas_to_expect

        i = 1
        while i <= separators_to_delete:
            list_of_comma_occurences = findOccurrences(
                file.loc[index].at[0], ',')
            new_string = replace_char_at_index(
                file.loc[index].at[0], list_of_comma_occurences[assigned_column_numb - 1], '')
            file.loc[index].at[0] = new_string
            i = i + 1

    processed_file = file[0].str.split(',', expand=True)
    return processed_file


def additional_processing(file):
    if 'MATERIAL' in file.columns:
        dataType_Obj = file.dtypes['MATERIAL']
        if dataType_Obj == np.float64 or dataType_Obj == np.int64:
            file['MATERIAL'] = file['MATERIAL'].apply(lambda x:np.format_float_positional(x,trim = '-'))
    else: 
        pass