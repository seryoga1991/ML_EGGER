import data_loader as dtl
import config as cfg
import cust_types as cst
import pandas as pd
import texthero as hero
from texthero import preprocessing
import data_manipulation_helper as dtm
from functools import reduce
from utils import Parallelize_Task

class WordList:
    def __init__(self, path_to_wordlists=cfg.path_to_wordlists, subset=[]):
        intialize_wl_dataframe(self, path_to_wordlists, subset)

    def get_wordlist(self, doc_numbers: cst.docs_list = [], words: list[str] = [], files_list: cst.docs_list = []) -> cst.wordlist:
        filtered_wl = self.wordlist
        if not self.filtered_wl.empty:
            return self.filtered_wl
        if doc_numbers != []:
            filtered_wl = self.wordlist[self.wordlist['DOC_NUMBER'].isin(
                doc_numbers)]
        if words != []:
            filtered_wl = self.wordlist[self.wordlist['WORT'].isin(words)]
        if files_list != []:
            filtered_wl = self.wordlist[self.wordlist['FILE_NAME'].isin(
                files_list)]
        return filtered_wl

    def set_filter(self, doc_numbers: cst.docs_list = [], wort: str = '', files_list: cst.docs_list = []):
        if doc_numbers != []:
            self.filtered_wl = self.wordlist[['DOC_NUMBER'].isin(doc_numbers)]
        if wort != '':
            self.filtered_wl = self.filtered_wl[['WORT'] == wort]
        if files_list != []:
            self.filtered_wl = self.filtered_wl[['FILE_NAME'].isin(files_list)]

    def get_unique_docids(self, wordlist: pd.DataFrame):
        unique_docs = wordlist['DOC_NUMBER'].unique().tolist()
        return unique_docs

    def reset_filter(self):
        self.filtered_wl = pd.DataFrame([])

   
    
    def clean_wordlist(self,group_words:bool = True):
        self.wordlist = process_wordlist(self.get_wordlist(),group_words = group_words)



def intialize_wl_dataframe(self, path_to_file, subset):
    self.wordlist = dtl.load_wordlists(path_to_file, subset)
    self.filtered_wl = pd.DataFrame([])


def process_wordlist(wordlist:pd.DataFrame,group_words:bool = True):
    pd.set_option('mode.chained_assignment', None)
    wl = wordlist
    wl = small_gap_clustering(wl)
    multi_proc = True
    if group_words:
        result = dtm.group_wordlocks(multi_proc = multi_proc,tolerance_row=50, tolerance_col=50, wordlist = wl)  
        if multi_proc:
            result = pd.concat((f for f in result))
        wl['CLEANED_WORDS'] = result
    wl = do_custom_cleaning(wl)
    custom_clean_pipeline = [preprocessing.fillna, 
                             preprocessing.lowercase,
                             preprocessing.remove_whitespace,
                             preprocessing.remove_diacritics]
    wl['CLEANED_WORDS'] = hero.clean(wl['WORT'],custom_clean_pipeline) 
    return wl

def do_custom_cleaning(wordlist: pd.DataFrame):
    pd.set_option('mode.chained_assignment', None)
    wordlist['LENGTH'] = wordlist['WORT'].str.len()
    filtered_wordlist = wordlist[(wordlist['length'] > 1) | (wordlist['WORT'].str.isnumeric())]
    custom_punctuation = '!"#$%&()\'*+;<=>?[\]^_`{|}~'
    filtered_wordlist['WORT'] = filtered_wordlist['WORT'].str.replace(rf"([{custom_punctuation}])+", '', regex=True)
    return filtered_wordlist


def small_gap_clustering(wl:pd.DataFrame):
    multi_proc = True
    result = dtm.group_wordlocks(multi_proc = multi_proc ,tolerance_row=10, tolerance_col=3, wordlist = wl)
    
    if multi_proc:
        result = pd.concat((f for f in result))
    wl['WORDBLOCK_ID'] = result
    wl = wl.sort_values(by=['DOC_NUMBER', 'FILE_NAME','SEITE', 'OBEN','LINKS'])
    multi_proc = False
    result = aggregate_wordblock(multi_proc = multi_proc , wordlist= wl)
    if multi_proc:
        result = pd.concat((f for f in result))
    return result
    

def conc_string(series):
    return reduce(lambda x ,y: x + y , series)



def _aggregate_wordblock(multi_proc, wordlist: pd.DataFrame):
    dict_for_aggregation = {'DOC_NUMBER': 'first',
                            'SEITE':'first',
                            'LINKS':'min',
                            'RECHTS':'max',
                            'OBEN':'first',
                            'UNTEN':'first',
                            'WORT':conc_string,
                            'FILE_NAME':'first',
                            'ATTACH_NO':'first'}

    wl = wordlist 
    wl = wl.dropna()
    wl = wl.groupby(['DOC_NUMBER','WORDBLOCK_ID']).agg(dict_for_aggregation)
    wl = wl.drop(columns=['DOC_NUMBER'])
    wl = wl.reset_index()
    wl = wl.drop(columns=['WORDBLOCK_ID'])
    return wl 

aggregate_wordblock = Parallelize_Task(_aggregate_wordblock,cst.parallelize_tasks.wordlist_task,100)