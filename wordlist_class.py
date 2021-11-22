import data_loader as dtl
import config as cfg
import cust_types as cst
import pandas as pd


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

    def reset_filter():
        self.filtered_wl = pd.DataFrame([])


def intialize_wl_dataframe(self, path_to_file, subset):
    self.wordlist = dtl.load_wordlists(path_to_file, subset)
    self.filtered_wl = pd.DataFrame([])
