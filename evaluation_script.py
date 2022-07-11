import pandas
import evaluator_class as ev
import cust_types as cst
import config as cfg
import global_config as glb
from wordlist_class import process_wordlist
import data_loader as dtl
if __name__ == '__main__':
    eval = ev.Evaluator()

    """     eval = ev.Evaluator()
        wordlist = eval.wordlist.get_wordlist()
        wordlists = ev.exclude_files(modul = glb.tangro_om, multi_proc = True  , wordlist = wordlist)
        total_new_wl = pandas.concat((ev.exclude_non_distinct_files(f) for f in wordlists))
        df = total_new_wl.groupby(['DOC_NUMBER','FILE_NAME']).agg(['first']).reset_index()
        print(len(ev.are_docs_distinct(df)))
        final_wordlist =  process_wordlist(total_new_wl)
        final_wordlist.to_csv("total_wordlist_grouped_50_50_all_distinct.csv",index = False) """

