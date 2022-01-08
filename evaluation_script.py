import pandas
import evaluator_class as ev
import cust_types as cst
import config as cfg
import global_config as glb
if __name__ == '__main__':
    wordlist = pandas.read_csv('total_wordlist_grouped_50_60.csv')
    wordlists = ev.exclude_files(modul = glb.tangro_om, wordlist = wordlist)
    total_new_wl = pandas.concat((ev.exclude_non_distinct_files(f) for f in wordlists))
    print(len(ev.are_docs_distinct(total_new_wl)))
    total_new_wl.to_csv("total_wordlist_grouped_50_60_all_distinct.csv",index = False)
    """     eval.classify_spam_by_modul_specs()
        eval.move(cst.ClassifierMethod.spam) """
