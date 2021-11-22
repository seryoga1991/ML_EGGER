import evaluator_class as ev
import cust_types as cst
import config as cfg
import global_config as glb

if __name__ == '__main__':
    eval = ev.Evaluator()
    eval.classify_spam(wordlist_class=eval.wordlist,
                       tangro_modul=cfg.tangro_om,
                       classifier_method=cst.ClassifierMethod.spam,
                       filter_method=cst.FilterMethod.debitor,
                       classified_dict={})
    eval.move(cst.ClassifierMethod.spam)
