import glob
import os
import cust_types as cst 
from numba.core.decorators import njit
from numba.np.ufunc import parallel
import config as cfg
import global_config as glb
import concurrent.futures
import functools
import pandas as pd
from numba import jit 
from tqdm import tqdm


class InvalidParalleizeTask(Exception):
    pass


def clean_tmp_data():
    all_files = glob.glob(os.path.join(
        cfg.temporary_save_dir, "*.csv"))
    for f in all_files:
        os.remove(f)



def printProgressBar(percentage, prefix='', suffix='', length=100):  # für Single Threading
    decimals = 1
    fill = '█'
    printEnd = "\r"
    percent = ("{0:." + str(decimals) + "f}").format(100 * percentage)
    filledLength = int(length * percentage)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if percentage == 100.0:
        print()


def equipartition_load_for_task(doc_series: pd.Series, slice_size):
    series = doc_series.unique()
    remainder = len(series) % slice_size
    slice = int((len(series) - remainder)/slice_size)
    sliced_series_list = [series[slice*i:slice *i + slice] for i in range(slice_size - 1)]
    sliced_series_list.append(series[slice*(slice_size - 1):])
    return sliced_series_list



class Parallelize_Task:
    def __init__(self, func, task_classifier: cst.parallelize_tasks , slice_size = 100 ):
        self.func = func
        self.task_classifier = task_classifier
        self.slice_size = slice_size
    
    def __call__(self, *args, **kwargs):
        pd.set_option('mode.chained_assignment', None)
        if glb.apply_multithread:
            if cfg.cores_to_use in cfg.core_check:
                if self.task_classifier == cst.parallelize_tasks.wordlist_task:
                    key_arg = 'wordlist'
                elif self.task_classifier == cst.parallelize_tasks.doc_series_task:
                    key_arg = 'doc_series'
                else: 
                    raise InvalidParalleizeTask(f'Parallelisierung für {self.task_classifier} existiert noch nicht!')
                object  = kwargs[key_arg]
                remaining_args = [kwargs[key] for key in kwargs.keys() if key != key_arg]
                frozen_func = functools.partial(self.func, *remaining_args)                       
                if self.task_classifier == cst.parallelize_tasks.wordlist_task:
                    sliced_series_list = equipartition_load_for_task(object['DOC_NUMBER'],self.slice_size)
                    work_with = [object[object['DOC_NUMBER'].isin(sliced_series_list[i])] for i in range(len(sliced_series_list))]
                elif self.task_classifier == cst.parallelize_tasks.doc_series_task:
                    work_with = equipartition_load_for_task(object,self.slice_size)
                
                with concurrent.futures.ProcessPoolExecutor(max_workers=cfg.cores_to_use) as executor:
                    result = list(tqdm( executor.map(frozen_func, work_with ),total = len(work_with)))
                return result
            else:
                raise cfg.InvalidCoreCount( f'Nicht vorgesehene CPU Kernangabe: {cfg.cpu_count} liegen vor und {cfg.cores_to_use} zugewiesen.')
        else:
            result = self.func(*args,**kwargs)
            return result