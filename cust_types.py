import pandas as pd
import numpy as np
from enum import Enum

pass_docs = None
kill_all_docs = None
page = int
first_margin = int
second_margin = int
margin_setup = list[list[page, list[first_margin, second_margin]]]
docs = str
partner = str
files = str
files_list = list[files]
docs_list = list[docs]
partner_list = list[partner]
file_name = str
aggregated_array = np.ndarray
two_doc_aggregation = tuple[list[docs,
                                 docs], list[aggregated_array, aggregated_array]]
doc_doc_correlation = list[tuple[tuple[docs,
                                       docs], np.float64]]
sap_dataclass_train_dict = str
train_dict = dict[sap_dataclass_train_dict]

wordlist = pd.DataFrame({
    'SEITE': pd.Series(dtype='int'),
    'LINKS': pd.Series(dtype='int'),
    'OBEN': pd.Series(dtype='int'),
    'RECHTS': pd.Series(dtype='int'),
    'UNTEN': pd.Series(dtype='int'),
    'WORT': pd.Series(dtype='str'),
    'DOC_NUMBER': pd.Series(dtype='str'),
    'ATTACH_NO': pd.Series(dtype='str'),
    'FILE_NAME': pd.Series(dtype='str')})


class ClassifierMethod(Enum):
    spam = 1


class FilterMethod(Enum):
    debitor = 1
