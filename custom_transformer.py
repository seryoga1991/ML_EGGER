from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import numpy as np


def hot_encode(wordlist: pd.DataFrame) -> np.ndarray:
    if not wordlist.empty:
        cat_encoder = OneHotEncoder()
        worte = wordlist[['WORT']]
        worte_1hot = cat_encoder.fit_transform(worte)
        return worte_1hot
    else:
        return np.zeros(1)
