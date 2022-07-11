from sklearn.preprocessing import OneHotEncoder
import numpy as np
import cust_types as cst
from gensim.models import Word2Vec
""" import tensorflow as tf


from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, Embedding, GlobalAveragePooling1D
from tensorflow.keras.layers.experimental.preprocessing import TextVectorization """


def hot_encode(wordlist: cst.wordlist) -> np.ndarray:
    if not wordlist.empty:
        cat_encoder = OneHotEncoder()
        worte = wordlist[['WORT']]
        worte_1hot = cat_encoder.fit_transform(worte)
        return worte_1hot
    else:
        return np.zeros(1)

def word2vec(sentences: list[list[str]],window_size = 5,min_count = 1, size = 100):
    result = Word2Vec(sentences, window_size = window_size ,min_count = min_count,size = size )
    return result

