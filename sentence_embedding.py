from sklearn.manifold import TSNE
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.cluster import AgglomerativeClustering
import pandas as pd
import re
import string
import unicodedata


def get_all_customer_message():
    customer_message_list = []
    for month in range(1, 8):
        file_path = "data/all_chat_fb_{month}.csv"
        file_path = file_path.format(month=str(month))
        df = pd.read_csv(file_path)
        user_messages = df["user_message"].to_list()
        user_messages = [x for x in user_messages if str(x) not in ["nan", "user"] and x is not None]
        customer_message_list += user_messages
        a = 0

    return customer_message_list


get_all_customer_message()
