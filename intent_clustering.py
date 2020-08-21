from sklearn.feature_extraction.text import TfidfVectorizer
from gensim.models import Word2Vec
from sklearn.manifold import TSNE
# from MulticoreTSNE import MulticoreTSNE as TSNE

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.cluster import AgglomerativeClustering
import pandas as pd
import re
import string
import unicodedata
import process_user_message
import numpy as np


def calculate_silhouette_score(tsne_input_path=None):
    if tsne_input_path:
        tsne_vectors = np.loadtxt(tsne_input_path, dtype=float)
    else:
        tsne_vectors = np.loadtxt('tsne_vectors.txt', dtype=float)
    scores = []
    for k in range(2, 20):
        kmeans = KMeans(n_clusters=k, random_state=0)
        kmeans = kmeans.fit(tsne_vectors)
        labels = kmeans.labels_
        score = silhouette_score(tsne_vectors, labels)
        inertia = kmeans.inertia_
        scores.append((k, score, inertia))

    scores_df = pd.DataFrame(scores, columns=['k', 'silhouette_score', 'inertia'])
    scores_df.to_csv("data/elbow_n_silhouette_scores.csv", index=False)


def sentence_embedding(customer_message_path, tsne_output_path):
    if customer_message_path:
        customer_message_df = pd.read_csv(customer_message_path)
        customer_message = customer_message_df["sentence"].drop_duplicates().to_list()
    else:
        customer_message = process_user_message.get_processed_customer_message()

    sentences = [x.split() for x in customer_message]
    model = Word2Vec(sentences, size=20, window=5, min_count=1, workers=4)

    vectors_list = []
    for sent_index, sent_value in enumerate(sentences):
        sentence_length = len(sent_value)
        vector = 0
        for word_index, word_value in enumerate(sent_value):
            vector += model.wv[word_value]
        vector = vector / sentence_length
        vector = vector.tolist()
        vectors_list.append(vector)

    tsne = TSNE(n_components=2, verbose=2)
    tsne_vectors = tsne.fit_transform(vectors_list)
    if tsne_output_path:
        np.savetxt(tsne_output_path, tsne_vectors, fmt='%f')
    else:
        np.savetxt('tsne_vectors.txt', tsne_vectors, fmt='%f')

    return tsne_vectors


def k_mean_clustering(number_of_clusters, customer_message_path=None, tsne_input_path=None):
    if tsne_input_path:
        tsne_vectors = np.loadtxt(tsne_input_path, dtype=float)
    else:
        tsne_vectors = np.loadtxt('tsne_vectors.txt', dtype=float)
    vectors_df = pd.DataFrame(data=tsne_vectors, columns=["x", "y"])

    if customer_message_path:
        customer_message_df = pd.read_csv(customer_message_path)
        customer_message = customer_message_df["sentence"].drop_duplicates().to_list()
    else:
        customer_message = process_user_message.get_processed_customer_message()

    kmeans = KMeans(n_clusters=number_of_clusters, random_state=0).fit(tsne_vectors)

    data_tuples = list(zip(customer_message, kmeans.labels_))
    clustering_df = pd.DataFrame(data_tuples, columns=['sentence', 'kmeans_cluster'])
    clustering_df = pd.merge(clustering_df, vectors_df, right_index=True, left_index=True)
    clustering_df.to_csv("data/all_cluster.csv", index=False)
    return clustering_df


def gaussian_mixture_clustering():
    tsne_vectors = np.loadtxt('tsne_vectors.txt', dtype=float)
    vectors_df = pd.DataFrame(data=tsne_vectors, columns=["x", "y"])

    customer_message = process_user_message.get_processed_customer_message()
    gm = GaussianMixture(n_components=6, n_init=10, covariance_type="spherical").fit(tsne_vectors)

    data_tuples = list(zip(customer_message, gm.predict(tsne_vectors)))
    clustering_df = pd.DataFrame(data_tuples, columns=['sentence', 'gm_cluster'])
    clustering_df = pd.merge(clustering_df, vectors_df, right_index=True, left_index=True)

    return clustering_df

# sentence_embedding("data/cluster_8.csv", "data/cluster_8/tsne_vectors.txt")
# k_mean_clustering("data/cluster_8.csv", "data/cluster_8/tsne_vectors.txt")
k_mean_clustering(6)
# calculate_silhouette_score("tsne_vectors.txt")