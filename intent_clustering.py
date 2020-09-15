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


def calculate_silhouette_score(tsne_input_path=None, output=None):
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
    if output:
        scores_df.to_csv(output, index=False)
    else:
        scores_df.to_csv("data/elbow_n_silhouette_scores.csv", index=False)


def split_to_bigram(word_list):
    bigram_wordlist = []
    for index, word in enumerate(word_list):
        if index < len(word_list) - 1:
            bigram_word = word + "_" + word_list[index + 1]
            bigram_wordlist.append(bigram_word)
        if len(word_list) == 1:
            bigram_wordlist.append(word)
    return bigram_wordlist

def sentence_embedding(no_cluster, customer_message_path, tsne_output_path, customer_message_output_path):
    if customer_message_path:
        customer_message_df = pd.read_csv(customer_message_path)
        customer_message_df = customer_message_df[customer_message_df["kmeans_cluster"] == no_cluster]
        customer_message_df.to_csv(customer_message_output_path, index=False)
        customer_message = customer_message_df["sentence"].to_list()
    else:
        customer_message = process_user_message.get_processed_customer_message()

    sentences = [str(x).split() for x in customer_message["message_group"].to_list()]
    bigrams = []
    for item in sentences:
        bigrams.append(split_to_bigram(item))

    sentences = bigrams
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


def k_mean_clustering(number_of_clusters, customer_message_path=None, tsne_input_path=None, cluster_output_path=None):
    if tsne_input_path:
        tsne_vectors = np.loadtxt(tsne_input_path, dtype=float)
    else:
        tsne_vectors = np.loadtxt('tsne_vectors.txt', dtype=float)
    vectors_df = pd.DataFrame(data=tsne_vectors, columns=["x", "y"])

    if customer_message_path:
        customer_message_df = pd.read_csv(customer_message_path)
        customer_message = customer_message_df["sentence"].to_list()
    else:
        customer_message = process_user_message.get_processed_customer_message()

    kmeans = KMeans(n_clusters=number_of_clusters, random_state=0).fit(tsne_vectors)

    data_tuples = list(zip(customer_message["message_group"], customer_message["customer_message"], kmeans.labels_))
    clustering_df = pd.DataFrame(data_tuples, columns=['sentence', 'og_sentence', 'kmeans_cluster'])
    clustering_df = pd.merge(clustering_df, vectors_df, right_index=True, left_index=True)

    clustering_df = clustering_df[clustering_df["sentence"].map(len) > 1]
    # clustering_df["og_sentence"] = customer_message["customer_message"]
    clustering_df.to_csv(cluster_output_path, index=False)
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


# for i in range(0, 11):
#     print(i)
#     tsne_vectors_output = "data/cluster_{no_cluster}/tsne_vectors.txt"
#     tsne_vectors_output = tsne_vectors_output.format(no_cluster=str(i + 1))
#
#     customer_message_cluster_output = "data/cluster_{no_cluster}/cluster_{no_cluster}.txt"
#     customer_message_cluster_output = customer_message_cluster_output.format(no_cluster=str(i+1))
#
#     sentence_embedding(i, "data/all_cluster.csv", tsne_vectors_output, customer_message_cluster_output)

# sentence_embedding(1, "data/all_cluster.csv", "data/cluster_2/tsne_vectors.txt", "data/cluster_2/cluster_2.txt")

# for i in range(0,11):
#     tsne_vectors_input = "data/cluster_{no_cluster}/tsne_vectors.txt"
#     tsne_vectors_input = tsne_vectors_input.format(no_cluster=str(i + 1))
#
#     silhouette_score_output = "data/cluster_{no_cluster}/elbow_n_silhouette_scores.csv"
#     silhouette_score_output = silhouette_score_output.format(no_cluster=str(i+1))
#     calculate_silhouette_score(tsne_vectors_input, silhouette_score_output)

# calculate_silhouette_score("data/cluster_2/tsne_vectors.txt", "data/cluster_2/elbow_n_silhouette_scores.csv")

# k_mean_clustering(5, "data/old_cluster/cluster_4/cluster_4.txt", "data/old_cluster/cluster_4/tsne_vectors.txt",
#                   cluster_output_path="data/old_cluster/cluster_4/cluster_4_all_cluster.csv")
# k_mean_clustering(11, None, None, "data/all_cluster.csv")

def main():
    sentence_embedding(1, None, None, None)
    calculate_silhouette_score()
    # k_mean_clustering(6, None, None, "data/all_cluster.csv")


if __name__ == '__main__':
    main()
