import pandas as pd
from collections import Counter
import itertools

file_name = "stop_words.txt"
with open(file_name) as f:
    line_list = f.readlines()
stop_words = [x.replace("\n", "") for x in line_list]


def main():
    all_cluster_df = pd.read_csv("data/all_cluster.csv")
    all_cluster_word_count = []
    number_of_clusters = all_cluster_df["kmeans_cluster"].drop_duplicates().to_list()
    for cluster in number_of_clusters:
        all_words = []
        cluster_df = all_cluster_df[all_cluster_df["kmeans_cluster"] == cluster]
        all_sentences = cluster_df["sentence"].to_list()
        all_words = list(itertools.chain.from_iterable(map(lambda x: x.split(), all_sentences)))
        all_words = [x for x in all_words if x not in stop_words]
        word_count = Counter(all_words)
        all_cluster_word_count.append(dict(word_count.most_common(5)))
    a = 0

if __name__ == '__main__':
    main()
