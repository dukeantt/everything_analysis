import pandas as pd
from collections import Counter
import itertools
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import re

file_name = "stop_words.txt"
with open(file_name) as f:
    line_list = f.readlines()
stop_words = [x.replace("\n", "") for x in line_list]


def has_numbers(inputString):
    return bool(re.search(r'\d', inputString))


def split_to_bigram(word_list):
    bigram_wordlist = []
    for index, word in enumerate(word_list):
        if index < len(word_list) - 1:
            bigram_word = word + " " + word_list[index + 1]
            bigram_word = bigram_word.strip()
            bigram_wordlist.append(bigram_word)
        if len(word_list) == 1:
            bigram_wordlist.append(word)
    return bigram_wordlist


def main():
    all_cluster_df = pd.read_csv("data/all_cluster.csv")
    all_cluster_word_count = []
    number_of_clusters = all_cluster_df["kmeans_cluster"].drop_duplicates().to_list()
    for cluster in number_of_clusters:
        all_words = []
        cluster_df = all_cluster_df[all_cluster_df["kmeans_cluster"] == cluster]
        all_sentences = cluster_df["sentence"].to_list()

        sentences = [str(x).split() for x in all_sentences]
        bigrams = []
        for item in sentences:
            bigrams.append(split_to_bigram(item))

        # all_words = list(itertools.chain.from_iterable(map(lambda x: x.split(), all_sentences)))
        # all_words = [x for x in all_words if x not in stop_words]

        all_words = list(itertools.chain.from_iterable(bigrams))
        all_words = [x for x in all_words if x not in stop_words]
        all_words = [x for x in all_words if not has_numbers(x)]
        all_words = [x.replace(" ", "_") for x in all_words]

        unique_string = (" ").join(all_words)
        wordcloud = WordCloud(width=1000, height=500, max_words=20, collocations=False).generate(unique_string)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.savefig("word_cloud_cluster_" + str(cluster) + ".png", bbox_inches='tight')
        plt.show()
        plt.close()

        word_count = Counter(all_words)
        all_cluster_word_count.append(dict(word_count.most_common(5)))
    a = 0


if __name__ == '__main__':
    main()
