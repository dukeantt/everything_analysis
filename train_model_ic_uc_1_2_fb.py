import pandas as pd
from naive_tf_idf.naive_bayes_model import NaiveBayesModel
from naive_tf_idf.text_classification_predict import TextClassificationPredict


def main():
    uc1_data_list = []
    uc2_data_list = []
    other_data_list = []
    for x in ["1","2","3", "4", "5", "6"]:
        uc1_data = pd.read_csv("temporary_data/uc1_data_" + x + ".csv")
        uc2_data = pd.read_csv("temporary_data/uc2_data_" + x + ".csv")
        other_data = pd.read_csv("temporary_data/other_data_" + x + ".csv")

        uc1_data_list.append(uc1_data)
        uc2_data_list.append(uc2_data)
        other_data_list.append(other_data)

    uc1_data_total = pd.concat(uc1_data_list)
    uc2_data_total = pd.concat(uc2_data_list)
    other_data_total = pd.concat(other_data_list)
    other_data_total = other_data_total.dropna(subset=['feature'])

    no_train_uc1 = int(2 * len(uc1_data_total) / 3)
    no_train_uc2 = int(2 * len(uc2_data_total) / 3)
    no_train_other = int(2 * len(other_data_total) / 3)

    # uc1_uc2_data_train = pd.concat([
    #     uc1_data_total[:no_train_uc1],
    #     uc2_data_total[:no_train_uc2],
    #     other_data_total[:no_train_other]
    # ])
    uc1_uc2_data_train = pd.concat([
        uc1_data_total,
        uc2_data_total,
        other_data_total
    ])

    tcp = TextClassificationPredict()
    clf = tcp.get_train_data(uc1_uc2_data_train)


main()
