import pandas as pd
# from model.svm_model import SVMModel
from naive_tf_idf.naive_bayes_model import NaiveBayesModel
import pickle


class TextClassificationPredict(object):
    def __init__(self):
        self.test = None

    def get_train_data(self, train_data_df=None):
        # Tạo train data
        df_train = train_data_df
        # init model naive bayes
        model = NaiveBayesModel()
        clf = model.clf.fit(df_train["feature"], df_train.target)
        with open('models/ic_for_uc1_2.pkl', 'wb') as file:
            pickle.dump(clf, file)
        return clf