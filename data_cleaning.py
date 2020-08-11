import pandas as pd
from spelling_correction.heuristic_correction import *
import pickle
import re


def remove_col_str(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    # remove a portion of string in a dataframe column - col_1
    df[col_name].replace('\n', '', regex=True, inplace=True)
    # remove all the characters after &# (including &#) for column - col_1
    df[col_name].replace(' &#.*', '', regex=True, inplace=True)
    return df


def remove_col_white_space(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    # remove white space at the beginning of string
    df[col_name] = df[col_name].str.lstrip()
    return df


def correction_message(df: pd.DataFrame, col_name: str, og_col_name: str) -> pd.DataFrame:
    filter_text = ["facebook.com", "started"]
    df[col_name] = df[og_col_name].map(
        lambda message: do_correction(message) if str(message) != "nan" and all(x not in str(message) for x in filter_text) else message)
    return df


def remove_special_characters(customer_messages):
    customer_messages = [re.sub('\}|\{|\]|\[|\;|\.|\,|\.|\:|\!|\@|\#|\$|\^|\&|\(|\)|\<|\>|\?|\"|\'', ' ', str(x)) for x
                         in customer_messages]
    customer_messages = [x for x in customer_messages if x != ' ']
    return customer_messages


def deEmojify(df: pd.DataFrame, col_name: str, og_col_name: str) -> pd.DataFrame:
    """

    @param df:
    @param col_name:
    @param og_col_name:
    @return:
    """
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    df[col_name] = df[og_col_name].map(lambda message: regrex_pattern.sub(r'', message) if str(message) != "nan" else message)
    return df



# def export_clean_customer_messages():
#     customer_messages = get_customer_message()
#     customer_messages = remove_col_str(customer_messages)
#     customer_messages = deEmojify(customer_messages)
#     customer_messages = remove_special_characters(customer_messages)
#     customer_messages = correction_message(customer_messages)
#     customer_messages = remove_col_white_space(customer_messages)
#     with open('data/customer_message/customer_messages.pkl', 'wb') as file:
#         # store the data as binary data stream
#         pickle.dump(customer_messages, file)
#     return customer_messages
