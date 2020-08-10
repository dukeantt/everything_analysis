import pandas as pd
from spelling_correction.heuristic_correction import *
import pickle
import re


def remove_col_str(customer_messages):
    # remove a portion of string in a dataframe column - col_1
    customer_messages = [str(x).replace('\n', ' ') for x in customer_messages]
    # remove all the characters after &# (including &#) for column - col_1
    customer_messages = [str(x).replace(' &#.*', ' ') for x in customer_messages]
    return customer_messages


def remove_col_white_space(customer_messages):
    # remove white space at the beginning of string
    customer_messages = [str(x).strip() for x in customer_messages]
    return customer_messages


def correction_message(customer_messages):
    filter_text = ["facebook.com", "started"]
    correct_messages = []
    for message in customer_messages:
        if all(x not in message for x in filter_text):
            message = do_correction(str(message))
        correct_messages.append(message)
    return correct_messages


def remove_special_characters(customer_messages):
    customer_messages = [re.sub('\}|\{|\]|\[|\;|\.|\,|\.|\:|\!|\@|\#|\$|\^|\&|\(|\)|\<|\>|\?|\"|\'', ' ', str(x)) for x
                         in customer_messages]
    customer_messages = [x for x in customer_messages if x != ' ']
    return customer_messages


def deEmojify(customer_messages):
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    customer_messages = [regrex_pattern.sub(r'',x) for x in customer_messages]
    return customer_messages


def get_customer_message():
    chatlog_df_list = []
    for month in range(1, 8):
        file_path = "data/chatlog_fb/all_chat_fb_{selected_month}.csv"
        file_path = file_path.format(selected_month=str(month))
        chatlog_df = pd.read_csv(file_path)
        chatlog_df_list.append(chatlog_df)

    all_chatlog = pd.concat(chatlog_df_list)
    customer_messages = [x for x in all_chatlog["user_message"] if str(x) != "nan" and str(x) != "user"]
    return customer_messages


def export_clean_customer_messages():
    customer_messages = get_customer_message()
    customer_messages = remove_col_str(customer_messages)
    customer_messages = deEmojify(customer_messages)
    customer_messages = remove_special_characters(customer_messages)
    customer_messages = correction_message(customer_messages)
    customer_messages = remove_col_white_space(customer_messages)
    with open('data/customer_message/customer_messages.pkl', 'wb') as file:
        # store the data as binary data stream
        pickle.dump(customer_messages, file)
    return customer_messages


def main():
    with open('data/customer_message/customer_messages.pkl', 'rb') as file:
        # store the data as binary data stream
        customer_messages = pickle.load(file)

    a = 0


main()
