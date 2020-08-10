import pandas as pd
from spelling_correction.heuristic_correction import *
import pickle


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
    customer_messages = [do_correction(str(x)) for x in customer_messages]
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


def main():
    customer_messages = get_customer_message()
    customer_messages = remove_col_str(customer_messages)
    customer_messages = remove_col_white_space(customer_messages)
    customer_messages = correction_message(customer_messages)
    with open('data/customer_message/customer_messages.pkl', 'wb') as file:
        # store the data as binary data stream
        pickle.dump(customer_messages, file)
    a = 0


main()
