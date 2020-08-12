import pandas as pd
import datetime
import time
from data_cleaning import *


def split_chatlog_to_conversation(fb_conversations: pd.DataFrame) -> pd.DataFrame:
    start_time = time.time()

    fb_conversations.insert(0, 'conversation_id', "")
    fmt = '%Y-%m-%d %H:%M:%S'

    ids = []

    sender_ids = list(fb_conversations["sender_id"])
    to_sender_ids = list(fb_conversations["to_sender_id"])

    for i, sender_id in enumerate(sender_ids):
        if sender_id == 1454523434857990:
            ids.append(to_sender_ids[i])
        else:
            ids.append(sender_id)

    ids = sorted(set(ids), key=ids.index)
    conversation_id = 0
    checked_sender_id = []

    for sender_id_index, sender_id in enumerate(ids):
        if sender_id not in checked_sender_id:
            conversation_id += 1
            checked_sender_id.append(sender_id)

        sub_df = fb_conversations[(fb_conversations["sender_id"] == sender_id) | (
                fb_conversations["to_sender_id"] == sender_id)].reset_index()

        for item_index in range(0, len(sub_df)):
            message_index = sub_df.at[item_index, "index"]
            fb_conversations.at[message_index, "conversation_id"] = conversation_id
            if item_index + 1 < len(sub_df):
                next_message_created_time = sub_df.at[item_index + 1, "created_time"]
                current_message_created_time = sub_df.at[item_index, "created_time"]

                current_time = current_message_created_time[:10] + " " + current_message_created_time[11:19]
                current_time = datetime.datetime.strptime(current_time, fmt)

                next_time = next_message_created_time[:10] + " " + next_message_created_time[11:19]
                next_time = datetime.datetime.strptime(next_time, fmt)

                time_diff = (next_time - current_time).total_seconds()
                if (current_time.weekday() > 3) and (time_diff > 259200):
                    conversation_id += 1
                elif (current_time.weekday() < 4) and (time_diff > 172800):
                    conversation_id += 1
    print("Split chatlog to conversations: " + str(time.time() - start_time))
    return fb_conversations


def split_conversation_to_tuns(chatlog_df: pd.DataFrame) -> pd.DataFrame:
    start_time = time.time()
    chatlog_df.insert(1, "turn", "")

    conversation_ids = chatlog_df["conversation_id"].drop_duplicates(keep="first").to_list()

    for conversation_id in conversation_ids:
        sub_df = chatlog_df[chatlog_df["conversation_id"] == conversation_id].reset_index()
        turn = 0
        first_item_in_sub_df = True
        for item_index in range(0, len(sub_df)):
            message_index = sub_df.at[item_index, "index"]
            if not first_item_in_sub_df:
                previous_sender_name = sub_df.at[item_index - 1, "user_message"]
                current_sender_name = sub_df.at[item_index, "user_message"]
                if previous_sender_name == 'user' and current_sender_name != previous_sender_name:
                    turn += 1
            first_item_in_sub_df = False
            chatlog_df.at[message_index, "turn"] = turn
    print("Split conversations to turns: " + str(time.time() - start_time))
    return chatlog_df


def pre_process_chat_log(chatlog_df: pd.DataFrame) -> pd.DataFrame:
    start_time = time.time()

    chatlog_df = split_chatlog_to_conversation(chatlog_df)
    chatlog_df = split_conversation_to_tuns(chatlog_df)

    print("Pre-process chat-log: " + str(time.time() - start_time))
    return chatlog_df


def main():
    for month in range(1, 7):
        print(month)
        input_file_path = "data/chatlog_fb/all_chat_fb_{month}.csv"
        output_file_path = "data/chatlog_fb/processed_chatlog/all_chat_fb_{month}.csv"

        chatlog_df = pd.read_csv(input_file_path.format(month=month))
        chatlog_df = do_clean(chatlog_df)
        chatlog_df = pre_process_chat_log(chatlog_df)
        chatlog_df.to_csv(output_file_path.format(month=month), index=False)


main()
