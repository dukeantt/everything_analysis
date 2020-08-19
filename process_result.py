import pandas as pd
import numpy as np
from outcome import *


def count_outcome_of_usecase():
    june_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_6.csv"
    july_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_7.csv"
    august_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_8.csv"

    uc4_dict = {
        "uc_s4.1": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
        "uc_s4.2": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
        "uc_s4.3": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
    }

    uc5_dict = {
        "uc_s5.1": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
        "uc_s5.2": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
        "uc_s5.3": {"thank": 0, "shipping_order": 0, "handover_to_inbox": 0, "silence": 0, "other": 0},
    }
    # for df in [pd.read_csv(june_file_path), pd.read_csv(july_file_path), pd.read_csv(august_file_path)]:
    for index, df in enumerate(
            [pd.read_csv(june_file_path), pd.read_csv(july_file_path), pd.read_csv(august_file_path)]):
        uc4_conversation_id = df[~df["uc4"].isna()]["conversation_id"].drop_duplicates().to_list()

        for id in uc4_conversation_id:
            sub_df = df[df["conversation_id"] == id]
            usecases = sub_df["uc4"].dropna().drop_duplicates().to_list()
            outcome = sub_df["outcome"].dropna().to_list()[0]
            for usecase in usecases:
                uc4_dict[usecase][outcome] += 1

        uc5_conversation_id = df[~df["uc5"].isna()]["conversation_id"].drop_duplicates().to_list()

        for id in uc5_conversation_id:
            sub_df = df[df["conversation_id"] == id]
            usecases = sub_df["uc5"].dropna().drop_duplicates().to_list()
            outcome = sub_df["outcome"].dropna().to_list()[0]
            for usecase in usecases:
                uc5_dict[usecase][outcome] += 1
    return uc4_dict, uc5_dict


def reformat_df():
    june_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_6.csv"
    july_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_7.csv"
    august_file_path = "data/chatlog_fb/result/data_with_outcome/all_chat_fb_8.csv"

    for index, path in enumerate([june_file_path, july_file_path, august_file_path]):
        df = pd.read_csv(path)
        uc4_uc5_conversation_ids = df[(~df["uc4"].isna()) | (~df["uc5"].isna())]["conversation_id"].drop_duplicates().to_list()

        uc41_conversation_ids = df[df["uc4"] == "uc_s4.1"]["conversation_id"].drop_duplicates().to_list()
        uc42_conversation_ids = df[df["uc4"] == "uc_s4.2"]["conversation_id"].drop_duplicates().to_list()
        uc43_conversation_ids = df[df["uc4"] == "uc_s4.3"]["conversation_id"].drop_duplicates().to_list()
        uc51_conversation_ids = df[df["uc5"] == "uc_s5.1"]["conversation_id"].drop_duplicates().to_list()
        uc52_conversation_ids = df[df["uc5"] == "uc_s5.2"]["conversation_id"].drop_duplicates().to_list()
        uc53_conversation_ids = df[df["uc5"] == "uc_s5.3"]["conversation_id"].drop_duplicates().to_list()

        df = df[df["conversation_id"].isin(uc52_conversation_ids)]
        df = df.sort_values(by=["conversation_id", "created_time"])
        info_dict = {x: [] for x in list(df.columns)}
        info_dict.pop('turn', None)
        info_dict.pop('message_id', None)
        info_dict.pop('to_sender_id', None)
        info_dict.pop('updated_time', None)
        info_dict.pop('use_case', None)
        info_dict.pop('sender', None)
        info_dict.pop('attachments', None)
        info_dict.pop('user_message_clean', None)
        info_dict.pop('sender_name', None)
        info_dict.pop('fb_conversation_id', None)

        user_counter = 0
        bot_counter = 0
        counter = 0
        for row in df.itertuples():
            counter += 1
            user_message = row.user_message
            bot_message = row.bot_message
            outcome = row.outcome
            if str(outcome) == "nan":
                outcome = ''

            if user_message is None or user_message == "None":
                user_message = np.NaN
            if bot_message is None or bot_message == "None":
                bot_message = np.NaN
            if user_message == "user":
                user_counter = 0
                bot_counter += 1
                info_dict["bot_message"].append(bot_message)
                uc4 = row.uc4
                uc5 = row.uc5
                if "outcome" in info_dict:
                    if outcome != '':
                        if bot_counter <= 1:
                            info_dict["outcome"].remove('')
                        info_dict["outcome"].append(outcome)

                if bot_counter > 1:
                    info_dict["conversation_id"].append(row.conversation_id)
                    info_dict["user_message"].append(np.NaN)
                    info_dict["created_time"].append("")
                    if "outcome" in info_dict and (outcome == ''):
                        info_dict["outcome"].append(outcome)

                    info_dict["uc4"].append(uc4)
                    info_dict["uc5"].append(uc5)
                    info_dict["sender_id"].append(row.sender_id)

            elif user_message != "user":
                user_counter += 1
                bot_counter = 0
                info_dict["conversation_id"].append(row.conversation_id)
                info_dict["user_message"].append(user_message)
                info_dict["created_time"].append(row.created_time)
                info_dict["uc4"].append(row.uc4)
                info_dict["uc5"].append(row.uc5)
                info_dict["sender_id"].append(row.sender_id)
                if "outcome" in info_dict:
                    info_dict["outcome"].append(outcome)

                if user_counter > 1 or counter == len(df):
                    info_dict["bot_message"].append(np.NaN)

                if counter == len(df) and user_counter > 1:
                    info_dict["bot_message"].append(np.NaN)

        df = pd.DataFrame.from_dict(info_dict)
        df.to_csv("data/chatlog_fb/reformat_result/uc52_" + path.split("/")[-1], index=False)


def main():
    reformat_df()


main()
