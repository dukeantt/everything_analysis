import pandas as pd


def main():
    june_file_path = "data/chatlog_fb/result/final/all_chat_fb_6.csv"
    july_file_path = "data/chatlog_fb/result/final/all_chat_fb_7.csv"
    august_file_path = "data/chatlog_fb/result/final/all_chat_fb_8.csv"

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
    for index, df in enumerate([pd.read_csv(june_file_path), pd.read_csv(july_file_path), pd.read_csv(august_file_path)]):
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

    a = 0


main()
