import pandas as pd
import json
from ast import literal_eval
from heuristic_correction import *
from utils.helper import *
from datetime import datetime
import unicodedata

def label_conversation(fb_conversations):
    fb_conversations.insert(0, 'conversation_id', "")
    fmt = '%Y-%m-%d %H:%M:%S'
    sender_ids = list(fb_conversations["sender_id"])
    sender_ids = sorted(set(sender_ids), key=sender_ids.index)
    conversation_id = 0
    checked_sender_id = []
    for sender_id_index, sender_id in enumerate(sender_ids):
        if sender_id not in checked_sender_id:
            conversation_id += 1
            checked_sender_id.append(sender_id)
        sub_df = fb_conversations[fb_conversations["sender_id"] == sender_id].reset_index()
        # sub_df = sub_df[sub_df["sender_name"] != "Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp"].reset_index()
        for index, item in sub_df.iterrows():
            message_index = item["index"]
            fb_conversations.at[message_index, "conversation_id"] = conversation_id
            # if item["sender_name"] == "Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp":

            if index + 1 < len(sub_df):
                next_message = sub_df.iloc[index + 1]
                current_time = item["created_time"][:10] + " " + item["created_time"][11:19]
                current_time = datetime.strptime(current_time, fmt)

                next_time = next_message["created_time"][:10] + " " + next_message["created_time"][11:19]
                next_time = datetime.strptime(next_time, fmt)

                time_diff = (next_time - current_time).total_seconds()
                if time_diff > 900:
                    conversation_id += 1

    return fb_conversations


def process_uc1_and_uc2(fb_conversations=None):
    fb_conversations = pd.read_csv("temporary_data/fb_conversaton.csv")
    fb_conversations.insert(6, 'usecase', "")
    conversation_ids = list(fb_conversations["conversation_id"])
    conversation_ids = sorted(set(conversation_ids), key=conversation_ids.index)
    case_1_2_conversation_ids = []
    uc_1_conversation_ids = []
    uc_2_conversation_ids = []
    others = []
    # Check each conversation
    for conversation_id in conversation_ids:
        sub_df = fb_conversations[fb_conversations["conversation_id"] == conversation_id]
        attachments = list(sub_df["attachments"])

        if any("scontent" in str(x) for x in attachments):
            if conversation_id not in case_1_2_conversation_ids:
                case_1_2_conversation_ids.append(conversation_id)

    for conversation_id in case_1_2_conversation_ids:
        sub_df = fb_conversations[fb_conversations["conversation_id"] == conversation_id]
        for index, item in sub_df.iterrows():
            user_message = item["user_message"]
            if str(user_message) == "nan":
                user_message = "nan"
                continue
            user_message_correction = do_correction(user_message)
            user_message_correction = unicodedata.normalize("NFC", user_message_correction)
            sender_name = item["sender_name"]
            if "giá" in user_message_correction or "bao nhiêu" in user_message_correction or "tiền" in user_message_correction:
                uc_2_conversation_ids.append((conversation_id, user_message))
                break
            elif ("có" in user_message_correction
                  or "còn" in user_message_correction or
                  "sẵn hàng" in user_message_correction or "có sẵn" in user_message_correction or "còn sẵn" in user_message_correction) \
                    and "không" in user_message_correction\
                    and "ship" not in user_message_correction and "link shopee" not in user_message_correction:
                uc_1_conversation_ids.append((conversation_id, user_message))
                break
            else:
                others.append((conversation_id, user_message))
                break

    a = 0

def main():
    fb_conversations = pd.read_csv("analyze_data/all_chat_fb/all_chat_fb_5.csv")
    fb_conversations = fb_conversations[~fb_conversations["sender_id"].isna()]
    fb_conversations = fb_conversations.iloc[::-1].reset_index(drop=True)
    fb_conversations = label_conversation(fb_conversations)
    fb_conversations.to_csv("temporary_data/fb_conversaton.csv", index=False)

    a = 0


# main()
process_uc1_and_uc2()
