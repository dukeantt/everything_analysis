import pandas as pd
import json
import datetime
from ast import literal_eval
from heuristic_correction import *
from utils.helper import *
from datetime import datetime


def get_sender_id_image_case(all_conv_detail):
    """
    Loop all coversation of customer
    Get only conversation in specific month
    If found "scontent.xx.fbcdn.net" in customer text append id and the date in a list and move to next conversation

    :param all_conv_detail:
    :return: List of sender id and timestamp of conversations that the customers send image
    """
    sender_id_image = []
    sender_id_text = []
    # Loop all conversation
    for conversation in all_conv_detail.itertuples():
        sender_id = conversation.sender_id
        events = literal_eval(conversation.events)
        user_messages = [x for x in events if x["event"] == "user"]
        # Only care about conversation in specific month
        user_messages = [x for x in user_messages]
        # user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%m") in ["03","04","05","06"]]

        img_conversation_date = []
        txt_conversation_date = []
        for index, user_message in enumerate(user_messages):
            timestamp = int(user_message["timestamp"])
            timestamp_date = get_timestamp(timestamp, "%d")

            try:
                next_event_timestamp_date = get_timestamp(int(user_messages[index + 1]["timestamp"]), "%d")
            except:
                next_event_timestamp_date = timestamp_date

            try:
                message_text = user_message["parse_data"]["text"]
            except Exception as e:
                message_text = "user message"

            if message_text is None:
                message_text = "user message"

                # If the conversation date of sender id already existed move to next conversation
            if "scontent.xx.fbcdn.net" in message_text and timestamp_date not in img_conversation_date:
                img_conversation_date.append(timestamp_date)
                # Append timestamp and sender id to list
                sender_id_image.append((sender_id, get_timestamp(timestamp, "%Y-%m-%d")))
            elif "scontent" not in message_text and timestamp_date not in txt_conversation_date:
                txt_conversation_date.append(timestamp_date)
                sender_id_text.append((sender_id, get_timestamp(timestamp, "%Y-%m-%d")))

    return sender_id_image, sender_id_text

def split_uc3_and_uc4(all_conv_no_image, sender_id_text):
    necessary_info_uc3 = {"timestamp": [], "sender_id": [], "message": []}

    for conversation in all_conv_no_image.itertuples():
        sender_id = conversation.sender_id
        set_date = sender_id_text[sender_id]
        events = literal_eval(conversation.events)
        user_messages = [x for x in events if x["event"] == "user"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%Y-%m-%d") == set_date]
        for user_message in user_messages:
            try:
                message_text = user_message["parse_data"]["text"]
            except Exception as e:
                message_text = "asd"
            # Do correction
            if message_text is not None:
                message_text = do_correction(message_text)
            else:
                continue
            key_words = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk",
                         "số tài khoản", "gửi về"]

            if "có" in message_text and "không" in message_text and "giá" not in message_text and "bao nhiêu" not in message_text:
                if any(a in message_text for a in key_words):
                    continue
                else:
                    necessary_info_uc3["timestamp"].append(set_date)
                    necessary_info_uc3["sender_id"].append(sender_id)
                    necessary_info_uc3["message"].append(message_text)

    necessary_info_uc3_df = pd.DataFrame.from_dict(necessary_info_uc3)
    return necessary_info_uc3_df, []


def get_uc2_price_case(all_conv_about_image, sender_id_image):
    necessary_info_uc2 = {"timestamp": [], "sender_id": [], "message": []}
    necessary_info_uc1 = {"timestamp": [], "sender_id": [], "message": []}
    # Loop all conversations
    for conversation in all_conv_about_image.itertuples():
        # Already have the list of all dates that customer send image in conversations
        # Get conversation of sender id on these specific dates
        sender_id = conversation.sender_id
        set_date = sender_id_image[sender_id]
        events = literal_eval(conversation.events)
        user_messages = [x for x in events if x["event"] == "user"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%Y-%m-%d") == set_date]
        for user_message in user_messages:
            try:
                message_text = user_message["parse_data"]["text"]
            except Exception as e:
                message_text = "asd"
            # Do correction
            message_text = do_correction(message_text)
            # If customer mention bao giá and bao nhiêu ->UC2:User sends product image and asks for the product's price
            if "giá" in message_text or "bao nhiêu" in message_text:
                necessary_info_uc2["timestamp"].append(set_date)
                necessary_info_uc2["sender_id"].append(sender_id)
                necessary_info_uc2["message"].append(message_text)
            # elif "có" in message_text or "còn" in message_text:
            #     # But if found có and còn before giá -> UC1
            #     break
            else:
                # all other cases is uc1
                if sender_id not in necessary_info_uc2["sender_id"]:
                    necessary_info_uc1["timestamp"].append(set_date)
                    necessary_info_uc1["sender_id"].append(sender_id)
                    necessary_info_uc1["message"].append(message_text)

    necessary_info_uc2_df = pd.DataFrame.from_dict(necessary_info_uc2)
    necessary_info_uc1_df = pd.DataFrame.from_dict(necessary_info_uc1)
    for index, item in necessary_info_uc1_df.iterrows():
        if item["sender_id"] in necessary_info_uc2["sender_id"]:
            necessary_info_uc1_df = necessary_info_uc1_df.drop([index])

    return necessary_info_uc1_df, necessary_info_uc2_df


def processing_uc_conversations(all_uc2_conversation, dict_info):
    necessary_info = {"timestamp": [],
                      "sender_id": [],
                      "message": [],
                      "bot_message": [],
                      "user_intent": [],
                      "user_timestamp_detail": [],
                      "bot_timestamp_detail": [],
                      "wait_time": []}
    # Loop all conversations
    for conversation in all_uc2_conversation.itertuples():
        # Already have the sender_id and timestamp of UC2 conversations
        # Get the conversation by timestamp
        sender_id = conversation.sender_id
        set_date = dict_info[sender_id]
        events = literal_eval(conversation.events)
        user_bot_messages = [x for x in events if x["event"] in ["user", "bot"]]
        user_bot_messages = [x for x in user_bot_messages if get_timestamp(int(x["timestamp"]), "%Y-%m-%d") == set_date]

        for i in range(0, len(user_bot_messages)):
            current_event = user_bot_messages[i]["event"]
            # Get user message and intent
            if current_event == "user":
                try:
                    user_message = user_bot_messages[i]["text"]
                except:
                    user_message = "Customer say nothing"

                try:
                    intent = user_bot_messages[i]['parse_data']["intent"]["name"]
                except:
                    intent = "no intent"

                # Check if next event is bot event
                bot_index = i + 1
                if bot_index < len(user_bot_messages) and user_bot_messages[bot_index]["event"] != "bot":
                    bot_index = i + 2

                if bot_index < len(user_bot_messages) and user_bot_messages[bot_index]["event"] == "bot":
                    bot_event = user_bot_messages[bot_index]

                    try:
                        # calculate customer wait time and get bot message
                        bot_message_text = bot_event["text"]
                        bot_timestamp = get_timestamp(int(bot_event["timestamp"]), "%Y-%m-%d %H:%M:%S")

                        fmt = '%Y-%m-%d %H:%M:%S'
                        user_time = datetime.strptime(
                            get_timestamp(int(user_bot_messages[i]["timestamp"]), "%Y-%m-%d %H:%M:%S"), fmt)
                        bot_time = datetime.strptime(bot_timestamp, fmt)
                        wait_time = (bot_time - user_time).total_seconds()
                    except Exception as e:
                        bot_message_text = "No bot text"
                        bot_timestamp = " "
                        wait_time = " "

                    if bot_message_text is None:
                        bot_message_text = "Bot doing sth"

                    necessary_info["timestamp"].append(set_date)
                    necessary_info["sender_id"].append(sender_id)
                    necessary_info["message"].append(user_message)
                    necessary_info["bot_message"].append(bot_message_text)
                    necessary_info["user_intent"].append(intent)
                    necessary_info["user_timestamp_detail"].append(
                        get_timestamp(int(user_bot_messages[i]["timestamp"]), "%Y-%m-%d %H:%M:%S"))
                    necessary_info["bot_timestamp_detail"].append(bot_timestamp)
                    necessary_info["wait_time"].append(wait_time)
                elif i == len(user_bot_messages) - 1:
                    necessary_info["timestamp"].append(set_date)
                    necessary_info["sender_id"].append(sender_id)
                    necessary_info["message"].append(user_message)
                    necessary_info["bot_message"].append("bot handover_to_inbox")
                    necessary_info["user_intent"].append(intent)
                    necessary_info["user_timestamp_detail"].append(
                        get_timestamp(int(user_bot_messages[i]["timestamp"]), "%Y-%m-%d %H:%M:%S"))
                    necessary_info["bot_timestamp_detail"].append("no_timestamp")
                    necessary_info["wait_time"].append("no_wait_time")

                else:
                    continue
            else:
                continue

    necessary_info_df = pd.DataFrame.from_dict(necessary_info)
    conversation_number = []
    last_conversation = 0
    for c in range(0, len(necessary_info_df)):
        if c == 0:
            conversation_number.append(1)
            last_conversation = 1
        else:
            last_sender_id = necessary_info_df.iloc[c - 1]["sender_id"]
            current_sender_id = necessary_info_df.iloc[c]["sender_id"]
            if current_sender_id == last_sender_id:
                conversation_number.append(last_conversation)
            else:
                conversation_number.append(last_conversation + 1)
                last_conversation += 1

    necessary_info_df.insert(0, 'id', conversation_number)
    necessary_info_df.insert(5, 'outcome', [" "] * len(necessary_info_df))
    necessary_info_df.insert(6, 'silent', [0] * len(necessary_info_df))
    return necessary_info_df


def add_outcome(uc_conversation_df, uc):
    uc2_conversations_df = pd.read_csv("analyze_data/uc3_conversation.csv")
    # uc2_conversations_df = uc_conversation_df
    conversation_ids_list = list(set(uc2_conversations_df["id"]))
    key_words = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk", "số tài khoản", "gửi về"]
    filter_words = ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]

    for conversation_id in conversation_ids_list:
        convesation_df = uc2_conversations_df[uc2_conversations_df["id"] == conversation_id]
        last_turn = convesation_df.iloc[-1]
        last_turn_row_index = convesation_df.index.tolist()[-1]
        bot_message = last_turn["bot_message"]
        user_message = last_turn["message"]
        user_intent = last_turn["user_intent"]

        if user_intent == "thank":
            uc2_conversations_df.at[last_turn_row_index, "outcome"] = "thanks"

        elif any(key_word in user_message for key_word in key_words) and any(
                filter_word not in user_message for filter_word in filter_words):
            uc2_conversations_df.at[last_turn_row_index, "outcome"] = "shipping_order"

        elif user_intent == "handover_to_inbox" or "handover_to_inbox" in bot_message:
            uc2_conversations_df.at[last_turn_row_index, "outcome"] = "handover_to_inbox"

        elif bot_message == "Mình chưa xác định được món đồ bạn hỏi, bạn mô tả rõ hơn giúp mình nhé!" or user_intent == "disagree":
            uc2_conversations_df.at[last_turn_row_index, "outcome"] = "cv_fail"
            uc2_conversations_df.at[last_turn_row_index, "silent"] = 1

        else:
            uc2_conversations_df.at[last_turn_row_index, "outcome"] = "other"
            uc2_conversations_df.at[last_turn_row_index, "silent"] = 1
    uc2_conversations_df.to_csv("analyze_data/" + uc + "_conversation_with_outcome.csv", index=False)


def handle_cases_in_uc1(uc1_conversation_df):
    user_message_keywords = ["không còn"]
    bot_message_keywords = ["mô tả rõ hơn", "chưa xác định", "hết hàng"]
    uc1_conversation_df.insert(5, 'uc', [None] * len(uc1_conversation_df))
    conversation_ids = list(set(uc1_conversation_df["id"]))
    for conversation_id in conversation_ids:
        checked_id = []
        sub_uc1_conversation_df = uc1_conversation_df[uc1_conversation_df["id"] == conversation_id]
        found_image = False
        for index, item in sub_uc1_conversation_df.iterrows():
            user_intent = item["user_intent"]
            user_message = item["message"]
            bot_message = item["bot_message"]
            if "scontent.xx.fbcdn.net" in user_message:
                found_image = True
            if found_image:
                if user_intent == "disagree" or \
                        any(key_word in bot_message for key_word in bot_message_keywords) or \
                        any(key_word in user_message for key_word in user_message_keywords):
                    uc1_conversation_df.at[index, "uc"] = "1.2"
                    checked_id.append(conversation_id)

    uc_1_2_conversation_ids = list(set(uc1_conversation_df[uc1_conversation_df["uc"] == "1.2"]["id"]))
    uc_1_1_conversation_ids = [id for id in conversation_ids if id not in uc_1_2_conversation_ids]
    for conversation_id in uc_1_1_conversation_ids:
        sub_uc_1_1_conversation_df = uc1_conversation_df[uc1_conversation_df["id"] == conversation_id]
        for item in sub_uc_1_1_conversation_df.itertuples():
            index = item.Index
            uc1_conversation_df.at[index, "uc"] = "1.1"
            break

    return uc1_conversation_df


def main():
    all_conv_detail = get_all_conv_detail()
    df_data = {"id": [], "sender_id": [],"message": [], "timestamp": []}
    id = 0
    fmt = '%Y-%m-%d %H:%M:%S'

    for index, conv in all_conv_detail.iterrows():
        id += 1
        events = literal_eval(conv["events"])
        user_messages = [x for x in events if x["event"] == "user"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%m") in ["05","06"]]
        sender_id = conv["sender_id"]
        for index, user_message in enumerate(user_messages):
            current_timestamp = get_timestamp(user_message["timestamp"], fmt)
            df_data["id"].append(id)
            df_data["sender_id"].append(sender_id)
            df_data["message"].append(user_message["text"])
            df_data["timestamp"].append(current_timestamp)

            current_timestamp = datetime.strptime(current_timestamp, fmt)
            try:
                next_timestamp = get_timestamp(user_messages[index + 1]["timestamp"], fmt)
                next_timestamp = datetime.strptime(next_timestamp, fmt)
            except:
                break
            time_diff = (next_timestamp - current_timestamp).total_seconds()
            if time_diff > 900:
                id += 1
    conversations_df = pd.DataFrame.from_dict(df_data)

    # Get all case that customer send image
    sender_id_image, sender_id_text = get_sender_id_image_case(all_conv_detail)
    sender_id_image = {a: b for a, b in sender_id_image}
    all_conv_about_image = all_conv_detail[all_conv_detail["sender_id"].isin(list(sender_id_image.keys()))]

    sender_id_text = {a: b for a,b in sender_id_text}
    all_conv_no_image = all_conv_detail[all_conv_detail["sender_id"].isin(list(sender_id_text.keys()))]

    df_uc3, df_uc4 = split_uc3_and_uc4(all_conv_no_image, sender_id_text)
    # Get all usecase 2: User sends product image and asks for the product's price
    df_uc1, df_uc2 = get_uc2_price_case(all_conv_about_image, sender_id_image)

    df_uc1 = df_uc1.drop_duplicates(subset=["timestamp", "sender_id"], keep="first")
    sender_id_info_uc1 = list(df_uc1["sender_id"])
    timestamp_info_uc1 = list(df_uc1["timestamp"])
    senderid_timestamp_pair_uc1 = dict(zip(sender_id_info_uc1, timestamp_info_uc1))

    df_uc2 = df_uc2.drop_duplicates(subset=["timestamp", "sender_id"], keep="first")
    sender_id_info_uc2 = list(df_uc2["sender_id"])
    timestamp_info_uc2 = list(df_uc2["timestamp"])
    senderid_timestamp_pair_uc2 = dict(zip(sender_id_info_uc2, timestamp_info_uc2))

    df_uc3 = df_uc3.drop_duplicates(subset=["timestamp", "sender_id"], keep="first")
    sender_id_info_uc3 = list(df_uc3["sender_id"])
    timestamp_info_uc3 = list(df_uc3["timestamp"])
    senderid_timestamp_pair_uc3 = dict(zip(sender_id_info_uc3, timestamp_info_uc3))

    # Processing all uc1
    all_uc1_conversation = all_conv_detail[all_conv_detail["sender_id"].isin(sender_id_info_uc1)]
    uc1_conversation_df = processing_uc_conversations(all_uc1_conversation, senderid_timestamp_pair_uc1)
    uc1_conversation_df = handle_cases_in_uc1(uc1_conversation_df)

    # Processing all uc2
    all_uc2_conversation = all_conv_detail[all_conv_detail["sender_id"].isin(sender_id_info_uc2)]
    uc2_conversation_df = processing_uc_conversations(all_uc2_conversation, senderid_timestamp_pair_uc2)
    # uc2_conversation_df.to_csv("analyze_data/uc2_conversation.csv", index=False)

    # Processing all uc3
    all_uc3_conversation = all_conv_detail[all_conv_detail["sender_id"].isin(sender_id_info_uc3)]
    uc3_conversation_df = processing_uc_conversations(all_uc3_conversation, senderid_timestamp_pair_uc3)
    for conversation_id in list(set(uc3_conversation_df["id"])):
        sub_uc3_df = uc3_conversation_df[uc3_conversation_df["id"] == conversation_id]
        messages = sub_uc3_df["message"].tolist()
        for message in messages:
            if "scontent" in message:
                indexNames = sub_uc3_df.index
                uc3_conversation_df = uc3_conversation_df.drop(indexNames)
                break
    uc3_conversation_df.to_csv("analyze_data/uc3_conversation.csv", index=False)

    # Add outcome for conversation uc1 and uc2
    # add_outcome(uc2_conversation_df, "uc2")
    # add_outcome(uc3_conversation_df, "uc3")
    # add_outcome(uc1_conversation_df, "uc1")


main()
# add_outcome([], "uc3")
