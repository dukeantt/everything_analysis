import pandas as pd
import json
import datetime
from ast import literal_eval
from heuristic_correction import *
from utils.helper import *


def get_sender_id_image_case(all_conv_detail):

    """
    Loop all coversation of customer
    Get only conversation in specific month
    If found "scontent.xx.fbcdn.net" in customer text append id and the date in a list and move to next conversation

    :param all_conv_detail:
    :return: List of sender id and timestamp of conversations that the customers send image
    """
    sender_id_image = []
    # Loop all conversation
    for conversation in all_conv_detail.itertuples():
        sender_id = conversation.sender_id
        events = literal_eval(conversation.events)
        user_messages = [x for x in events if x["event"] == "user"]
        # Only care about conversation in specific month
        # user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%m") == "06"]
        conversation_date = []
        for user_message in user_messages:
            timestamp = int(user_message["timestamp"])
            timestamp_date = get_timestamp(timestamp, "%d")
            try:
                message_text = user_message["parse_data"]["text"]
            except Exception as e:
                message_text = "user message"

            if message_text is None:
                message_text = "user message"

                # If the conversation date of sender id already existed move to next conversation
            if "scontent.xx.fbcdn.net" in message_text and timestamp_date not in conversation_date:
                conversation_date.append(timestamp_date)
                # Append timestamp and sender id to list
                sender_id_image.append((sender_id, get_timestamp(timestamp, "%Y-%m-%d")))
    return sender_id_image


def get_uc2_case(all_conv_about_image, sender_id_image):
    necessary_info = {"timestamp": [], "sender_id": [], "message": []}
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
                necessary_info["timestamp"].append(set_date)
                necessary_info["sender_id"].append(sender_id)
                necessary_info["message"].append(message_text)
            elif "có" in message_text or "còn" in message_text:
            # But if found có and còn before giá -> UC1
                break

    necessary_info_df = pd.DataFrame.from_dict(necessary_info)
    return necessary_info_df


def export_all_uc2_to_csv(all_uc2_conversation, dict_info):
    necessary_info = {"timestamp": [], "sender_id": [], "message": [], "bot_message": []}
    # Loop all conversations
    for conversation in all_uc2_conversation.itertuples():
        # Already have the sender_id and timestamp of UC2 conversations
        # Get the conversation by timestamp
        sender_id = conversation.sender_id
        set_date = dict_info[sender_id]
        events = literal_eval(conversation.events)
        user_messages = [x for x in events if x["event"] == "user"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%Y-%m-%d") == set_date]
        bot_messages = [x for x in events if x["event"] == "bot"]
        bot_messages = [x for x in bot_messages if get_timestamp(int(x["timestamp"]), "%Y-%m-%d") == set_date]

        for i in range(0, len(user_messages)):
            try:
                message_text = user_messages[i]["parse_data"]["text"]
            except Exception as e:
                message_text = "user text"
            try:
                bot_message_text = bot_messages[i]["text"]
            except Exception as e:
                bot_message_text = "bot text"

            necessary_info["timestamp"].append(set_date)
            necessary_info["sender_id"].append(sender_id)
            necessary_info["message"].append(message_text)
            necessary_info["bot_message"].append(bot_message_text)

    necessary_info_df = pd.DataFrame.from_dict(necessary_info)
    necessary_info_df.to_csv("analyze_data/uc2_conversation.csv", index=False)
    return necessary_info_df


def main():
    all_conv_detail = get_all_conv_detail()
    # Get all case that customer send image
    sender_id_image = get_sender_id_image_case(all_conv_detail)
    sender_id_image = {a: b for a, b in sender_id_image}
    all_conv_about_image = all_conv_detail[all_conv_detail["sender_id"].isin(list(sender_id_image.keys()))]

    # Get all usecase 2: User sends product image and asks for the product's price
    df = get_uc2_case(all_conv_about_image, sender_id_image)
    df = df.drop_duplicates(subset=["timestamp", "sender_id"], keep="first")
    sender_id_info = list(df["sender_id"])
    timestamp_info = list(df["timestamp"])
    dict_info = dict(zip(sender_id_info, timestamp_info))

    # Export all uc2 to csv
    all_uc2_conversation = all_conv_detail[all_conv_detail["sender_id"].isin(sender_id_info)]
    export_all_uc2_to_csv(all_uc2_conversation, dict_info)
    a = 0


main()
