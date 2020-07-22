import pandas as pd
from csv import DictWriter
import os
import json
import logging
import pickle
import datetime
import time
from ast import literal_eval

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_all_conv():
    with open('../chatlog_data/all_conv.pkl', 'rb') as f:
        data = pickle.load(f)
    return data


def get_all_conv_detail():
    """
    Get conversation detail from file
    """
    all_conv_detail_df = pd.read_csv("chatlog_data/all_conv_detail.csv",
                                     names=["sender_id", "slots", "latest_message", "latest_event_time",
                                            "followup_action", "paused", "events", "latest_input_channel",
                                            "active_form", "latest_action_name"
                                            ])
    return all_conv_detail_df


def append_dict_as_row(file_name, dict_of_elem, field_names):
    with open(file_name, 'a+', newline='') as write_obj:
        dict_writer = DictWriter(write_obj, fieldnames=field_names)
        dict_writer.writerow(dict_of_elem)


def export_conversation_detail():
    """
    Export all conversation detail to file so that we dont have to crawl everytime
    """
    file_name = "../chatlog_data/all_conv_detail.csv"
    all_conv = get_all_conv()
    all_sender_id = [x["sender_id"] for x in all_conv]
    for sender_id in all_sender_id:
        conversation_detail_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations/{}"
        token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
        if os.popen(token + " && " + conversation_detail_api.format(sender_id)).read() is not None:
            try:
                conversation_detail = json.loads(
                    os.popen(token + " && " + conversation_detail_api.format(sender_id)).read())
                field_names = list(conversation_detail.keys())
                append_dict_as_row(file_name, conversation_detail, field_names)
                logger.info("Add row to file")
            except Exception as ex:
                logger.error(ex)


def export_conversations():
    """
    Export all conversation to file so that we dont have to crawl everytime
    """
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations"
    token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
    all_conversations = json.loads(os.popen(token + " && " + conversation_api).read())
    with open('../chatlog_data/all_conv.pkl', 'wb') as f:
        pickle.dump(all_conversations, f)


def get_timestamp(timestamp: int, format: str):
    """

    :param timestamp:
    :param format: %Y-%m-%d %H:%M:%S
    :return:
    """
    readable_timestamp = datetime.datetime.utcfromtimestamp(timestamp).strftime(format)
    return readable_timestamp


def get_fb_conversations():
    month_list = ["01", "02", "03", "04", "05", "06", "07"]
    conversation_api = "curl -i -X GET \"https://graph.facebook.com/v6.0/" \
                       "1454523434857990?fields=conversations&" \
                       "access_token=EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD\""

    next_conversations_api = ""
    conversations_timestamp_year = "2020"
    first_time = True
    all_message_df = []
    while conversations_timestamp_year == "2020":
        if first_time:
            conversations = os.popen(conversation_api).read().replace("\n", " ")
            first_time = False
        else:
            conversations = os.popen('curl -i -X GET \"' + next_conversations_api + '\"').read().replace("\n", " ")

        conversations = json.loads(conversations.split(" ")[-1])

        try:
            conversations = conversations["conversations"]
        except:
            conversations = conversations

        conversations_timestamp = conversations["data"][-1]["updated_time"][:10]
        conversations_timestamp = time.mktime(
            datetime.datetime.strptime(conversations_timestamp, "%Y-%m-%d").timetuple())
        conversations_timestamp_year = get_timestamp(int(conversations_timestamp), "%Y")
        conversations_timestamp_month = get_timestamp(int(conversations_timestamp), "%m")
        next_conversations_api = conversations["paging"]["next"]
        if conversations_timestamp_month != "05":
            continue
        conversations_data = conversations["data"]
        for conversation in conversations_data:
            id = conversation["id"]
            updated_time = conversation["updated_time"]
            message_df = get_fb_converstaions_message(id, updated_time)
            if isinstance(message_df, pd.DataFrame):
                all_message_df.append(message_df)
            else:
                a = 0
    result = pd.concat(all_message_df)
    result.to_csv("../analyze_data/all_chat_fb/all_chat_fb_may.csv", index=False)
    return result


def get_fb_converstaions_message(conversation_id, updated_time):
    collect_info = {"sender_id": [],"sender_name":[], "user_message": [], "bot_message": [], "updated_time": [], "created_time": []}
    shop_name = 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp'
    message_api = "curl -i -X GET \"https://graph.facebook.com/v6.0/" \
                  "{id}/messages?" \
                  "fields=from,message,created_time&" \
                  "access_token=EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD\""
    message_api = message_api.format(id=conversation_id)
    messages = os.popen(message_api).read().replace("\n", " ")
    try:
        messages = literal_eval(messages[messages.index('{"data"'):])
        sender_id = ""
        for message in messages["data"]:
            message_from = message["from"]["name"]
            if message_from != shop_name:
                sender_id = message["from"]["id"]
                user_message = message["message"].encode('utf-16', 'surrogatepass').decode('utf-16')
                collect_info["sender_id"].append(sender_id)
                collect_info["user_message"].append(user_message)
                collect_info["bot_message"].append("bot")
                collect_info["updated_time"].append(updated_time)
                collect_info["created_time"].append(updated_time)
            else:
                bot_message = message["message"].encode('utf-16', 'surrogatepass').decode('utf-16')
                collect_info["sender_id"].append(sender_id)
                collect_info["user_message"].append("user")
                collect_info["bot_message"].append(bot_message)
                collect_info["updated_time"].append(updated_time)
                collect_info["created_time"].append(updated_time)
    except:
        collect_info["sender_id"].append("")
        collect_info["user_message"].append("")
        collect_info["bot_message"].append("")
        collect_info["updated_time"].append("")

    message_df = pd.DataFrame.from_dict(collect_info)
    return message_df


def add_outcome(uc_conversation_df, uc):
    """

    :param uc_conversation_df:
    :param uc: processing uc
    """
    # uc_conversation_df = pd.read_csv("analyze_data/uc3_conversation.csv")
    conversation_ids_list = list(set(uc_conversation_df["id"]))
    # key words để filter shipping/ order case
    # cho minh
    key_words = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk", "số tài khoản",
                 "gửi về"]
    filter_words = ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]

    # loop qua từng conversation_id  rồi sau đó lấy conversation đấy trong tập conversations theo id
    for conversation_id in conversation_ids_list:
        convesation_df = uc_conversation_df[uc_conversation_df["id"] == conversation_id]
        # đánh giá outcome của conversation theo turn cuối cùng
        last_turn = convesation_df.iloc[-1]
        last_turn_row_index = convesation_df.index.tolist()[-1]
        # lấy message của người và bot turn cuối cùng
        bot_message = last_turn["bot_message"]
        user_message = last_turn["message"]
        user_intent = last_turn["user_intent"]
        if user_intent is None:
            a = 0

        # dựa vào IC để lấy trường hợp cảm ơn
        if user_intent == "thank":
            uc_conversation_df.at[last_turn_row_index, "outcome"] = "thanks"
        elif any(key_word in user_message for key_word in key_words) and any(
                filter_word not in user_message for filter_word in filter_words):
            # dựa vào key words lấy trường hợp shipping order
            uc_conversation_df.at[last_turn_row_index, "outcome"] = "shipping_order"
        elif user_intent == "handover_to_inbox" or "handover_to_inbox" in bot_message:
            uc_conversation_df.at[last_turn_row_index, "outcome"] = "handover_to_inbox"
        elif bot_message == "Mình chưa xác định được món đồ bạn hỏi, bạn mô tả rõ hơn giúp mình nhé!" or user_intent == "disagree":
            uc_conversation_df.at[last_turn_row_index, "outcome"] = "cv_fail"
            uc_conversation_df.at[last_turn_row_index, "silent"] = 1
        else:
            uc_conversation_df.at[last_turn_row_index, "outcome"] = "other"
            uc_conversation_df.at[last_turn_row_index, "silent"] = 1
    uc_conversation_df = uc_conversation_df.drop_duplicates(subset=["timestamp", "sender_id", "message"],
                                                            keep="first")
    uc_conversation_df.to_csv("analyze_data/" + uc + "_conversation_with_outcome.csv", index=False)
    return uc_conversation_df


