import pandas as pd
import numpy as np
import json
import os
import datetime
import pickle
from csv import DictWriter
from ast import literal_eval
import logging

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_all_conv():
    with open('chatlog_data/all_conv.pkl', 'rb') as f:
        data = pickle.load(f)
    return data


def get_all_conv_detail():
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


def export_conversations():
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations"
    token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
    all_conversations = json.loads(os.popen(token + " && " + conversation_api).read())
    with open('chatlog_data/all_conv.pkl', 'wb') as f:
        pickle.dump(all_conversations, f)


def export_conversation_detail():
    file_name = "chatlog_data/all_conv_detail.csv"
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

        a = 0


def extract_info_from_conversation(conversation_detail, info: dict):
    """

    :param conversation_detail:
    :param info:
    :return: info dictionary
    """
    begin_time = datetime.datetime.utcfromtimestamp(int(conversation_detail["events"][0]["timestamp"]))
    end_time = datetime.datetime.utcfromtimestamp(int(conversation_detail["events"][-1]["timestamp"]))

    user_messages_in_conversation = [x for x in conversation_detail["events"] if x["event"] == "user"]
    if user_messages_in_conversation[-1]["parse_data"]['intent']["name"] == "thank":
        info["thank"].append(1)
    else:
        info["thank"].append(0)
    info["sender_id"].append("a")
    info["begin"].append(begin_time)
    info["end"].append(end_time)
    return info


def get_success_conv(all_conv_detail, level:int):
    necessary_info = {"sender_id": [], "begin": [], "end": [], "thank": [], "handover": [], "message": []}
    for conversation in all_conv_detail.itertuples():
        events = literal_eval(conversation.events)
        begin_time = datetime.datetime.utcfromtimestamp(int(events[0]["timestamp"]))
        end_time = datetime.datetime.utcfromtimestamp(int(events[-1]["timestamp"]))
        user_messages_in_conversation = [x for x in events if x["event"] == "user"]
        # if len(user_messages_in_conversation) > 0:
        if len(user_messages_in_conversation) > (level - 1):
            user_intent = None
            if "name" in user_messages_in_conversation[-level]["parse_data"]['intent']:
                user_intent = user_messages_in_conversation[-level]["parse_data"]['intent']["name"]
            if user_intent == "thank" and len(user_messages_in_conversation) > 1:
                necessary_info["thank"].append(1)
                necessary_info["message"].append(user_messages_in_conversation[-level]["parse_data"]["text"])
            else:
                necessary_info["thank"].append(0)


            if user_intent == "handover_to_inbox" and len(user_messages_in_conversation) > 1:
                necessary_info["handover"].append(1)
                necessary_info["message"].append(user_messages_in_conversation[-level]["parse_data"]["text"])
            else:
                necessary_info["handover"].append(0)

            if user_intent not in ["handover_to_inbox", "thank"] or len(user_messages_in_conversation) <= 1:
                necessary_info["message"].append("")

            necessary_info["sender_id"].append(conversation.sender_id)
            necessary_info["begin"].append(begin_time)
            necessary_info["end"].append(end_time)

    necessary_info_df = pd.DataFrame.from_dict(necessary_info)
    success_conv = necessary_info_df[necessary_info_df["thank"] == 1]
    key_word = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk", "số tài khoản"]
    all_handover_df = necessary_info_df[necessary_info_df["handover"] == 1]
    all_handover_df_sub = all_handover_df.copy()
    for index, item in all_handover_df.iterrows():
        for word_index, word in enumerate(key_word):
            if word in item["message"].lower():
                break
            else:
                if word_index == len(key_word) - 1:
                    try:
                        all_handover_df_sub = all_handover_df_sub.drop(index)
                    except Exception as ex:
                        logger.error(ex)
    filter_word = ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]
    for word in filter_word:
        all_handover_df_sub = all_handover_df_sub[~all_handover_df_sub["message"].str.lower().str.contains(word)]
    combine_df = pd.concat([all_handover_df_sub, success_conv])
    return combine_df

def main():
    all_conv_detail = get_all_conv_detail()
    result_df1 = get_success_conv(all_conv_detail, level=1)
    result_df2 = get_success_conv(all_conv_detail, level=2)
    result_df3 = get_success_conv(all_conv_detail, level=3)
    result_df = pd.concat([result_df3, result_df2, result_df1])
    result_df = result_df.sort_values(by="begin", ascending=False).drop_duplicates(subset=["sender_id"], keep="first")
    a = 0

# export_conversations()
# export_conversation_detail()
main()

# chatlog from 18/12/2019 to 19/6/2020
# ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng","đ/c", "thanh toán", "tổng", "ck", "chuyển khoản"]
# ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]