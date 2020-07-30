import pandas as pd
from csv import DictWriter
import os
import json
import logging
import pickle
import datetime
import time
from ast import literal_eval
import json

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


def export_conversations():
    """
    Export all conversation to file so that we dont have to crawl everytime
    """
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations"
    token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
    all_conversations = json.loads(os.popen(token + " && " + conversation_api).read())
    with open('chatlog_data/all_conv.pkl', 'wb') as f:
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
    conversation_api = "curl -i -X GET \"https://graph.facebook.com/v6.0/1454523434857990?fields=conversations&access_token=EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD\""

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

        try:
            conversations = json.loads(conversations.split(" ")[-1])
        except:
            try:
                conversations = os.popen('curl -i -X GET \"' + next_conversations_api + '\"').read().replace("\n", " ")
                conversations = json.loads(conversations.split(" ")[-1])
            except:
                break

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
        if int(conversations_timestamp_month) < 3:
            break
        conversations_data = conversations["data"]
        for conversation in conversations_data:
            id = conversation["id"]
            updated_time = conversation["updated_time"]
            message_df = get_fb_converstaions_message(id, updated_time)
            all_message_df.append(message_df)

    result = pd.concat(all_message_df)
    result.to_csv("analyze_data/all_chat_fb/all_chat_fb_3.csv", index=False)
    return result


def get_fb_converstaions_message(conversation_id, updated_time):
    collect_info = {"message_id": [], "fb_conversation_id": [], "sender_id": [], "to_sender_id": [], "sender_name": [],
                    "user_message": [], "bot_message": [], "updated_time": [],
                    "created_time": [], "attachments": []}
    shop_name = 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp'
    message_api = "curl -i -X GET \"https://graph.facebook.com/v6.0/" \
                  "{id}/messages?" \
                  "fields=from,to,message,created_time,attachments&" \
                  "access_token=EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD\""
    message_api = message_api.format(id=conversation_id)
    messages = os.popen(message_api).read().replace("\n", " ")
    try:
        # messages = literal_eval(messages[messages.index('{"data"'):])
        messages = json.loads(messages[messages.index('{"data"'):])
        sender_id = ""
        to_sender_id = ""
        for message in messages["data"]:
            message_from = message["from"]["name"]
            message_id = message["id"]
            created_time = message["created_time"]
            created_time_date = created_time[:10]
            created_time_date = time.mktime(datetime.datetime.strptime(created_time_date, "%Y-%m-%d").timetuple())
            created_time_month = get_timestamp(int(created_time_date), "%m")
            created_time_year = get_timestamp(int(created_time_date), "%Y")
            if created_time_year == "2020" and created_time_month == "03":
                message_attachments = ""
                if 'attachments' in message:
                    attachments_data = message["attachments"]["data"]
                    try:
                        for item in attachments_data:
                            message_attachments += item["image_data"]["url"] +", "
                    except:
                        message_attachments = " "
                if "id" in message["from"]:
                    sender_id = message["from"]["id"]
                if "data" in message["to"] and len(message["to"]["data"]) > 0:
                    to_sender_id = message["to"]["data"][0]["id"]
                if message_from != shop_name:
                    user_message = message["message"].encode('utf-16', 'surrogatepass').decode('utf-16')
                    collect_info["sender_id"].append(sender_id)
                    collect_info["fb_conversation_id"].append(conversation_id)
                    collect_info["to_sender_id"].append(to_sender_id)
                    collect_info["message_id"].append(message_id)
                    collect_info["sender_name"].append(message_from)
                    collect_info["user_message"].append(user_message)
                    collect_info["bot_message"].append("bot")
                    collect_info["updated_time"].append(updated_time)
                    collect_info["created_time"].append(created_time)
                    collect_info["attachments"].append(message_attachments)
                else:
                    bot_message = message["message"].encode('utf-16', 'surrogatepass').decode('utf-16')
                    collect_info["sender_id"].append(sender_id)
                    collect_info["fb_conversation_id"].append(conversation_id)
                    collect_info["to_sender_id"].append(to_sender_id)
                    collect_info["message_id"].append(message_id)
                    collect_info["sender_name"].append(message_from)
                    collect_info["user_message"].append("user")
                    collect_info["bot_message"].append(bot_message)
                    collect_info["updated_time"].append(updated_time)
                    collect_info["created_time"].append(created_time)
                    collect_info["attachments"].append(message_attachments)
    except:
        collect_info["fb_conversation_id"].append(conversation_id)
        collect_info["sender_id"].append("")
        collect_info["to_sender_id"].append("")
        collect_info["message_id"].append("")
        collect_info["sender_name"].append("")
        collect_info["user_message"].append("")
        collect_info["bot_message"].append("")
        collect_info["updated_time"].append("")
        collect_info["created_time"].append("")
        collect_info["attachments"].append("")
    message_df = pd.DataFrame.from_dict(collect_info)
    return message_df


def get_message_attachments(message_id):
    message_attachments_api = "curl -i -X GET \"https://graph.facebook.com/v6.0/" \
                              "{id}/attachments?" \
                              "access_token=EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD\""
    message_attachments_api = message_attachments_api.format(id=message_id)
    message_attachments = os.popen(message_attachments_api).read().replace("\n", " ")
    try:
        message_attachments = literal_eval(message_attachments[message_attachments.index('{"data"'):])
        attachments = json.dumps(message_attachments["data"])
    except:
        attachments = " "

    return attachments


get_fb_conversations()
# export_conversations()
# export_conversation_detail()
