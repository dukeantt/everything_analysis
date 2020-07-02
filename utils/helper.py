import pandas as pd
from csv import DictWriter
import os
import json
import logging
import pickle
import datetime


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
