import pandas as pd
from csv import DictWriter
import os
import json
import logging
import pickle
import datetime
import time
from ast import literal_eval
from pymongo import MongoClient

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
    counter = 0
    for sender_id in all_sender_id:
        counter += 1
        conversation_detail_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations/{}"
        token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
        response = os.popen(token + " && " + conversation_detail_api.format(sender_id)).read()
        if response is not None:
            try:
                conversation_detail = json.loads(response)
                field_names = list(conversation_detail.keys())
                append_dict_as_row(file_name, conversation_detail, field_names)
                logger.info("Add row to file " + str(counter))
            except Exception as ex:
                logger.error(ex)


def export_conversations():
    """
    Export all conversation to file so that we dont have to crawl everytime
    """
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations?start=2020-07-31T17:00:00.000Z&until=2020-08-30T17:00:00.000Z"
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


def process_raw_rasa_chatlog(input_month):
    """
        Get chatlog on specific month from raw chatlog from rasa

        :param month: string to indicate month ["01", "02", "03", "04", "05", "06", "07]
        :param raw_chatlog: path to raw chatlog csv file
        """
    logger.info("Get chatlog by month")
    field_name = ['sender_id', 'slots', 'latest_message', 'latest_event_time', 'followup_action', 'paused',
                  'events',
                  'latest_input_channel', 'active_form', 'latest_action_name']
    rasa_conversation = pd.read_csv("chatlog_data/all_conv_detail.csv", names=field_name, header=None)
    df_data = {
        "message_id": [],
        "sender_id": [],
        "sender": [],
        "user_message": [],
        "bot_message": [],
        "intent": [],
        "entities": [],
        "created_time": [],
        "attachments": [],

    }
    fmt = "%Y-%m-%d %H:%M:%S"
    for rasa_index, item in rasa_conversation.iterrows():
        if item["events"] is not None and str(item["events"]) != "nan":
            events = literal_eval(item["events"])
        else:
            continue
        sender_id = item["sender_id"]
        # Get user and bot event
        user_bot_events = [x for x in events if x["event"] == "user" or x["event"] == "bot"]
        for event_index, event in enumerate(user_bot_events):
            timestamp = get_timestamp(int(event["timestamp"]), fmt)
            timestamp_month = get_timestamp(int(event["timestamp"]), "%m")
            message_id = ""
            user_intent = ""
            if timestamp_month == input_month:
                entity_list = ""
                if "parse_data" in event:
                    if "entities" in event["parse_data"]:
                        entities = event["parse_data"]["entities"]
                        if entities:
                            for item in entities:
                                if "value" in item:
                                    if item["value"] is not None:
                                        entity_list += item["value"] + ","
                    if "intent" in event["parse_data"]:
                        if "name" in event["parse_data"]["intent"]:
                            user_intent = event['parse_data']['intent']['name']

                if "message_id" in event:
                    message_id = event["message_id"]

                message = event["text"]
                attachments = ""
                if message is None:
                    message = ""

                if "scontent" in message:
                    messsage_list = message.split("\n")
                    text_message = ""
                    for item in messsage_list:
                        if "scontent" in item:
                            attachments += item + ", "
                        else:
                            text_message += item + " "
                    message = text_message

                df_data["entities"].append(entity_list)
                df_data["sender"].append(event["event"])
                df_data["intent"].append(user_intent)
                df_data["message_id"].append(message_id)
                df_data["sender_id"].append(sender_id)
                df_data["created_time"].append(timestamp)
                df_data["attachments"].append(attachments)

                event_owner = event["event"]
                if event_owner == "user":
                    df_data["user_message"].append(message)
                    df_data["bot_message"].append("")
                else:
                    df_data["user_message"].append("")
                    df_data["bot_message"].append(message)

    rasa_chatlog_df = pd.DataFrame.from_dict(df_data)
    output_file_path = "chatlog_data/rasa_chatlog_{month}.csv"
    output_file_path = output_file_path.format(month=input_month)
    rasa_chatlog_df.to_csv(output_file_path, index=False)
    return rasa_chatlog_df


def crawl_rasa_chatlog():
    export_conversations()
    export_conversation_detail()


def upload_rasa_chatlog_to_db(month: str):
    df = pd.read_csv("chatlog_data/rasa_chatlog_" + month + ".csv")
    client = MongoClient("mongodb://127.0.0.1:47944/")
    db = client["uc_outcome_rasa_chatlog"]
    collection = db["rasa_chatlog_" + month.replace("0", "")]
    df.reset_index(inplace=True)
    data_dict = df.to_dict("records")
    collection.insert_many(data_dict)


def upload_all_rasa_chatlog_to_atlas_mongodb():
    chatlog_rasa = pd.read_csv("chatlog_data/all_rasa_conversations.csv")

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db['rasa_chatlog_all']
    # chatlog_rasa.reset_index(inplace=True)
    data_dict = chatlog_rasa.to_dict("records")

    # Insert collection
    collection.insert_many(data_dict)


def main():
    crawl_rasa_chatlog()
    process_raw_rasa_chatlog("08")
    # upload_all_rasa_chatlog_to_atlas_mongodb()


main()
