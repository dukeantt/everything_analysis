from csv import DictWriter
import pandas as pd
import os
import json
from datetime import date, timedelta
import datetime
from ast import literal_eval
from pymongo import MongoClient
from data_cleaning import *
from rasa_chatlog_processor import RasaChalogProcessor
import logging

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

db_name = "rasa_chatlog_all_28_8"


def get_timestamp(timestamp: int, format: str):
    """

    :param timestamp:
    :param format: %Y-%m-%d %H:%M:%S
    :return:
    """
    readable_timestamp = datetime.datetime.fromtimestamp(timestamp).strftime(format)
    return readable_timestamp


def upload_all_rasa_chatlog_to_atlas_mongodb(chalog_all):
    chatlog_rasa = chalog_all
    date_list = [datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in list(chatlog_rasa["created_time"])]
    time_list = [datetime.datetime.strptime(x[11:], "%H:%M:%S") for x in list(chatlog_rasa["created_time"])]
    week_day = [datetime.datetime.strptime(x[:10], "%Y-%m-%d").weekday() for x in list(chatlog_rasa["created_time"])]
    chatlog_rasa.insert(9, "date", date_list)
    chatlog_rasa.insert(10, "conversation_time", time_list)
    chatlog_rasa.insert(11, "week_day", week_day)

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    # collection = db[db_name]
    collection = db["test_crawl_daily"]
    data_dict = chatlog_rasa.to_dict("records")

    # Insert collection
    collection.insert_many(data_dict)


def process_raw_rasa_chatlog(input_date, rasa_chatlog_daily: pd.DataFrame):
    logger.info("Get chatlog by month")
    rasa_conversation = rasa_chatlog_daily
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
            events = item["events"]
            if not isinstance(item["events"], list):
                events = literal_eval(item["events"])
        else:
            continue
        sender_id = item["sender_id"]
        # Get user and bot event
        user_bot_events = [x for x in events if x["event"] == "user" or x["event"] == "bot"]
        for event_index, event in enumerate(user_bot_events):
            timestamp = get_timestamp(int(event["timestamp"]), fmt)
            timestamp_month = get_timestamp(int(event["timestamp"]), "%m")
            timestamp_date = get_timestamp(int(event["timestamp"]), "%Y-%m-%d")
            message_id = ""
            user_intent = ""
            # if timestamp_month == input_month:
            if timestamp_date == input_date:
                entity_list = ""
                if "parse_data" in event:
                    if "entities" in event["parse_data"]:
                        entities = event["parse_data"]["entities"]
                        if entities:
                            for item in entities:
                                if "value" in item:
                                    if item["value"] is not None:
                                        entity_list += str(item["value"]) + ","
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

    return rasa_chatlog_df


def export_conversations(today, yesterday):
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations?start={yesterday}T17:00:00.000Z&until={today}T17:00:00.000Z"
    conversation_api = conversation_api.format(today=str(today), yesterday=str(yesterday))
    token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
    "https://babeshop.ftech.ai/api/conversations?limit=20&offset=0&tart=2020-08-18T17:00:00.000Z&until=2020-08-19T17:00:00.000Z&intent=&entity=&action=&policies=&tags_any=&exclude_self=true&with_tags=true"
    try:
        all_conversations = json.loads(os.popen(token + " && " + conversation_api).read())
    except:
        all_conversations = None
    return all_conversations


def export_conversation_detail(all_conv):
    all_sender_id = [x["sender_id"] for x in all_conv]
    counter = 0
    conversation_detail_list = []
    for sender_id in all_sender_id:
        counter += 1
        conversation_detail_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations/{}"
        token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
        response = os.popen(token + " && " + conversation_detail_api.format(sender_id)).read()
        if response is not None:
            try:
                conversation_detail = json.loads(response)
                conversation_detail_list.append(conversation_detail)
            except Exception as ex:
                logger.error(ex)
    conversation_detail_df = pd.DataFrame(conversation_detail_list)
    return conversation_detail_df


def clean_rasa_chatlog(rasa_chatlog):
    rasa_chatlog["user_message_correction"] = rasa_chatlog["user_message"]
    rasa_chatlog = remove_col_str(df=rasa_chatlog, col_name="user_message_correction")
    rasa_chatlog = deEmojify(df=rasa_chatlog, col_name="user_message_correction", og_col_name="user_message_correction")
    rasa_chatlog = correction_message(df=rasa_chatlog, col_name="user_message_correction",
                                      og_col_name="user_message_correction")
    rasa_chatlog = remove_col_white_space(df=rasa_chatlog, col_name="user_message_correction")
    return rasa_chatlog


def get_last_document_from_db():
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db[db_name]
    # last_document = [document for document in collection.find().limit(1).sort([('$natural', -1)])]
    # last_conversation_id = 0
    # if len(last_document) > 0:
    #     last_conversation_id = last_document[0]["conversation_id"]

    last_conversation_document = [document for document in collection.find({}, {'conversation_id': 1, '_id':0}).sort([('conversation_id',-1)]).limit(1)][0]
    last_conversation_id = last_conversation_document["conversation_id"]
    return last_conversation_id


def crawl_daily():
    today = date.today() - timedelta(2)
    yesterday = today - timedelta(1)
    month = str(today)[5:7]
    all_conversations = export_conversations(today, yesterday)
    all_conversation_detail = export_conversation_detail(all_conversations)
    rasa_chatlog_processed = process_raw_rasa_chatlog(str(today), all_conversation_detail)
    rasa_chatlog_clean = clean_rasa_chatlog(rasa_chatlog_processed)

    last_conversation_id = get_last_document_from_db()
    processor = RasaChalogProcessor()
    rasa_chatlog = processor.process_rasa_chatlog(rasa_chatlog_clean, last_conversation_id)
    upload_all_rasa_chatlog_to_atlas_mongodb(rasa_chatlog)


def crawl_daily_2():
    last_conversation_id = 1537
    for x in list(reversed(range(7, 15))):
        today = date.today() - timedelta(x)
        yesterday = today - timedelta(1)
        month = str(today)[5:7]
        all_conversations = export_conversations(today, yesterday)
        all_conversation_detail = export_conversation_detail(all_conversations)
        rasa_chatlog_processed = process_raw_rasa_chatlog(str(today), all_conversation_detail)
        rasa_chatlog_clean = clean_rasa_chatlog(rasa_chatlog_processed)

        processor = RasaChalogProcessor()
        rasa_chatlog = processor.process_rasa_chatlog(rasa_chatlog_clean, last_conversation_id)
        if len(rasa_chatlog) > 0:
             last_conversation_id = int(rasa_chatlog.iloc[-1]["conversation_id"])
        rasa_chatlog.to_csv(str(today)+"_full.csv", index=False)


crawl_daily_2()
