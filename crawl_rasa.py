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
from data_cleaning import *
from rasa_chatlog_processor import RasaChalogProcessor

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

date_map = {
    "01": "2020-01-31",
    "02": "2020-02-28",
    "03": "2020-03-31",
    "04": "2020-04-30",
    "05": "2020-05-31",
    "06": "2020-06-30",
    "07": "2020-07-31",
    "08": "2020-08-31",
    "09": "2020-09-30",
    "10": "2020-10-31",
    "11": "2020-11-30",
    "12": "2020-12-31",
}


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


def export_conversation_detail(all_conv, selected_month: str):
    """
    Export all conversation detail to file so that we dont have to crawl everytime
    """
    if selected_month is None:
        selected_month = ""
    file_name = "chatlog_data/all_conv_detail_" + selected_month + ".csv"
    # all_conv = get_all_conv()
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


def export_conversations(month_start: str, month_end: str):
    """
    Export all conversation to file so that we dont have to crawl everytime
    """
    conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations"
    if month_start and month_end:
        date_start = date_map[month_start]
        date_end = date_map[month_end]
        conversation_api = "curl -H \"Authorization: Bearer $TOKEN\" -s https://babeshop.ftech.ai/api/conversations?start=" + date_start + "T17:00:00.000Z&until=" + date_end + "T17:00:00.000Z"

    token = "TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{\"username\": \"me\", \"password\": \"w4J6OObi996nDGcQ4mlYNK4F\"}' | jq -r .access_token)"
    # token = "TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1c2VybmFtZSI6Im1lIiwiZXhwIjoxNTk2OTI2MTM1LCJ1c2VyIjp7InVzZXJuYW1lIjoibWUiLCJyb2xlcyI6WyJhZG1pbiJdLCJkYXRhIjpudWxsLCJhcGlfdG9rZW4iOiI0NjkxMzAxZjRkMGU3M2JiMDEwNDJjZmEyYWMyMzg1MzJlM2EzZWM1In0sInNjb3BlcyI6WyJhY3Rpb25QcmVkaWN0aW9uOmNyZWF0ZSIsImFjdGlvblByZWRpY3Rpb246Z2V0IiwiYWN0aW9uczpjcmVhdGUiLCJhY3Rpb25zOmRlbGV0ZSIsImFjdGlvbnM6Z2V0IiwiYWN0aW9uczp1cGRhdGUiLCJhbGxFdmFsdWF0aW9uczpjcmVhdGUiLCJhbGxFdmFsdWF0aW9uczpsaXN0IiwiYW5hbHl0aWNzOmdldCIsImJyYW5jaDp1cGRhdGUiLCJidWxrRGF0YTpnZXQiLCJidWxrRGF0YTp1cGRhdGUiLCJidWxrUmVzcG9uc2VUZW1wbGF0ZXM6dXBkYXRlIiwiYnVsa1N0b3JpZXM6Z2V0IiwiYnVsa1N0b3JpZXM6dXBkYXRlIiwiY2hhdFRva2VuOmdldCIsImNoYXRUb2tlbjp1cGRhdGUiLCJjbGllbnRFdmFsdWF0aW9uOmRlbGV0ZSIsImNsaWVudEV2YWx1YXRpb246Z2V0IiwiY2xpZW50RXZhbHVhdGlvbjp1cGRhdGUiLCJjbGllbnRFdmVudHM6Y3JlYXRlIiwiY2xpZW50RXZlbnRzOnVwZGF0ZSIsImNsaWVudE1lc3NhZ2VzOmxpc3QiLCJjbGllbnRzOmdldCIsImNvbW1pdDpjcmVhdGUiLCJjb25maWc6Z2V0IiwiY29udmVyc2F0aW9uQWN0aW9uczpsaXN0IiwiY29udmVyc2F0aW9uRW50aXRpZXM6bGlzdCIsImNvbnZlcnNhdGlvbkludGVudHM6bGlzdCIsImNvbnZlcnNhdGlvblBvbGljaWVzOmxpc3QiLCJjb252ZXJzYXRpb25UYWdzOmNyZWF0ZSIsImNvbnZlcnNhdGlvblRhZ3M6ZGVsZXRlIiwiY29udmVyc2F0aW9uVGFnczpsaXN0IiwiY29udmVyc2F0aW9uVGFnczp1cGRhdGUiLCJkb21haW46Z2V0IiwiZG9tYWluOnVwZGF0ZSIsImRvbWFpbldhcm5pbmdzOmdldCIsImVudGl0aWVzOmxpc3QiLCJlbnRpdHlfc3lub255bV92YWx1ZXM6Y3JlYXRlIiwiZW50aXR5X3N5bm9ueW1fdmFsdWVzOmRlbGV0ZSIsImVudGl0eV9zeW5vbnltX3ZhbHVlczp1cGRhdGUiLCJlbnRpdHlfc3lub255bXM6Y3JlYXRlIiwiZW50aXR5X3N5bm9ueW1zOmRlbGV0ZSIsImVudGl0eV9zeW5vbnltczpnZXQiLCJlbnRpdHlfc3lub255bXM6bGlzdCIsImVudGl0eV9zeW5vbnltczp1cGRhdGUiLCJlbnZpcm9ubWVudHM6bGlzdCIsImVudmlyb25tZW50czp1cGRhdGUiLCJleGFtcGxlczpjcmVhdGUiLCJleGFtcGxlczpkZWxldGUiLCJleGFtcGxlczpnZXQiLCJleGFtcGxlczpsaXN0IiwiZXhhbXBsZXM6dXBkYXRlIiwiZmVhdHVyZXM6dXBkYXRlIiwiaW50ZW50czpjcmVhdGUiLCJpbnRlbnRzOmRlbGV0ZSIsImludGVudHM6bGlzdCIsImludGVudHM6dXBkYXRlIiwibG9nczpjcmVhdGUiLCJsb2dzOmRlbGV0ZSIsImxvZ3M6Z2V0IiwibG9nczpsaXN0IiwibG9nczpsaXN0IiwibG9va3VwX3RhYmxlczpjcmVhdGUiLCJsb29rdXBfdGFibGVzOmRlbGV0ZSIsImxvb2t1cF90YWJsZXM6Z2V0IiwibG9va3VwX3RhYmxlczpsaXN0IiwibWVzc2FnZUZsYWdzOmRlbGV0ZSIsIm1lc3NhZ2VGbGFnczp1cGRhdGUiLCJtZXNzYWdlSW50ZW50czpkZWxldGUiLCJtZXNzYWdlSW50ZW50czp1cGRhdGUiLCJtZXNzYWdlczpjcmVhdGUiLCJtZXRhZGF0YTpjcmVhdGUiLCJtZXRhZGF0YTpnZXQiLCJtZXRhZGF0YTpsaXN0IiwibW9kZWxzOmNyZWF0ZSIsIm1vZGVsczpkZWxldGUiLCJtb2RlbHMuZXZhbHVhdGlvbnM6ZGVsZXRlIiwibW9kZWxzLmV2YWx1YXRpb25zOmdldCIsIm1vZGVscy5ldmFsdWF0aW9uczpsaXN0IiwibW9kZWxzLmV2YWx1YXRpb25zOnVwZGF0ZSIsIm1vZGVsczpnZXQiLCJtb2RlbHMuam9iczpjcmVhdGUiLCJtb2RlbHM6bGlzdCIsIm1vZGVscy5tb2RlbEJ5VGFnOmdldCIsIm1vZGVscy5zZXR0aW5nczpnZXQiLCJtb2RlbHMuc2V0dGluZ3M6dXBkYXRlIiwibW9kZWxzLnRhZ3M6ZGVsZXRlIiwibW9kZWxzLnRhZ3M6dXBkYXRlIiwibmxnUmVzcG9uc2U6Y3JlYXRlIiwicHJvamVjdHM6Y3JlYXRlIiwicmVnZXhlczpjcmVhdGUiLCJyZWdleGVzOmRlbGV0ZSIsInJlZ2V4ZXM6Z2V0IiwicmVnZXhlczpsaXN0IiwicmVnZXhlczp1cGRhdGUiLCJyZXBvc2l0b3JpZXM6Y3JlYXRlIiwicmVwb3NpdG9yaWVzOmRlbGV0ZSIsInJlcG9zaXRvcmllczpnZXQiLCJyZXBvc2l0b3JpZXM6bGlzdCIsInJlcG9zaXRvcmllczp1cGRhdGUiLCJyZXBvc2l0b3J5X3N0YXR1czpnZXQiLCJyZXNwb25zZVRlbXBsYXRlczpjcmVhdGUiLCJyZXNwb25zZVRlbXBsYXRlczpkZWxldGUiLCJyZXNwb25zZVRlbXBsYXRlczpsaXN0IiwicmVzcG9uc2VUZW1wbGF0ZXM6dXBkYXRlIiwicm9sZXM6Y3JlYXRlIiwicm9sZXM6ZGVsZXRlIiwicm9sZXM6Z2V0Iiwicm9sZXM6bGlzdCIsInJvbGVzOnVwZGF0ZSIsInJvbGVzLnVzZXJzOmxpc3QiLCJyb2xlcy51c2Vyczp1cGRhdGUiLCJzdGF0aXN0aWNzOmdldCIsInN0b3JpZXM6Y3JlYXRlIiwic3RvcmllczpkZWxldGUiLCJzdG9yaWVzOmdldCIsInN0b3JpZXM6bGlzdCIsInN0b3JpZXM6dXBkYXRlIiwidGVsZW1ldHJ5OmNyZWF0ZSIsInRlbGVtZXRyeTpkZWxldGUiLCJ0ZWxlbWV0cnk6Z2V0IiwidXNlcjpnZXQiLCJ1c2VyLnBhc3N3b3JkOnVwZGF0ZSIsInVzZXI6dXBkYXRlIiwidXNlci52YWx1ZXM6dXBkYXRlIiwidXNlckdvYWxzOmNyZWF0ZSIsInVzZXJHb2FsczpkZWxldGUiLCJ1c2VyczpjcmVhdGUiLCJ1c2VyczpkZWxldGUiLCJ1c2VyczpsaXN0IiwidXNlcnMucm9sZXM6ZGVsZXRlIiwidXNlcnMucm9sZXM6bGlzdCIsInVzZXJzLnJvbGVzOnVwZGF0ZSIsIndhcm5pbmdzOmdldCJdfQ.cjtSWyhI79lyI_Om31S22W5KRYlQ2CEf8nL-gYvnhYPH0ZFiOpE5XLM67RznbThdTaRujCoiwUjkkwDO3lHm_Eq5fHXNGy8xM1mErqGSYD4aeFY8gn4SbInOFVoWTkxviTOR6xwGLhajBx-xs4P4tiNS8Eht9DnJmIHxQ2_tMtkkgvfoCWbSg1z5EaZk1KpYQI5BQvhyQPdqyq1fmWUvgwUx1vyErJGOOjx0xRyxt-HLUJZsMvtJCEjniURmpgDxHnsa_N1z31vbwDW8h6wSBlN8GvCrRtUKnbNPhWgAn9uy2KIXFUfQuUhFCD1KFC74YGTWPfNdt8IorB7QNDupOg"
    try:
        all_conversations = json.loads(os.popen(token + " && " + conversation_api).read())
    except:
        all_conversations = None
    # with open('chatlog_data/all_conv.pkl', 'wb') as f:
    #     pickle.dump(all_conversations, f)
    return all_conversations


def get_timestamp(timestamp: int, format: str):
    """

    :param timestamp:
    :param format: %Y-%m-%d %H:%M:%S
    :return:
    """
    readable_timestamp = datetime.datetime.fromtimestamp(timestamp).strftime(format)
    return readable_timestamp


def process_raw_rasa_chatlog(input_month, rasa_chatlog_in_month: pd.DataFrame):
    """
        Get chatlog on specific month from raw chatlog from rasa

        :param month: string to indicate month ["01", "02", "03", "04", "05", "06", "07]
        :param raw_chatlog: path to raw chatlog csv file
        """
    logger.info("Get chatlog by month")
    rasa_conversation = rasa_chatlog_in_month
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
    output_file_path = "chatlog_data/rasa/rasa_chatlog_{month}.csv"
    output_file_path = output_file_path.format(month=input_month)
    rasa_chatlog_df.to_csv(output_file_path, index=False)
    return rasa_chatlog_df


def crawl_rasa_chatlog(month_start=None, month_end=None):
    all_conversations = export_conversations(month_start=month_start, month_end=month_end)
    if all_conversations is None:
        return None
    export_conversation_detail(all_conversations, selected_month=month_end)
    if month_end is None:
        month_end = ""
    file_name = "chatlog_data/all_conv_detail_" + month_end + ".csv"
    field_name = ['sender_id', 'slots', 'latest_message', 'latest_event_time', 'followup_action', 'paused',
                  'events',
                  'latest_input_channel', 'active_form', 'latest_action_name']

    all_conversations = pd.read_csv(file_name, names=field_name, header=None)
    return all_conversations


def upload_rasa_chatlog_to_db(month: str):
    df = pd.read_csv("chatlog_data/rasa_chatlog_" + month + ".csv")
    client = MongoClient("mongodb://127.0.0.1:47944/")
    db = client["uc_outcome_rasa_chatlog"]
    collection = db["rasa_chatlog_" + month.replace("0", "")]
    df.reset_index(inplace=True)
    data_dict = df.to_dict("records")
    collection.insert_many(data_dict)


def upload_all_rasa_chatlog_to_atlas_mongodb(chalog_all):
    chatlog_rasa = chalog_all
    # date_list = [datetime.datetime.strptime(x[:10], "%Y-%m-%d") for x in list(chatlog_rasa["created_time"])]
    date_list = [datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in list(chatlog_rasa["created_time"])]
    time_list = [datetime.datetime.strptime(x[11:], "%H:%M:%S") for x in list(chatlog_rasa["created_time"])]
    week_day = [datetime.datetime.strptime(x[:10], "%Y-%m-%d").weekday() for x in list(chatlog_rasa["created_time"])]
    chatlog_rasa.insert(9, "date", date_list)
    chatlog_rasa.insert(10, "conversation_time", time_list)
    chatlog_rasa.insert(11, "week_day", week_day)

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db['rasa_chatlog_all_18_8']
    # chatlog_rasa.reset_index(inplace=True)
    data_dict = chatlog_rasa.to_dict("records")

    # Insert collection
    collection.insert_many(data_dict)


def get_chatlog_from_db(from_date, to_date):
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db['rasa_chatlog_all_18_8']
    start = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(to_date, "%Y-%m-%d")

    time_start_morning = datetime.datetime.strptime("09:00:00", "%H:%M:%S")
    time_end_morning = datetime.datetime.strptime("12:05:00", "%H:%M:%S")

    time_start_afternoon = datetime.datetime.strptime("14:00:00", "%H:%M:%S")
    time_end_afternoon = datetime.datetime.strptime("17:05:00", "%H:%M:%S")

    # chatlog_df = pd.DataFrame([document for document in collection.find({'conversation_begin_date': {'$gte': start, '$lte': end, }})])
    chatlog_df = pd.DataFrame([document for document in collection.find({
        '$and': [
            {'conversation_begin_date': {'$gte': start, '$lte': end}},
            {'$or': [
                {'conversation_begin_time': {'$gte': time_start_morning, '$lte': time_end_morning}},
                {'conversation_begin_time': {'$gte': time_start_afternoon, '$lte': time_end_afternoon}},
            ]},
            {'week_day': {'$gte': 0, '$lte': 4}},
        ]
    })])

    chatlog_df = chatlog_df.drop(columns=["_id", "conversation_time", "conversation_begin_date", "week_day"])
    return chatlog_df


def main():
    month_list = ["01", "02", "03", "04", "05", "06", "07", "08"]

    # rasa_chatlog_in_month = crawl_rasa_chatlog()
    # field_name = ['sender_id', 'slots', 'latest_message', 'latest_event_time', 'followup_action', 'paused',
    #               'events',
    #               'latest_input_channel', 'active_form', 'latest_action_name']
    # file_name = "chatlog_data/all_conv_detail_.csv"
    # rasa_chatlog_in_month = pd.read_csv(file_name, names=field_name, header=None)
    # if rasa_chatlog_in_month is not None:
    #     for index, month in enumerate(month_list):
    #         if month != "01":
    #             process_raw_rasa_chatlog(input_month=month, rasa_chatlog_in_month=rasa_chatlog_in_month)

    # chatlog_list = []
    # for index, month in enumerate(month_list):
    #     if month != "01":
    #         print(month)
    #         chat_log = pd.read_csv("chatlog_data/rasa/rasa_chatlog_" + month + ".csv")
    #         chat_log["user_message_correction"] = chat_log["user_message"]
    #         chat_log = remove_col_str(df=chat_log, col_name="user_message_correction")
    #         chat_log = deEmojify(df=chat_log, col_name="user_message_correction", og_col_name="user_message_correction")
    #         chat_log = correction_message(df=chat_log, col_name="user_message_correction", og_col_name="user_message_correction")
    #         chat_log = remove_col_white_space(df=chat_log, col_name="user_message_correction")
    #         chatlog_list.append(chat_log)
    # chatlog_all = pd.concat(chatlog_list)
    # chatlog_all = chatlog_all.reset_index(drop=True)

    chatlog_all = get_chatlog_from_db("2020-08-01", "2020-08-14")
    processor = RasaChalogProcessor()
    chatlog_all = processor.process_rasa_chatlog(chatlog_all)

    conversation_ids = chatlog_all["conversation_id"].drop_duplicates(keep="first").to_list()
    trash_conversation_ids = []

    for id in conversation_ids:
        sub_df = chatlog_all[chatlog_all["conversation_id"] == id]
        number_of_turns = len(sub_df["turn"].drop_duplicates(keep="first").to_list())
        outcome = list(filter(lambda x: x != "", list(sub_df["outcome"])))[0]
        if outcome == "thank" and len(sub_df["turn"].drop_duplicates()) == 1:
            trash_conversation_ids.append(id)

    chatlog_all = chatlog_all[~chatlog_all["conversation_id"].isin(trash_conversation_ids)]
    upload_all_rasa_chatlog_to_atlas_mongodb(chatlog_all)


main()
