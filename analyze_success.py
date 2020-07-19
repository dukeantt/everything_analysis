import datetime
from ast import literal_eval
import logging
from heuristic_correction import *
from utils.helper import *
from datetime import datetime

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def count_message_on_day(user_message, user_messages_in_conversation):
    timestamp_int = int(user_message["timestamp"])
    timestamp = datetime.datetime.utcfromtimestamp(timestamp_int).strftime('%Y-%m-%d')
    message_counter = 0
    for i in range(0, len(user_messages_in_conversation)):
        timestamp_int_2 = int(user_messages_in_conversation[i]["timestamp"])
        timestamp_2 = datetime.datetime.utcfromtimestamp(timestamp_int_2).strftime('%Y-%m-%d')
        if timestamp_2 == timestamp:
            message_counter += 1
    return message_counter


def get_success_conv(all_conv_detail):
    """

    :param all_conv_detail:
    :return: dataframe of success conversation

    """
    necessary_info = {"sender_id": [], "begin": [], "end": [], "message_timestamp": [], "message_timestamp_month": [],
                      "message_timestamp_date": [], "thank": [], "handover": [],
                      "message": [], "obj_type": [],
                      }
    conversation_id_to_remove = []
    for conversation in all_conv_detail.itertuples():
        events = literal_eval(conversation.events)
        begin_time = datetime.datetime.utcfromtimestamp(int(events[0]["timestamp"]))
        end_time = datetime.datetime.utcfromtimestamp(int(events[-1]["timestamp"]))
        user_messages_in_conversation = [x for x in events if x["event"] == "user"]
        if len(user_messages_in_conversation) > 1:
            for user_message in user_messages_in_conversation:
                user_intent = None
                if "name" in user_message["parse_data"]['intent']:
                    user_intent = user_message["parse_data"]['intent']["name"]
                # Count number of messages on the day
                message_count_on_day = count_message_on_day(user_message, user_messages_in_conversation)

                # We only care about the conversation that have 2 messages on day
                if user_intent == "thank" and len(user_messages_in_conversation) > 1:
                    if message_count_on_day < 2:
                        necessary_info["thank"].append(0)
                        necessary_info["message"].append(user_message["parse_data"]["text"])
                        conversation_id_to_remove.append(conversation.sender_id)
                    else:
                        # Get all intent: thank messages
                        necessary_info["thank"].append(1)
                        necessary_info["message"].append(user_message["parse_data"]["text"])
                else:
                    necessary_info["thank"].append(0)

                if user_intent == "handover_to_inbox" and len(user_messages_in_conversation) > 1:
                    if message_count_on_day < 2:
                        necessary_info["handover"].append(0)
                        necessary_info["message"].append(user_message["parse_data"]["text"])
                        conversation_id_to_remove.append(conversation.sender_id)
                    else:
                        # Get all intent: handover messages
                        necessary_info["handover"].append(1)
                        necessary_info["message"].append(user_message["parse_data"]["text"])
                else:
                    necessary_info["handover"].append(0)

                if user_intent not in ["handover_to_inbox", "thank"] or len(user_messages_in_conversation) <= 1:
                    necessary_info["message"].append("")
                obj_type = ""

                if "object_type" in literal_eval(conversation.slots):
                    obj_type = literal_eval(conversation.slots)["object_type"]
                necessary_info["obj_type"].append(obj_type)

                # 3 columns month, date, full timestamp -> easier to analyze later
                message_timestamp_month = datetime.datetime.utcfromtimestamp(int(user_message["timestamp"])).strftime(
                    '%Y-%m')
                message_timestamp_date = datetime.datetime.utcfromtimestamp(int(user_message["timestamp"])).strftime(
                    '%m-%d')
                message_timestamp = datetime.datetime.utcfromtimestamp(int(user_message["timestamp"])).strftime(
                    '%Y-%m-%d')

                necessary_info["message_timestamp_month"].append(message_timestamp_month)
                necessary_info["message_timestamp_date"].append(message_timestamp_date)
                necessary_info["message_timestamp"].append(message_timestamp)

                necessary_info["sender_id"].append(conversation.sender_id)
                necessary_info["begin"].append(begin_time)
                necessary_info["end"].append(end_time)

    necessary_info_df = pd.DataFrame.from_dict(necessary_info)
    # All conversation that have intent:thank is considered successful
    success_conv = necessary_info_df[necessary_info_df["thank"] == 1]

    key_word = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk", "số tài khoản"]
    all_handover_df = necessary_info_df[necessary_info_df["handover"] == 1]
    all_handover_df_sub = all_handover_df.copy()
    for index, item in all_handover_df.iterrows():
        message = item["message"].lower()
        message = message.replace("\n", ". ")
        message = do_correction(message)
        for word_index, word in enumerate(key_word):
            if word in message:
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
    return combine_df, conversation_id_to_remove


def get_conversation_case_by_month(months: list):
    """

    :param months:
    """
    all_conv_detail = get_all_conv_detail()
    df_data = {"id": [], "sender_id": [], "message": [], "timestamp": [], "user_intent": []}
    id = 0
    fmt = '%Y-%m-%d %H:%M:%S'
    checked_senderid = []
    for index, conv in all_conv_detail.iterrows():
        try:
            events = literal_eval(conv["events"])
        except:
            continue

        user_messages = [x for x in events if x["event"] == "user"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%m") in months]
        if len(user_messages) > 0:
            sender_id = conv["sender_id"]
            if sender_id not in checked_senderid:
                id += 1
            for index, user_message in enumerate(user_messages):
                current_timestamp = get_timestamp(user_message["timestamp"], fmt)
                df_data["id"].append(id)
                df_data["sender_id"].append(sender_id)
                df_data["message"].append(user_message["text"])
                df_data["timestamp"].append(current_timestamp)
                try:
                    intent = user_message["parse_data"]["intent"]["name"]
                except:
                    intent = " "
                df_data["user_intent"].append(intent)

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
    add_outcome(conversations_df, "abc")


def main():
    get_conversation_case_by_month(["07"])
    all_conv_detail = get_all_conv_detail()
    result_df1, conversation_id_to_remove = get_success_conv(all_conv_detail)
    result_df = pd.concat([result_df1])
    result_df = result_df.sort_values(by=["begin"], ascending=False).drop_duplicates(subset=["sender_id"], keep="first")
    conversation_id_to_remove = list(set(conversation_id_to_remove))
    result_sender_id_list = list(result_df["sender_id"])

    for item in conversation_id_to_remove:
        if item in result_sender_id_list:
            conversation_id_to_remove.remove(item)

    all_sender_id = all_conv_detail[~all_conv_detail["sender_id"].isin(conversation_id_to_remove)]
    all_sender_id = all_sender_id[~all_sender_id["sender_id"].isin(["default", "me", "abcdef", "123456"])]
    all_sender_id.to_csv("analyze_data/all_conversations_without_trash.csv", index=False)

    result_df = result_df[~result_df["sender_id"].isin(["default", "me", "abcdef", "123456"])]
    result_df.to_csv("analyze_data/success_conversations.csv", index=False)




main()

# chatlog from 18/12/2019 to 19/6/2020
# test_sender_id = ["default", "me", "abcdef", "123456"]

