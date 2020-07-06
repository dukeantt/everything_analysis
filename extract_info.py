from utils.helper import *
from ast import literal_eval
import pandas as pd
from datetime import datetime
from heuristic_correction import *

trash_sender_id = ["default", "me", "abcdef", "123456"]


def get_all_line_chat(all_conv_detail):
    """
    Tong so line chat da serve tuong duong voi tong so line chat cua bot
    :param all_conv_detail:
    :return:
    """
    all_bot_message = []
    for sender_info in all_conv_detail.itertuples():
        sender_id = sender_info.sender_id
        events = literal_eval(sender_info.events)
        bot_events = [x for x in events if x["event"] == "bot"]
        for bot_event in bot_events:
            timestamp = get_timestamp(bot_event["timestamp"], "%Y-%m-%d")
            try:
                bot_text = bot_event["text"]
            except Exception as e:
                bot_text = "not found"
            all_bot_message.append((sender_id, bot_text, timestamp))
    all_bot_message_df = pd.DataFrame(all_bot_message, columns=["sender_id", "bot_text", "timestamp"])
    return all_bot_message_df


def count_conversations(all_conv_detail):
    """
    Tong so conversation cua moi khach hang se la số ngày mà khách ntin cho shop
    vd khach nhan vào 20 21 22/6 nghia la co 3 conversations
    :param all_conv_detail:
    :return:
    """
    all_conversations = []

    for sender_info in all_conv_detail.itertuples():
        sender_id = sender_info.sender_id
        events = literal_eval(sender_info.events)
        user_events = [x for x in events if x["event"] == "user"]
        bot_events = [x for x in events if x["event"] == "bot"]
        for user_event in user_events:
            user_message = user_event["text"]
            timestamp_month = get_timestamp(user_event["timestamp"], "%m")
            # if timestamp_month not in ["03", "04", "05", "06"]:
            if timestamp_month not in ["04", "05", "06"]:
                continue
            timestamp = get_timestamp(user_event["timestamp"], "%Y-%m-%d")
            timestamp_time = get_timestamp(user_event["timestamp"], "%H:%M:%S")
            try:
                user_intent = user_event["parse_data"]["intent"]["name"]
            except:
                user_intent = " "

            all_conversations.append((sender_id, timestamp, user_message, user_intent, timestamp_time))

    all_conversations_df = pd.DataFrame(all_conversations, columns=["sender_id", "timestamp", "user_message", "user_intent", "timestamp_time"])
    count_start_conversation(all_conversations_df)

    all_conversations_df_group = all_conversations_df.groupby(["sender_id", "timestamp"]).size().to_frame("turns").reset_index()
    conversations_division = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0, "7": 0, "8": 0, "9": 0, ">=10": 0}
    sender_ids = list(set(all_conversations_df["sender_id"]))
    for sender_id in sender_ids:
        no_conversation = len(all_conversations_df_group[all_conversations_df_group["sender_id"] == sender_id])
        if no_conversation >=10:
            conversations_division[">=10"] += 1
        else:
            conversations_division[str(no_conversation)] += 1


    return all_conversations_df_group


def count_start_conversation(all_conversations_df):
    no_get_started = all_conversations_df[all_conversations_df["user_message"] == '/start_conversation']
    no_greet = all_conversations_df[all_conversations_df["user_intent"] == "greet"]

    no_get_started_group = no_get_started.groupby(["sender_id", "timestamp"]).size().to_frame("times").reset_index()
    no_greet_group = no_greet.groupby(["sender_id", "timestamp"]).size().to_frame("times").reset_index()

    no_get_started_more_than_one = no_get_started_group[no_get_started_group["times"] > 1]
    no_greet_more_than_one = no_greet_group[no_greet_group["times"] > 1]

    no_get_started_more_than_one_sender_id = list(set(no_get_started_more_than_one["sender_id"]))
    no_greet_more_than_one_sender_id = list(set(no_greet_more_than_one["sender_id"]))

    special_case_sender_id = no_get_started_more_than_one_sender_id + no_greet_more_than_one_sender_id
    sender_id_dict = {}
    for sender_id in special_case_sender_id:
        row_df = all_conversations_df[all_conversations_df["sender_id"] == sender_id]
        row_start = row_df[row_df["user_message"] == '/start_conversation'].reset_index()
        row_greet = row_df[row_df["user_intent"] == "greet"].reset_index()
        if len(row_start) > 1:
            counter = 0
            set_df = row_start
            for index, item in set_df.iterrows():
                current_timestamp = item["timestamp"] + " " + item["timestamp_time"]
                fmt = '%Y-%m-%d %H:%M:%S'
                try:
                    next_timestamp = set_df.iloc[index + 1]["timestamp"] + " " + set_df.iloc[index + 1][
                        "timestamp_time"]
                    current_timestamp = datetime.strptime(current_timestamp, fmt)
                    next_timestamp = datetime.strptime(next_timestamp, fmt)
                    time_diff = (next_timestamp - current_timestamp).total_seconds()
                    if time_diff >= 900:
                        counter += 1
                except:
                    break
            counter += 1
            no_get_started_group.loc[no_get_started_group["sender_id"] == sender_id, "times"] = counter


        if len(row_greet) > 1:
            counter = 0
            set_df = row_greet
            for index, item in set_df.iterrows():
                current_timestamp = item["timestamp"] + " " + item["timestamp_time"]
                fmt = '%Y-%m-%d %H:%M:%S'
                try:
                    next_timestamp = set_df.iloc[index + 1]["timestamp"] + " " + set_df.iloc[index + 1][
                        "timestamp_time"]
                    current_timestamp = datetime.strptime(current_timestamp, fmt)
                    next_timestamp = datetime.strptime(next_timestamp, fmt)
                    time_diff = (next_timestamp - current_timestamp).total_seconds()
                    if time_diff >= 900:
                        counter += 1
                except:
                    break
            counter += 1
            no_greet_group.loc[no_greet_group["sender_id"] == sender_id, "times"] = counter

    a = 0


def get_avg_respond_time(all_conv_detail):
    turn_take_much_time = []
    turns_counter = 0
    total_response_time = 0
    all_response_time = []
    format = '%H:%M:%S'

    success_events = []

    for sender_info in all_conv_detail.itertuples():
        sender_id = sender_info.sender_id
        events = literal_eval(sender_info.events)
        user_and_bot_events = [x for x in events if x["event"] in ["user", "bot"]]
        time_date = []
        for i in range(0, len(user_and_bot_events)):
            current_event = user_and_bot_events[i]
            current_event_type = current_event["event"]
            if current_event_type == "user" and current_event["text"] is not None:
                timestamp_month = get_timestamp(current_event["timestamp"], "%m")
                if timestamp_month not in ["03", "04", "05", "06"]:
                    continue
                message = current_event["text"].lower()
                message = message.replace("\n", ". ")
                message = do_correction(message)
                key_word = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk",
                            "số tài khoản"]
                # for word in key_word:
                #     if word in message:
                #         timestamp = get_timestamp(int(current_event["timestamp"]), "%Y-%m-%d")
                #         timestamp_time = get_timestamp(int(current_event["timestamp"]), "%H:%M:%S")
                #         success_events.append((sender_id, timestamp, timestamp_time, message))
                #         break

                if i+1 < len(user_and_bot_events) and user_and_bot_events[i+1]["text"] is not None:
                    next_event = user_and_bot_events[i+1]
                    next_event_type = next_event["event"]
                    if next_event_type == "bot":
                        timestamp_user = get_timestamp(int(current_event["timestamp"]), format)
                        timestamp_bot = get_timestamp(int(next_event["timestamp"]), format)

                        tdelta = datetime.strptime(timestamp_bot, format) - datetime.strptime(timestamp_user, format)
                        tdelta_seconds = tdelta.total_seconds()
                        total_response_time += tdelta_seconds
                        if tdelta_seconds >= 10.0:
                            turn_take_much_time.append((sender_id, current_event["text"], next_event["text"], tdelta_seconds))
                        all_response_time.append(tdelta_seconds)
                        turns_counter += 1
                    else:
                        continue
                elif i+2 <len(user_and_bot_events) and user_and_bot_events[i+2]["text"] is not None:
                    next_event = user_and_bot_events[i+2]
                    next_event_type = next_event["event"]
                    if next_event_type == "bot":
                        timestamp_user = get_timestamp(int(current_event["timestamp"]), format)
                        timestamp_bot = get_timestamp(int(next_event["timestamp"]), format)

                        tdelta = datetime.strptime(timestamp_bot, format) - datetime.strptime(timestamp_user, format)

                        tdelta_seconds = tdelta.total_seconds()
                        total_response_time += tdelta_seconds
                        all_response_time.append(tdelta_seconds)
                        if tdelta_seconds >= 10.0:
                            turn_take_much_time.append((sender_id, current_event["text"], next_event["text"], tdelta_seconds))
                        turns_counter += 1
                    else:
                        continue
            else:
                continue
    success_events_df = pd.DataFrame(success_events, columns=["sender_id", "timestamp","timestamp_time", "message"])
    filter_word = ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]
    for word in filter_word:
        success_events_df = success_events_df[~success_events_df["message"].str.lower().str.contains(word)]
    success_events_df = success_events_df.drop_duplicates(subset=["sender_id", "message"])
    success_events_df = success_events_df.drop_duplicates(subset=["sender_id", "timestamp"])
    turn_take_much_time_df = pd.DataFrame(turn_take_much_time, columns=["sender_id", "user_text", "bot_text", "response_time"])
    turn_take_much_time_df.to_csv("analyze_data/turns_take_much_time.csv", index=False)
    return 0


def main():
    # all_conv_detail = get_all_conv_detail()
    all_conv_detail = pd.read_csv("analyze_data/all_conversations_without_trash.csv")
    all_conv_detail = all_conv_detail[~all_conv_detail["sender_id"].isin(trash_sender_id)]

    # get_avg_respond_time(all_conv_detail)

    count_conversations_df = count_conversations(all_conv_detail)
    no_conversations = len(count_conversations_df)



    all_turns = list(count_conversations_df["turns"])
    average_turn = sum(all_turns) / len(all_turns)

    sender_ids = list(set(list(all_conv_detail["sender_id"])))
    total_customer = len(sender_ids)
    all_bot_message_df = get_all_line_chat(all_conv_detail)
    all_bot_message_df = all_bot_message_df[~all_bot_message_df["bot_text"].isnull()]
    total_line_serve = len(all_bot_message_df)
    a = 0


if __name__ == '__main__':
    main()
