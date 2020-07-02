from utils.helper import *
from ast import literal_eval
import pandas as pd
from datetime import datetime

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
        time_date = []
        for i in range(0,len(bot_events)):
            try:
                bot_text = bot_events[i]["text"]
            except:
                continue
            if bot_text is None:
                continue
            timestamp = get_timestamp(bot_events[i]["timestamp"], "%Y-%m-%d")
            timestamp_time = get_timestamp(bot_events[i]["timestamp"], "%H:%M:%S")
            if i in user_events:
                user_message = user_events[i]["parse_data"]["text"]
            else:
                user_message = "no message"
            all_conversations.append((sender_id, timestamp, user_message, timestamp_time))

            # if timestamp not in time_date:
            #     time_date.append(timestamp)
        a = 0
    all_conversations_df = pd.DataFrame(all_conversations,columns=["sender_id", "timestamp", "user_message", "timestamp_date"])
    all_conversations_df_group = all_conversations_df.groupby(["sender_id","timestamp"]).size().to_frame("turns").reset_index()
    return all_conversations_df_group


def get_avg_respond_time(all_conv_detail):
    turn_take_much_time = []
    turns_counter = 0
    total_response_time = 0
    all_response_time = []
    format = '%H:%M:%S'

    for sender_info in all_conv_detail.itertuples():
        sender_id = sender_info.sender_id
        events = literal_eval(sender_info.events)
        user_and_bot_events = [x for x in events if x["event"] in ["user", "bot"]]
        time_date = []
        for i in range(0, len(user_and_bot_events)):
            current_event = user_and_bot_events[i]
            current_event_type = current_event["event"]
            if current_event_type == "user":
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
    turn_take_much_time_df = pd.DataFrame(turn_take_much_time, columns=["sender_id", "user_text", "bot_text", "response_time"])
    turn_take_much_time_df.to_csv("analyze_data/turns_take_much_time.csv", index=False)
    return 0


def main():
    all_conv_detail = get_all_conv_detail()
    all_conv_detail = all_conv_detail[~all_conv_detail["sender_id"].isin(trash_sender_id)]

    get_avg_respond_time(all_conv_detail)

    count_conversations_df = count_conversations(all_conv_detail)
    no_conversations = len(count_conversations_df) - 1  # boi vi conversation '1454523434857990' co van de
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
