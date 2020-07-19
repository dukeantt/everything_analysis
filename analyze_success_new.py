import datetime
from ast import literal_eval
import logging
from heuristic_correction import *
from utils.helper import *
from datetime import datetime


def get_conversation_case_by_month(months: list):
    """

    :param months:
    """
    all_conv_detail = get_all_conv_detail()
    df_data = {"id": [],
               "sender_id": [],
               "message": [],
               "bot_message": [],
               "timestamp": [],
               "bot_timestamp": [],
               "user_intent": []}
    id = 0
    fmt = '%Y-%m-%d %H:%M:%S'
    checked_senderid = []
    # loop all conversations
    for index, conv in all_conv_detail.iterrows():
        try:
            events = literal_eval(conv["events"])
        except:
            continue
        # get all user and bot event in specific month
        user_messages = [x for x in events if x["event"] == "user" or x["event"] == "bot"]
        user_messages = [x for x in user_messages if get_timestamp(int(x["timestamp"]), "%m") in months]
        if len(user_messages) > 0:
            sender_id = conv["sender_id"]
            # set id for each conversation
            if sender_id not in checked_senderid:
                id += 1
            # loop all user and bot events
            # first event always user
            for index, user_message in enumerate(user_messages):
                next_user_event_index = 0
                # if current event is bot then skip to next event
                if user_message["event"] == "bot":
                    continue
                # check if next event is bot event
                if index +1 < len(user_messages) and user_messages[index+1]["event"] == "bot":
                    if user_messages[index + 1]["text"] is None:
                        df_data["bot_message"].append(" ")
                    else:
                        df_data["bot_message"].append(user_messages[index + 1]["text"])
                    df_data["bot_timestamp"].append(get_timestamp(user_messages[index + 1]["timestamp"], fmt))
                    next_user_event_index = index + 2
                else:
                    df_data["bot_message"].append(" ")
                    df_data["bot_timestamp"].append(" ")

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
                # find next user event
                while  next_user_event_index < len(user_messages) and user_messages[next_user_event_index]["event"] != "user":
                    next_user_event_index += 1
                try:
                    next_timestamp = get_timestamp(user_messages[next_user_event_index]["timestamp"], fmt)
                    next_timestamp = datetime.strptime(next_timestamp, fmt)
                except:
                    break

                time_diff = (next_timestamp - current_timestamp).total_seconds()
                if time_diff > 900:
                    id += 1
    conversations_df = pd.DataFrame.from_dict(df_data)
    conversations_df.insert(7, 'outcome', [" "] * len(conversations_df))

    months = "_".join(months)
    conversations_df = add_outcome(conversations_df, months)
    a = 0


def main():
    get_conversation_case_by_month(["07"])


main()