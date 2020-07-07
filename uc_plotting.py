import pandas as pd
import plotly.graph_objects as go
from ast import literal_eval
import datetime
from collections import Counter
from collections import OrderedDict

list_outcome = ["thanks", "shipping_order", "handover_to_inbox", "cv_fail", "other"]
list_outcome_value = []
uc2_df = pd.read_csv("analyze_data/uc2_conversation_with_outcome.csv")


def piechart():
    total_uc2_conversation = len(set(uc2_df["id"]))
    for item in list_outcome:
        len_outcome = len(uc2_df[uc2_df["outcome"] == item])
        list_outcome_value.append(len_outcome)
    no_customer_silent = len(uc2_df[uc2_df["silent"] == 1])
    fig = go.Figure(data=[go.Pie(
        labels=list_outcome,
        values=list_outcome_value,
        textinfo='label+value',
        hole=.2
    )])
    fig.show()


def main():
    user_messages = uc2_df["message"]
    detect_img_string = "scontent.xx.fbcdn.net"
    img_counter = 0
    for message in user_messages.tolist():
        if str(message) != "nan":
            no_image = message.count(detect_img_string)
            # if no_image > 1:
            #     a = 0
            img_counter += no_image

    conversations_outcome_other_ids = uc2_df[uc2_df["outcome"] == "other"]["id"].tolist()
    success_counter = 0
    detect_success_string_1 = "/confirm_object_type"
    for id in conversations_outcome_other_ids:
        sub_df = uc2_df[uc2_df["id"] == id]
        user_messages = sub_df["message"]
        bot_messages = sub_df["bot_message"]
        is_success = any(detect_success_string_1 in x for x in user_messages)
        if is_success:
            success_counter += 1
        a = 0


    a = 0


main()
