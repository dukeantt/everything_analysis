import pandas as pd
from rasa_chatlog_processor import RasaChalogProcessor


def main():
    df = pd.read_csv("all_chatlog_3_9.csv")
    df = df.sort_values(by=["created_time"])
    # df = df.drop(columns=['conversation_id', 'turn', 'conversation_begin_date', 'conversation_begin_time',"_id"])
    df = df.drop(columns=["_id"])

    df_rows_conversation_id = df["conversation_id"].to_list()
    conversation_id = df["conversation_id"].drop_duplicates(keep="first").to_list()
    order_conversation_id = list(range(1,1638))

    df_rows_order_conversation_id = []
    for id in df_rows_conversation_id:
        id_index = conversation_id.index(id)
        new_id = order_conversation_id[id_index]
        df_rows_order_conversation_id.append(new_id)
    # processor = RasaChalogProcessor()
    # df = processor.split_chatlog_to_conversations(df)
    # df = processor.split_chatlog_conversations_to_turns(df)
    a = 0
    df.insert(0, "new_conversation_id", df_rows_order_conversation_id)
    df = df.sort_values(by=["new_conversation_id", "turn"])
    df = df[df["sender_id"] != '3547113778635846']
    df.to_csv("fixed_data.csv", index=False)


main()
