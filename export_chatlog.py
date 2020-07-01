import pandas as pd
from ast import literal_eval
import datetime

all_convo = pd.read_csv("analyze_data/all_conversations_without_trash.csv")
success_convo = pd.read_csv("analyze_data/success_conversations.csv")

conversation_info = {"timestamp": [],
                     "conv_id": [],
                     "input_text": [],
                     # "image": [],
                     # "intent": [],
                     # "entities": [],
                     # "action_1": [],
                     "bot_text": []
                     }
all_success_sender_id = list(success_convo["sender_id"])
all_sender_id = list(all_convo["sender_id"])

# for sender_id in all_success_sender_id:
for sender_id in all_sender_id:
    if len(all_convo[all_convo["sender_id"] == sender_id]["events"]) == 0:
        continue
    else:
        test_conversation = literal_eval(all_convo[all_convo["sender_id"] == sender_id]["events"].values[0])
        user_events = [x for x in test_conversation if x["event"] == 'user']
        bot_events = [x for x in test_conversation if x["event"] == 'bot']
        loop = len(user_events)
        if len(user_events) < len(bot_events):
            loop = len(bot_events)

        user_message = [x['text'] for x in user_events]
        bot_message = [x['text'] for x in bot_events]

        for i in range(0, loop):
            timestamp = datetime.datetime.utcfromtimestamp(int(user_events[0]['timestamp'])).strftime('%Y-%m-%d, %H:%M:%S')
            image = ""
            action = ""
            try:
                input_text = user_message[i]
            except Exception as e:
                input_text = ""
            try:
                bot_text = bot_message[i]
            except Exception as e:
                bot_text = ""
            try:
                intent = user_events["parse_data"]["intent"]["name"]
            except:
                intent = ""
            conversation_info["timestamp"].append(timestamp)
            conversation_info["conv_id"].append(sender_id)
            conversation_info["input_text"].append(input_text)
            # conversation_info["entities"].append("")
            # conversation_info["image"].append(image)
            # conversation_info["intent"].append(intent)
            # conversation_info["action_1"].append(action)
            conversation_info["bot_text"].append(bot_text)
            a = 0

chatlog_df = pd.DataFrame.from_dict(conversation_info)
# chatlog_df.to_csv("analyze_data/success_conversation_log.csv", index=False)
chatlog_df.to_csv("analyze_data/raw_conversation_log.csv", index=False)
