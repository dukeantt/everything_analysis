import pandas as pd
import time
import pickle
from denver.datasets.ic_dataset import ICDataset
from denver.trainers.language_model_trainer import LanguageModelTrainer
from denver.models.ic import ULMFITClassifier
from denver.trainers.trainer import ModelTrainer
from denver.models.ner import FlairSequenceTagger
from pre_process_all_chatlog_fb import *
import unicodedata

def get_conversation_turn_without_and_with_image(chatlog_df: pd.DataFrame):
    start_time = time.time()

    conversation_turn_list_without_image = []
    conversation_turn_list_with_image = []
    conversation_ids = chatlog_df["conversation_id"].drop_duplicates(keep="first").to_list()
    for id in conversation_ids:
        sub_df = chatlog_df[chatlog_df["conversation_id"] == id]
        turns = sub_df["turn"].drop_duplicates(keep="first").to_list()
        for turn in turns:
            attachments = sub_df.loc[sub_df["turn"] == turn, "attachments"].dropna().to_list()
            if len(attachments) == 0:
                conversation_turn_list_without_image.append((id, turn))
            else:
                conversation_turn_list_with_image.append((id, turn))
    print("Get conversation turn without image: " + str(time.time() - start_time))
    return conversation_turn_list_without_image, conversation_turn_list_with_image


def specify_uc4(chatlog_df, conversation_turn_pair_without_img, model_ner):
    chatlog_df.insert(2, "uc4", "")
    attributes = ["age_of_use", "guarantee", "color", "material", "origin", "promotion", "size", "weight", "brand", "price"]
    CON = ['còn', ' con ', ' conf ']
    KHONG = ['không', ' k ', ' ko ', ' hem ', ' hok ', ' khg ', 'khoong']

    uc4_base_require_entities = ["object_type", "mention"]

    filter_word = ["ship", "shopee", "shoppee", "freeship", "shope"]
    for pair in conversation_turn_pair_without_img:
        conversation_id = pair[0]
        turn = pair[1]
        sub_df = chatlog_df[(chatlog_df["conversation_id"] == conversation_id) & (chatlog_df["turn"] == turn) & (
                ~chatlog_df["user_message"].isin(["nan", "user"]))].reset_index()
        if len(sub_df) > 0:
            for item_index in range(0, len(sub_df)):
                user_message = sub_df.at[item_index, "user_message_clean"]
                user_message = unicodedata.normalize("NFC", str(user_message))
                user_message = user_message.lower()
                message_index = sub_df.at[item_index, "index"]
                con = [word for word in CON if (word in user_message)]
                khong = [word for word in KHONG if (word in user_message)]
                if str(user_message) == "nan" or str(user_message) == "user":
                    continue
                ner_output = model_ner.process(sample=user_message)
                entities = [x["entity"] for x in ner_output]
                prod_attribute = [x["entity"] for x in ner_output if x["entity"] in attributes]

                if con and khong and entities:
                    # if any(x in entities for x in uc4_base_require_entities) and all(x not in user_message for x in filter_word):
                    if any(x in entities for x in uc4_base_require_entities):
                        if len(prod_attribute) == 0:
                            chatlog_df.at[message_index, "uc4"] = "uc_s4.1"
                        elif len(prod_attribute) == 1:
                            chatlog_df.at[message_index, "uc4"] = "uc_s4.2"
                        else:
                            chatlog_df.at[message_index, "uc4"] = "uc_s4.3"
    return chatlog_df

def specify_usecase():
    # do_process()
    with open("models/model_ner.pkl", "rb") as model_file:
        model_ner = pickle.load(model_file)
    # for month in range(1, 7):
    for month in [2]:
        print(month)
        start_time = time.time()
        input_file = "data/chatlog_fb/processed_chatlog/all_chat_fb_{month}.csv"
        output_file = "data/chatlog_fb/result/all_chat_fb_{month}.csv"

        chatlog_df = pd.read_csv(input_file.format(month=str(month)))
        conversation_turn_pair_without_img, conversation_turn_pair_with_img = get_conversation_turn_without_and_with_image(
            chatlog_df)
        chatlog_df = specify_uc4(chatlog_df, conversation_turn_pair_without_img, model_ner)

        # chatlog_df.insert(2, "use_case", "")
        #
        # uc_s4_necessary_attribute = ["object_type"]
        # uc_s51_necessary_attribute = ["object_type", "price"]
        # all_attribute = ["attribute", "age_of_use", "guarantee", "color", "material", "origin", "promotion",
        #                  "size", "weight", "brand",
        #                  # "price"
        #                  ]
        # filter_word_for_price_case = ["ship", "số điện thoại"]
        # con_variation = ['con', 'còn', 'conf', 'cofn']
        # khong_variation = ['không', 'k', 'ko', 'hem', 'hok', 'khg', 'khoong']
        # price = ["giá", "tiền", "nhiêu", "bao nhiêu"]

        # for item in conversation_turn_pair_without_img:
        #     conversation_id = item[0]
        #     turn = item[1]
        #     sub_df = chatlog_df[(chatlog_df["conversation_id"] == conversation_id) & (chatlog_df["turn"] == turn) & (
        #             chatlog_df["user_message"] != "user")].reset_index()
        #     if len(sub_df) > 0:
        #         for item_index in range(0, len(sub_df)):
        #             user_message = sub_df.at[item_index, "user_message_clean"]
        #             message_index = sub_df.at[item_index, "index"]
        #
        #             if str(user_message) == "nan" or str(user_message) == "user":
        #                 continue
        #
        #             if (all(x in user_message for x in ["còn", "không"]) and str(user_message).index("còn") < str(
        #                     user_message).index("không")) or any(x in user_message for x in price):
        #                 ner_output = model_ner.process(sample=user_message)
        #                 entities = [x["entity"] for x in ner_output]
        #
        #                 if len(entities) == 0:
        #                     continue
        #                 entity_values = [x["value"] for x in ner_output]
        #                 if any(x in entities for x in uc_s4_necessary_attribute) and all(
        #                         x in user_message for x in ["còn", "không"]):
        #                     chatlog_df.at[message_index, "use_case"] = "uc_s4"
        #                 elif not any(x in user_message for x in filter_word_for_price_case) \
        #                         and (
        #                         all(x in entities for x in uc_s51_necessary_attribute) or "bao nhiêu" in user_message):
        #                     if len(set(all_attribute[1:]) & set(entities)) == 0:
        #                         if "price" in entities or "price" in entity_values:
        #                             chatlog_df.at[message_index, "use_case"] = "uc_s51"
        #                     elif len(set(all_attribute) & set(entities)) <= 3:
        #                         chatlog_df.at[message_index, "use_case"] = "uc_s52"
        #                     else:
        #                         chatlog_df.at[message_index, "use_case"] = "uc_s53"
        #                 else:
        #                     chatlog_df.at[message_index, "use_case"] = "other"
        #             else:
        #                 chatlog_df.at[message_index, "use_case"] = "other"
        #
        # for item in conversation_turn_pair_with_img:
        #     conversation_id = item[0]
        #     turn = item[1]
        #     sub_df = chatlog_df[(chatlog_df["conversation_id"] == conversation_id) & (chatlog_df["turn"] == turn) & (
        #             chatlog_df["user_message"] != "user")].reset_index()
        #     if len(sub_df) > 0:
        #         for item_index in range(0, len(sub_df)):
        #             user_message = sub_df.at[item_index, "user_message_clean"]
        #             message_index = sub_df.at[item_index, "index"]
        #             if str(user_message) == "nan" or str(user_message) == "user":
        #                 continue
        #             if (all(x in user_message for x in ["có", "không"]) and str(user_message).index("có") < str(
        #                     user_message).index("không")) or "có sẵn" in str(user_message):
        #                 ner_output = model_ner.process(sample=user_message)
        #                 entities = [x["entity"] for x in ner_output]
        #                 if len(entities) == 0:
        #                     continue
        #                 if "có sẵn" in str(user_message):
        #                     chatlog_df.at[message_index, "use_case"] = "uc_s7"
        #                 elif all(x in user_message for x in ["có", "không"]) and str(user_message).index("có") < str(
        #                         user_message).index("không"):
        #                     if any(x in entities for x in ["mention", "object_type"]) and not any(
        #                             x in entities for x in all_attribute[:-1]):
        #                         chatlog_df.at[message_index, "use_case"] = "uc_s6"
        #                     else:
        #                         chatlog_df.at[message_index, "use_case"] = "uc_s6x"

        chatlog_df.to_csv(output_file.format(month=str(month)), index=False)
        print("Specify usecase -" + str(time.time() - start_time))


specify_usecase()
