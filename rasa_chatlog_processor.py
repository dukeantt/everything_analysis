from datetime import datetime
from utils.helper import *
import datetime
import logging
import pandas as pd
import unicodedata
from pyvi import ViTokenizer, ViPosTagger
from denver.models.ner import FlairSequenceTagger

logging.basicConfig(filename="logging_data/rasa_chatlog_processor_log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
with open("obj_type", "r", encoding="utf-8") as obj_type_file:
    lines = obj_type_file.readlines()
    objtype_list = [x.strip() for x in lines]


class RasaChalogProcessor():
    def __init__(self):
        pass

    def get_chatlog_by_month(self, input_month: str, raw_chatlog: str):
        """
        Get chatlog on specific month from raw chatlog from rasa

        :param month: string to indicate month ["01", "02", "03", "04", "05", "06", "07]
        :param raw_chatlog: path to raw chatlog csv file
        """
        logger.info("Get chatlog by month")
        field_name = ['sender_id', 'slots', 'latest_message', 'latest_event_time', 'followup_action', 'paused',
                      'events',
                      'latest_input_channel', 'active_form', 'latest_action_name']
        rasa_conversation = pd.read_csv(raw_chatlog, names=field_name, header=None)
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
                                            entity_list += item["value"] + ","
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
        output_file_path = "output_data/chatlog_rasa/rasa_chatlog_{month}.csv"
        output_file_path = output_file_path.format(month=input_month)
        rasa_chatlog_df.to_csv(output_file_path, index=False)
        return rasa_chatlog_df

    def split_chatlog_to_conversations(self, rasa_chatlog_df: pd.DataFrame, begin_conversation_id=0):
        """
       Split chatlog to conversation
       :param fb_conversations:
       :return:
       """
        start_time = time.time()
        logger.info("Split chatlog to conversations")
        rasa_chatlog_df.insert(0, 'conversation_id', 0)
        rasa_chatlog_df.insert(9, 'conversation_begin_date', None)
        rasa_chatlog_df.insert(10, 'conversation_begin_time', None)

        fmt = '%Y-%m-%d %H:%M:%S'
        sender_ids = list(rasa_chatlog_df["sender_id"].dropna())
        sender_ids = sorted(set(sender_ids), key=sender_ids.index)
        conversation_id = begin_conversation_id
        last_conversation_id = None
        conversation_begin_date = None
        conversation_begin_time = None
        checked_sender_id = []
        for sender_id_index, sender_id in enumerate(sender_ids):
            if sender_id not in checked_sender_id:
                conversation_id += 1
                checked_sender_id.append(sender_id)
            sub_df = rasa_chatlog_df[rasa_chatlog_df["sender_id"] == sender_id].reset_index()
            for index, item in sub_df.iterrows():
                if last_conversation_id is None or last_conversation_id != conversation_id:
                    conversation_begin_date = datetime.datetime.strptime(item["created_time"][:10], "%Y-%m-%d")
                    conversation_begin_time = datetime.datetime.strptime(item["created_time"][11:], "%H:%M:%S")
                message_index = item["index"]
                rasa_chatlog_df.at[message_index, "conversation_id"] = conversation_id
                rasa_chatlog_df.at[message_index, "conversation_begin_date"] = conversation_begin_date
                rasa_chatlog_df.at[message_index, "conversation_begin_time"] = conversation_begin_time

                last_conversation_id = conversation_id

                if index + 1 < len(sub_df):
                    next_message = sub_df.iloc[index + 1]
                    current_time = item["created_time"][:10] + " " + item["created_time"][11:19]
                    current_time = datetime.datetime.strptime(current_time, fmt)

                    next_time = next_message["created_time"][:10] + " " + next_message["created_time"][11:19]
                    next_time = datetime.datetime.strptime(next_time, fmt)

                    time_diff = (next_time - current_time).total_seconds()
                    if time_diff > 86400:
                        conversation_id += 1
        logger.info("Split to conversations: --- %s seconds ---" % (time.time() - start_time))
        return rasa_chatlog_df

    def split_chatlog_conversations_to_turns(self, rasa_chatlog_df: pd.DataFrame):
        """
        Split conversations to turns
        :param rasa_chatlog_df:
        :return:
        """
        start_time = time.time()
        logger.info("Split conversations to turns")
        rasa_chatlog_df.insert(1, "turn", "")
        conversation_ids = list(rasa_chatlog_df["conversation_id"])
        conversation_ids = list(dict.fromkeys(conversation_ids))
        for id in conversation_ids:
            sub_df = rasa_chatlog_df[rasa_chatlog_df["conversation_id"] == id]
            turn = 0
            previous_index = 0
            first_item_in_sub_df = True
            for index, item in sub_df.iterrows():
                if not first_item_in_sub_df:
                    previous_sender_name = sub_df.at[previous_index, "sender"]
                    current_sender_name = item["sender"]
                    try:
                        if previous_sender_name == 'bot' and current_sender_name != previous_sender_name:
                            turn += 1
                    except:
                        a = 0
                first_item_in_sub_df = False
                previous_index = index
                rasa_chatlog_df.at[index, "turn"] = turn
        logger.info("Split conversations to turns: --- %s seconds ---" % (time.time() - start_time))
        return rasa_chatlog_df

    def set_uc1_and_uc2_for_conversations(self, rasa_chatlog_df: pd.DataFrame):
        logger.info("Specify uc for conversations")
        start_time = time.time()
        conversation_ids = rasa_chatlog_df["conversation_id"].drop_duplicates(keep="first").to_list()
        rasa_chatlog_df.insert(2, "use_case", "")
        for id in conversation_ids:
            chatlog_sub_df = rasa_chatlog_df[rasa_chatlog_df["conversation_id"] == id]
            chatlog_sub_df_first_turn = chatlog_sub_df[chatlog_sub_df["turn"].isin([0, 1])]
            # conversation_attachments = chatlog_sub_df['attachments'].to_list()
            conversation_attachments = chatlog_sub_df_first_turn['attachments'].to_list()
            index_list = chatlog_sub_df_first_turn.index.tolist()
            conversation_has_images = False
            if any("scontent" in str(x) for x in conversation_attachments):
                conversation_has_images = True

            for i, item_index in enumerate(index_list):
                user_message = chatlog_sub_df_first_turn.loc[item_index, "user_message"]
                if str(chatlog_sub_df_first_turn.loc[item_index, "entities"]) != "nan":
                    entities_list = chatlog_sub_df_first_turn.loc[item_index, "entities"].split(",")
                    if any("price" in str(x) for x in entities_list):
                        if conversation_has_images:
                            rasa_chatlog_df.at[item_index, "use_case"] = "uc_s2"
                            break
                        else:
                            break
                    if str(user_message) != "nan":
                        user_message_correction = chatlog_sub_df_first_turn.loc[item_index, "user_message_correction"]

                        message_pos_tag = ViPosTagger.postagging(ViTokenizer.tokenize(user_message_correction))
                        words = [x for x in message_pos_tag[0]]
                        pos = [x for x in message_pos_tag[1]]

                        con_x_khong_form = False
                        co_x_khong_form = False
                        if "còn" in words and "không" in words:
                            con_index = words.index("còn")
                            khong_index = words.index("không")
                            if con_index < khong_index:
                                in_between_word_pos = pos[con_index:khong_index]
                                """
                                N - Common noun
                                Nc - Noun Classifier
                                Ny - Noun abbreviation
                                Np - Proper noun
                                Nu - Unit noun
                                """
                                if any(x in in_between_word_pos for x in ["N", "Nc", "Ny", "Np", "Nu"]):
                                    con_x_khong_form = True
                        elif "có" in words and "không" in words:
                            co_index = words.index("có")
                            khong_index = words.index("không")
                            if co_index < khong_index:
                                in_between_word_pos = pos[co_index:khong_index]
                                if any(x in in_between_word_pos for x in ["N", "Nc", "Ny", "Np", "Nu"]):
                                    co_x_khong_form = True

                        # if conversation_has_images and (con_x_khong_form or "còn không" in user_message_correction or all(x in user_message_correction for x in ["còn", "không"])):
                        if conversation_has_images and ("còn không" in user_message_correction or all(
                                x in user_message_correction for x in ["còn", "không"])):
                            rasa_chatlog_df.at[item_index, "use_case"] = "uc_s1"
                            break
                        # elif not conversation_has_images and (co_x_khong_form or "có không" in user_message_correction or all(x in user_message_correction for x in ["có", "không"])):
                        elif not conversation_has_images and ("có không" in user_message_correction or all(
                                x in user_message_correction for x in ["có", "không"])):
                            if str(chatlog_sub_df_first_turn.loc[item_index, "entities"]) != "nan":
                                entities_list = chatlog_sub_df_first_turn.loc[item_index, "entities"].split(",")
                                entities_list = [x for x in entities_list if x != '']
                                if any(x in objtype_list for x in entities_list):
                                    if len(entities_list) == 1:
                                        rasa_chatlog_df.at[item_index, "use_case"] = "uc_s31"
                                        break
                                    else:
                                        rasa_chatlog_df.at[item_index, "use_case"] = "uc_s32"
                                        break
        logger.info("Specify usecases: --- %s seconds ---" % (time.time() - start_time))
        return rasa_chatlog_df

    def specify_conversation_outcome(self, rasa_chatlog_df: pd.DataFrame):
        logger.info("Specify outcome for conversations")
        start_time = time.time()
        rasa_chatlog_df.insert(3, "outcome", "")
        conversation_ids = rasa_chatlog_df["conversation_id"].drop_duplicates(keep="first").to_list()

        key_words = ["ship", "gửi hàng", "lấy", "địa chỉ", "giao hàng", "đ/c", "thanh toán", "tổng", "stk",
                     "số tài khoản",
                     "gửi về"]
        filter_words = ["địa chỉ shop", "địa chỉ cửa hàng", "lấy rồi", "giao hàng chậm"]
        handover_bot_message = [
            "Dạ, bạn chờ trong ít phút shop kiểm tra kho hàng rồi báo lại bạn ngay ạ!",
            "Mời bạn bấm vào sản phẩm để xem thông tin chi tiết nhé",
        ]
        for id in conversation_ids:
            sub_conversation_df = rasa_chatlog_df[rasa_chatlog_df["conversation_id"] == id]
            sub_conversation_df = sub_conversation_df.dropna(subset=["bot_message", "user_message"], how="all")
            try:
                last_turn = max(list(sub_conversation_df["turn"]))
            except:
                rasa_chatlog_df.loc[rasa_chatlog_df.conversation_id == id, "outcome"] = "other"
                continue
            last_turn_message_df = sub_conversation_df[sub_conversation_df["turn"] == last_turn]
            last_turn_message_df = last_turn_message_df.dropna(subset=["bot_message", "user_message"], how="all")
            message_counter = 0
            for index, item in last_turn_message_df.iterrows():
                user_message = item["user_message"]
                user_message_correction = False
                if str(user_message) != "nan":
                    user_message_correction = item["user_message_correction"]

                bot_message = item["bot_message"]
                user_intent = item["intent"]
                if str(user_intent) != "nan" and (user_intent == "thank" or any(x in user_message_correction for x in
                                                                                ["thanks", "thank", "tks", "cảm ơn",
                                                                                 "thankyou", "cám ơn"])):
                    rasa_chatlog_df.at[index, "outcome"] = "thank"
                    break
                elif user_message_correction and any(x in user_message_correction for x in key_words) and all(
                        x not in user_message_correction for x in filter_words):
                    rasa_chatlog_df.at[index, "outcome"] = "shipping_order"
                    break
                elif (str(user_intent) != "nan" and user_intent == "handover_to_inbox") or any(
                        x in str(bot_message) for x in handover_bot_message):
                    rasa_chatlog_df.at[index, "outcome"] = "handover_to_inbox"
                    break
                elif str(user_intent) != "nan" and user_intent == "agree":
                    # rasa_chatlog_df.at[index, "outcome"] = "agree"
                    rasa_chatlog_df.at[index, "outcome"] = "other"
                    break
                elif message_counter == (len(last_turn_message_df) - 1) and item["sender"] == "bot":
                    rasa_chatlog_df.at[index, "outcome"] = "silence"
                    break
                elif message_counter == (len(last_turn_message_df) - 1):
                    rasa_chatlog_df.at[index, "outcome"] = "other"
                    break
                message_counter += 1
        logger.info("Specify outcomes: --- %s seconds ---" % (time.time() - start_time))
        return rasa_chatlog_df

    def specify_uc4(self, chatlog_df, conversation_turn_pair_without_img, model_ner):
        start_time = time.time()
        # chatlog_df.insert(2, "uc4", "")
        attributes = ["age_of_use", "guarantee", "color", "material", "origin", "promotion", "size", "weight", "brand",
                      "price"]
        CON = ['còn', ' con ', ' conf ']
        KHONG = ['không', ' k ', ' ko ', ' hem ', ' hok ', ' khg ', 'khoong']

        uc4_base_require_entities = ["object_type", "mention"]

        filter_word = ["ship", "shopee", "shoppee", "freeship", "shope"]
        for pair in conversation_turn_pair_without_img:
            conversation_id = pair[0]
            turn = pair[1]
            sub_df_full_conversation = chatlog_df[chatlog_df["conversation_id"] == conversation_id]
            list_uc = [x for x in sub_df_full_conversation['use_case'].to_list() if str(x) not in ['', 'nan']]
            if all(x not in list_uc for x in ["uc_s1", "uc_s2"]):
                sub_df = chatlog_df[(chatlog_df["conversation_id"] == conversation_id) & (chatlog_df["turn"] == turn) & (
                    ~chatlog_df["user_message"].isin(["nan", "user"]))].reset_index()
                if len(sub_df) > 0:
                    for item_index in range(0, len(sub_df)):
                        user_message = sub_df.at[item_index, "user_message_correction"]
                        user_message = unicodedata.normalize("NFC", str(user_message))
                        user_message = user_message.lower()
                        message_index = sub_df.at[item_index, "index"]
                        con = [word for word in CON if (word in user_message)]
                        khong = [word for word in KHONG if (word in user_message)]

                        if str(user_message) in ["nan", "user", "", " "]:
                            continue
                        ner_output = model_ner.process(sample=user_message)
                        entities = [x["entity"] for x in ner_output]
                        prod_attribute = [x["entity"] for x in ner_output if x["entity"] in attributes]

                        if con and khong and entities:
                            # if any(x in entities for x in uc4_base_require_entities) and all(x not in user_message for x in filter_word):
                            if any(x in entities for x in uc4_base_require_entities):
                                if len(prod_attribute) == 0:
                                    chatlog_df.at[message_index, "use_case"] = "uc_s4.1"
                                elif len(prod_attribute) == 1:
                                    chatlog_df.at[message_index, "use_case"] = "uc_s4.2"
                                else:
                                    chatlog_df.at[message_index, "use_case"] = "uc_s4.3"
        logger.info("Specify UC4: " + str(time.time() - start_time))
        return chatlog_df

    def specify_uc5(self, chatlog_df, conversation_turn_pair_without_img, model_ner):
        start_time = time.time()
        # chatlog_df.insert(2, "uc5", "")
        attributes = ["age_of_use", "guarantee", "color", "material", "origin", "promotion", "size", "weight", "brand",
                      "price"]
        filter_word = ["ship", "shopee", "shoppee", "freeship", "shope"]
        uc5_base_require_entities = ["object_type", "mention"]

        PRICE = ['giá ', 'gia ', 'gía ', 'tiền', 'tieen', 'tieenf', 'tien', 'bao nhiều', ' bn ', 'nhiu', 'bnh', 'nhieu',
                 'bao nhiu', 'nhiêu']
        for pair in conversation_turn_pair_without_img:
            conversation_id = pair[0]
            turn = pair[1]
            sub_df_full_conversation = chatlog_df[chatlog_df["conversation_id"] == conversation_id]
            list_uc = [x for x in sub_df_full_conversation['use_case'].to_list() if str(x) not in ['', 'nan']]
            if all(x not in list_uc for x in ["uc_s1", "uc_s2"]):
                sub_df = chatlog_df[(chatlog_df["conversation_id"] == conversation_id) & (chatlog_df["turn"] == turn) & (
                    ~chatlog_df["user_message"].isin(["nan", "user"]))].reset_index()
                for item_index in range(0, len(sub_df)):
                    user_message = sub_df.at[item_index, "user_message_correction"]
                    user_message = unicodedata.normalize("NFC", str(user_message))
                    user_message = user_message.lower()
                    message_index = sub_df.at[item_index, "index"]

                    price = [key_price for key_price in PRICE if (key_price in user_message)]

                    if str(user_message) == "nan" or str(user_message) == "user":
                        continue
                    ner_output = model_ner.process(sample=user_message)
                    entities = [x["entity"] for x in ner_output]
                    prod_attribute = [x["entity"] for x in ner_output if x["entity"] in attributes]
                    attribute_value = [x["value"] for x in ner_output if x["entity"] == "attribute"]

                    if len(price) > 0 or "price" in attribute_value:
                        # if any(x in entities for x in uc4_base_require_entities) and all(x not in user_message for x in filter_word):
                        if any(x in entities for x in uc5_base_require_entities):
                            if len(prod_attribute) == 0:
                                chatlog_df.at[message_index, "use_case"] = "uc_s5.1"
                            elif len(prod_attribute) == 1:
                                chatlog_df.at[message_index, "use_case"] = "uc_s5.2"
                            else:
                                chatlog_df.at[message_index, "use_case"] = "uc_s5.3"
        logger.info("Specify UC5: " + str(time.time() - start_time))
        return chatlog_df

    def get_conversation_first_turn_without_and_with_image(self, chatlog_df: pd.DataFrame):
        start_time = time.time()

        conversation_turn_list_without_image = []
        conversation_turn_list_with_image = []
        conversation_ids = chatlog_df["conversation_id"].drop_duplicates(keep="first").to_list()
        for id in conversation_ids:
            sub_df = chatlog_df[chatlog_df["conversation_id"] == id]
            # turns = sub_df["turn"].drop_duplicates(keep="first").to_list()
            turns = [0, 1]
            for turn in turns:
                attachments = sub_df.loc[sub_df["turn"] == turn, "attachments"].dropna().to_list()
                if len(attachments) == 0:
                    conversation_turn_list_without_image.append((id, turn))
                else:
                    conversation_turn_list_with_image.append((id, turn))
        logger.info("Get conversation turn without image: " + str(time.time() - start_time))
        return conversation_turn_list_without_image, conversation_turn_list_with_image

    def process_rasa_chatlog(self, df: pd.DataFrame, begin_converastion_id=0):
        """
        Processor
        :param input_month:
        :param raw_chatlog:
        :return:
        """
        start_time = time.time()
        logger.info("Start process chatlog")
        rasa_chatlog_by_month_df = df.dropna(subset=["bot_message", "user_message", "intent"], how="all")
        rasa_chatlog_by_month_df = rasa_chatlog_by_month_df.sort_values(by=["sender_id", "created_time"])

        model_path = "/home/ducanh/pycharm_prj/test_ner/models/vi_nerr.pt"
        model_ner = FlairSequenceTagger(mode="inference", model_path=model_path)

        # rasa_chatlog_by_month_df = self.get_chatlog_by_month(input_month, raw_chatlog)
        rasa_chatlog_by_month_df = self.split_chatlog_to_conversations(rasa_chatlog_by_month_df, begin_converastion_id)
        rasa_chatlog_by_month_df = self.split_chatlog_conversations_to_turns(rasa_chatlog_by_month_df)
        rasa_chatlog_by_month_df = self.set_uc1_and_uc2_for_conversations(rasa_chatlog_by_month_df)
        conversation_turn_pair_without_img, conversation_turn_pair_with_img = self.get_conversation_first_turn_without_and_with_image(
            rasa_chatlog_by_month_df)
        rasa_chatlog_by_month_df = self.specify_uc4(rasa_chatlog_by_month_df, conversation_turn_pair_without_img, model_ner)
        rasa_chatlog_by_month_df = self.specify_uc5(rasa_chatlog_by_month_df, conversation_turn_pair_without_img, model_ner)
        rasa_chatlog_by_month_df = self.specify_conversation_outcome(rasa_chatlog_by_month_df)

        logger.info("Process rasa chatlog: --- %s seconds ---" % (time.time() - start_time))
        return rasa_chatlog_by_month_df
