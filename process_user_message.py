import pandas as pd
from spelling_correction.heuristic_correction import *
import pickle
import re
import logging
import time
import re
import unicodedata
import unidecode

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

with open("obj_type", "r", encoding="utf-8") as obj_type_file:
    lines = obj_type_file.readlines()
    objtype_list = [x.strip().replace("_", " ") for x in lines]

with open("brand", "r", encoding="utf-8") as obj_type_file:
    lines = obj_type_file.readlines()
    brand_list = [x.strip() for x in lines]


def remove_col_str(customer_messages):
    start_time = time.time()
    logger.info("Remove col string")

    # remove a portion of string in a dataframe column - col_1
    customer_messages = [str(x).replace('\n', ' ') for x in customer_messages]
    # remove all the characters after &# (including &#) for column - col_1
    customer_messages = [str(x).replace(' &#.*', ' ') for x in customer_messages]

    logger.info(str(time.time() - start_time))
    return customer_messages


def remove_col_white_space(customer_messages):
    start_time = time.time()
    logger.info("Remove col white space")

    # remove white space at the beginning of string
    customer_messages = [str(x).strip() for x in customer_messages]

    logger.info(str(time.time() - start_time))
    return customer_messages


def correction_message(customer_messages):
    start_time = time.time()
    logger.info("Correction message")

    filter_text = ["facebook", "started"]
    correct_messages = []
    for message in customer_messages:
        if all(x not in message for x in filter_text):
            message = do_correction(str(message))
        correct_messages.append(message)

    logger.info(str(time.time() - start_time))
    return correct_messages


def remove_special_characters(customer_messages):
    start_time = time.time()
    logger.info("Remove special character")

    customer_messages = [re.sub('\}|\{|\]|\[|\;|\.|\,|\.|\:|\!|\@|\#|\$|\^|\&|\(|\)|\<|\>|\?|\"|\'', ' ', str(x)) for x
                         in customer_messages]
    # customer_messages = [x for x in customer_messages if x != ' ']

    logger.info(str(time.time() - start_time))
    return customer_messages


def deEmojify(customer_messages):
    start_time = time.time()
    logger.info("de-emojify")

    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    customer_messages = [regrex_pattern.sub(r'', x) for x in customer_messages]

    logger.info(str(time.time() - start_time))
    return customer_messages


def get_customer_message():
    start_time = time.time()
    logger.info("Get customer message")

    chatlog_df_list = []
    for month in range(1, 8):
        file_path = "data/chatlog_fb/all_chat_fb_{selected_month}.csv"
        file_path = file_path.format(selected_month=str(month))
        chatlog_df = pd.read_csv(file_path)
        chatlog_df_list.append(chatlog_df)

    all_chatlog = pd.concat(chatlog_df_list)
    customer_messages = [x for x in all_chatlog["user_message"] if str(x) not in ["nan", "user"]]

    logger.info(str(time.time() - start_time))
    return customer_messages


def remove_stop_word(customer_messages=None):
    start_time = time.time()
    logger.info("Remove stop words")

    file_name = "stop_words.txt"
    with open(file_name) as f:
        line_list = f.readlines()
    stop_words = [x.replace("\n", "") for x in line_list]

    new_customer_messages = []

    for message in customer_messages:
        message = message.split(" ")
        message = [x for x in message if x not in stop_words]
        message = " ".join(message)
        if message != '':
            new_customer_messages.append(message)

    logger.info(str(time.time() - start_time))
    return new_customer_messages


def remove_one_char_sentences(customer_messages):
    customer_messages = [x for x in customer_messages if len(x) > 1]
    return customer_messages


def export_clean_customer_messages():
    start_time = time.time()
    logger.info("Export clean customer message")

    customer_messages = get_customer_message()
    clean_customer_messages = remove_col_str(customer_messages)
    clean_customer_messages = deEmojify(clean_customer_messages)
    clean_customer_messages = remove_special_characters(clean_customer_messages)
    clean_customer_messages = correction_message(clean_customer_messages)
    clean_customer_messages = remove_col_white_space(clean_customer_messages)

    df = pd.DataFrame({"customer_message": customer_messages, "clean_customer_message": clean_customer_messages})
    df.to_csv("data/customer_message/customer_messages.csv", index=False)
    # with open('data/customer_message/customer_messages.pkl', 'wb') as file:
    #     # store the data as binary data stream
    #     pickle.dump(customer_messages, file)

    logger.info(str(time.time() - start_time))
    return df


def get_processed_customer_message():
    with open('data/customer_message/customer_messages_old.pkl', 'rb') as file:
        # store the data as binary data stream
        customer_messages = pickle.load(file)
    # customer_messages = remove_stop_word(customer_messages)
    customer_messages = pd.read_csv("data/customer_message/customer_messages.csv")
    customer_messages["clean_customer_message"] = deEmojify(customer_messages["clean_customer_message"].to_list())
    # customer_messages = remove_one_char_sentences(customer_messages)
    customer_messages = customer_messages[customer_messages['clean_customer_message'].str.len() > 1]
    customer_messages["clean_customer_message"] = [unicodedata.normalize('NFC', x.lower()) for x in
                                                   customer_messages["clean_customer_message"].to_list()]
    message_group = []
    for item in customer_messages.itertuples():
        clean_message = item.clean_message
        if any(x in clean_message for x in ["ship", "sip"]):
            message_group.append("shipping")
        elif any(x in clean_message for x in ["giá", "bao nhiêu"]):
            message_group.append("price")
        elif any(x in clean_message for x in objtype_list):
            message_group.append("object type related")
        elif any(x in clean_message for x in objtype_list):
            message_group.append("brand related")
        elif any(x in clean_message for x in ["thanks", "tks", "cảm ơn", "cám ơn", "thank"]):
            message_group.append("thank")
        elif any(x in clean_message for x in ["ok", "uk"]):
            message_group.append("agree")
        elif any(x in clean_message for x in ["điện thoại", "đt", "dt"]):
            message_group.append("telephone")
        elif "shopee" in clean_message:
            message_group.append("shopee related")
        elif any(x in clean_message for x in ["địa chỉ", "đc", "dc"]):
            message_group.append("address")
        else:
            message_group.append(clean_message)
    customer_messages["message_group"] = message_group
    customer_messages = customer_messages[customer_messages["clean_customer_message"] != " "]
    return customer_messages


def main():
    export_clean_customer_messages()


if __name__ == '__main__':
    main()
