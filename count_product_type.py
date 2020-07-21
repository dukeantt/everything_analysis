import pandas as pd
import json
import datetime
from ast import literal_eval
from heuristic_correction import *
from utils.helper import *
from datetime import datetime
from nltk import word_tokenize
from nltk.util import ngrams
from unidecode import unidecode
import pickle

obj_type_list_1 = ["tham", "quay", "xe_day", "noi", "phao", "ghe_an", "diu", "giuong", "be_boi", "may_hut_sua", "goi",
                   "thanh_chan_giuong", "ke", "cau_truot", "balo", "coc", "xe_tap_di", "ngua_bap_benh", "xe_choi_chan",
                   "ghe_da_nang", "may_pha_sua", "bat", "man", "may_hut_mui", "khan", "yem", "may_ham_sua", "num",
                   "binh_sua", "ghe",
                   "xe", "may", "ao_hut_sua", "bo_ban_ghe", "bo_do_choi", "bo", "chan", "chau", "da_kho", "dai",
                   "ghe_o_to",
                   "ghe_rung", "khay_an", "may_tao_am", "may_tiet_trung", "may_xay", "may_dun_nuoc", "nhiet_ke",
                   "noi_com_dien", "nuoc_rua",
                   "quan_ao", "sach", "thap", "thia", "thu_nhun", "ti_gia", "tui_tru_sua", "tui_ham_sua", "tu_nhua",
                   "xe_3_banh",
                   "xuc_xac", "dan", "other"]

obj_type_list_2 = ["thảm", "quây", "xe đẩy", "nôi", "phao", "ghế ăn", "địu", "giường", "bể bơi", "máy hút sữa", "gối",
                   "thanh chắn giường", "kệ", "cầu trượt", "balo", "cốc", "xe tập đi", "ngựa bập bênh", "xe chòi chân",
                   "ghế đa năng", "máy pha sữa", "bát", "màn", "máy hút mũi", "khăn", "yếm", "máy hâm sữa", "núm",
                   "bình sữa", "ghế ", "xe", "máy", "áo hút sữa", "bộ bàn ghế", "bộ đồ chơi", "bô", "chăn", "chậu",
                   "đá khô", "đai", "ghế ô tô", "ghế rung", "khay ăn", "máy tạo ẩm", "máy tiệt trùng", "máy xay",
                   "máy đun nước", "nhiệt kế", "nồi cơm điện", "nước rửa", "quần áo", "sách", "tháp", "thìa ",
                   "thú nhún", "ti giả", "túi trữ sữa", "túi hâm sữa", "tủ nhựa", "xe 3 bánh", "xúc xắc", "đàn",
                   "ghế oto", "ghế ôtô", "xe ba bánh", "tủ", "other"]

obj_type_dict = {x: [] for x in obj_type_list_2}
obj_counter_dict = {x: 0 for x in obj_type_list_2}


def split_obj_type(obj_type_list):
    """
    Chia obj_type thanhg 3 loại
    Loại 1: Từ có 1 chữ
    Loại 2: Từ có 2 chữ
    Loại 3: Từ có 3 chữ
    :param obj_type_list:
    :return:
    """
    obj_type_1 = []
    obj_type_2 = []
    obj_type_3 = []
    for obj_type in obj_type_list:
        word_count = len(obj_type.split(" "))
        if word_count == 1:
            obj_type_1.append(obj_type)
        elif word_count == 2:
            obj_type_2.append(obj_type)
        else:
            obj_type_3.append(obj_type)
    return obj_type_1, obj_type_2, obj_type_3


def detect_obj_type_in_conversations(sentence, updated_time, obj_type_list, type):
    correction_sentence = do_correction(sentence)
    unidecode_sentence = unidecode(sentence)
    # Tách các từ trong câu thành n-gram, n tùy theo obj_type nếu là loại 3 thì n = 3, ...
    n_gram_word = [" ".join(x) for x in list(ngrams(correction_sentence.split(), type))]
    for index, word in enumerate(n_gram_word):
        # Check các từ n-gram trong câu có nằm trong list obj_type không
        if word in obj_type_list:
            obj_type_dict[word].append((sentence, updated_time))
            return 1
        elif index == len(n_gram_word) - 1:
            return 0


def main():
    obj_type_1, obj_type_2, obj_type_3 = split_obj_type(obj_type_list_2)
    # Loop qua tất cả messages từng tháng
    fb_chat_df = pd.read_csv("analyze_data/all_chat_fb/all_chat_fb_1.csv")
    fb_chat_df = fb_chat_df.sort_values(by=["updated_time"])
    fb_chat_df = fb_chat_df[~fb_chat_df["sender_id"].isna()]
    sender_ids = list(set(fb_chat_df["sender_id"]))
    user_message_counter = 0
    for index, sender_id in enumerate(sender_ids):
        sub_df = fb_chat_df[fb_chat_df["sender_id"] == sender_id]
        user_messages = [x for x in sub_df["user_message"].to_list() if x != "user" and str(x) != "nan"]
        updated_time = list(sub_df["updated_time"])[0]
        for user_message in user_messages:
            user_message_counter += 1
            # Check những obj_type loại 3 trước nếu tìm được chuyển sang message tiếp theo
            case_1 = detect_obj_type_in_conversations(user_message, updated_time, obj_type_3, 3)
            if case_1 == 0:
                # Nếu ko có obj_type loại 3 check obj_type loại 2
                case_2 = detect_obj_type_in_conversations(user_message, updated_time, obj_type_2, 2)
                if case_2 == 0:
                    # Cuối cùng là loại 1
                    case_3 = detect_obj_type_in_conversations(user_message, updated_time, obj_type_1, 1)
                    if case_3 == 0:
                        obj_type_dict["other"].append((user_message, updated_time))

    filename = 'analyze_data/count_obj_type/january.pkl'
    outfile = open(filename, 'wb')
    pickle.dump(obj_type_dict, outfile)
    outfile.close()


def count_each_obj_type():
    month_list = ["january", "february", "march", "april", "may", "june", "july"]
    for month in month_list:
        with open("analyze_data/count_obj_type/" + month + ".pkl", 'rb') as file:
            dict = pickle.load(file)
            for index, item in dict.items():
                obj_counter_dict[index] += len(item)

    filename = 'analyze_data/count_obj_type/count_obj_type.pkl'
    outfile = open(filename, 'wb')
    pickle.dump(obj_counter_dict, outfile)
    outfile.close()

# main()
count_each_obj_type()
