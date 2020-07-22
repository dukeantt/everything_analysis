import pandas as pd
from heuristic_correction import *
from pyvi import ViTokenizer, ViPosTagger


def predict():
    for month in ["1", "2", "3", "4", "5", "6"]:
        fb_conversation_by_month = pd.read_csv("temporary_data/fb_conversation_" + month + ".csv")
        fb_conversation_by_month.insert(9, "prediction", "")
        fb_conversation_by_month.insert(10, "turn", "")
        conversation_ids = list(fb_conversation_by_month["conversation_id"])
        conversation_ids = sorted(set(conversation_ids),
                                  key=conversation_ids.index)
        for conversation_id in conversation_ids:
            sub_df = fb_conversation_by_month[fb_conversation_by_month["conversation_id"] == conversation_id]
            attachments = list(sub_df["attachments"])
            if all("scontent" not in str(x) for x in attachments):
                found_user_message = False
                turn = 0
                for index, item in sub_df.iterrows():
                    if item["sender_name"] != 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp':
                        found_user_message = True
                    if found_user_message:
                        user_message = item["user_message"]
                        if item["sender_name"] == 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp':
                            turn += 1
                        if str(user_message) == "nan":
                            continue

                        user_message_correction = do_correction(user_message)
                        if "giá" in user_message_correction or "bao nhiêu" in user_message_correction or "tiền" in user_message_correction or "tài khoản" in user_message_correction\
                                or "tk" in user_message_correction:
                            break
                        else:
                            pos_tag = ViPosTagger.postagging(ViTokenizer.tokenize(user_message_correction))
                            co_X_khong_form = False
                            if "không" in pos_tag[0]:
                                if "có" in pos_tag[0]:
                                    khong_index = pos_tag[0].index("không")
                                    co_index = pos_tag[0].index("có")
                                    if co_index < khong_index:
                                        if "N" in pos_tag[1][co_index:khong_index]:
                                            co_X_khong_form = True
                                elif "còn" in pos_tag[0]:
                                    khong_index = pos_tag[0].index("không")
                                    con_index = pos_tag[0].index("còn")
                                    if con_index < khong_index:
                                        if "N" in pos_tag[1][con_index:khong_index]:
                                            co_X_khong_form = True

                            if (co_X_khong_form or "sẵn hàng" in user_message_correction or "có sẵn" in user_message_correction or "còn sẵn" in user_message_correction) \
                                    and "ship" not in user_message_correction and "link shopee" not in user_message_correction:
                                fb_conversation_by_month.at[index, "prediction"] = "uc_3"
                                fb_conversation_by_month.at[index, "turn"] = turn
                                break

        fb_conversation_by_month.to_csv("result/uc3_month_" + month, index=False)


def count_uc3():
    for month in ["1", "2", "3", "4", "5", "6"]:
        conversation_by_month = pd.read_csv("result/uc3_month_" + month)
        total_conversations = len(set(conversation_by_month["conversation_id"]))
        first_turn_each_conv = conversation_by_month[(conversation_by_month["turn"] == 0) | (conversation_by_month["turn"] == 1)]
        conversation_uc3 = first_turn_each_conv[first_turn_each_conv["prediction"] == "uc_3"]
        print("Uc3 in month " +month+": "+str(len(conversation_uc3))+"/"+str(total_conversations)+" ("
              +str(int(len(conversation_uc3)/total_conversations * 100))+"%)")

def main():
    count_uc3()
    # predict()


main()