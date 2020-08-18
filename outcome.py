import logging
import pandas as pd
import time

def specify_conversation_outcome(rasa_chatlog_df: pd.DataFrame):
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
        uc4 = sub_conversation_df["uc4"].dropna().to_list()
        uc5 = sub_conversation_df["uc5"].dropna().to_list()
        if uc4 or uc5:
            sub_conversation_df = sub_conversation_df[(sub_conversation_df["user_message"] != "user") | (sub_conversation_df["bot_message"] != "bot")]

            try:
                last_turn = max(list(sub_conversation_df["turn"]))
            except:
                rasa_chatlog_df.loc[rasa_chatlog_df.conversation_id == id, "outcome"] = "other"
                continue
            last_turn_message_df = sub_conversation_df[sub_conversation_df["turn"] == last_turn]
            message_counter = 0
            for index, item in last_turn_message_df.iterrows():
                user_message = item["user_message"]
                user_message_correction = False
                if str(user_message) != "nan":
                    user_message_correction = item["user_message_clean"]

                bot_message = item["bot_message"]
                # user_intent = item["intent"]
                if user_message_correction and any(x in user_message_correction for x in ["thanks", "thank", "tks", "cảm ơn", "thankyou","thank you", "cám ơn"]):
                    rasa_chatlog_df.at[index, "outcome"] = "thank"
                    break
                elif user_message_correction and any(x in user_message_correction for x in key_words) and all(
                        x not in user_message_correction for x in filter_words):
                    rasa_chatlog_df.at[index, "outcome"] = "shipping_order"
                    break
                elif any(x in str(bot_message) for x in handover_bot_message):
                    rasa_chatlog_df.at[index, "outcome"] = "handover_to_inbox"
                    break
                # elif str(user_intent) != "nan" and user_intent == "agree":
                #     # rasa_chatlog_df.at[index, "outcome"] = "agree"
                #     rasa_chatlog_df.at[index, "outcome"] = "other"
                #     break
                elif message_counter == (len(last_turn_message_df) - 1) and item["sender_name"] == "Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp":
                    rasa_chatlog_df.at[index, "outcome"] = "silence"
                    break
                elif message_counter == (len(last_turn_message_df) - 1):
                    rasa_chatlog_df.at[index, "outcome"] = "other"
                    break
                message_counter += 1
    print("Specify outcomes: --- %s seconds ---" % (time.time() - start_time))
    return rasa_chatlog_df
