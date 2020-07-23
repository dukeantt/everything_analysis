import pickle
import pandas as pd
from heuristic_correction import *
from utils.helper import *


def get_accuracy():
    uc1_data_list = []
    uc2_data_list = []
    other_data_list = []
    for x in ["1", "2", "3", "4", "5", "6"]:
        uc1_data = pd.read_csv("temporary_data/uc1_data_" + x + ".csv")
        uc2_data = pd.read_csv("temporary_data/uc2_data_" + x + ".csv")
        other_data = pd.read_csv("temporary_data/other_data_" + x + ".csv")

        uc1_data_list.append(uc1_data)
        uc2_data_list.append(uc2_data)
        other_data_list.append(other_data)

    uc1_data_total = pd.concat(uc1_data_list)
    uc2_data_total = pd.concat(uc2_data_list)
    other_data_total = pd.concat(other_data_list)
    other_data_total = other_data_total.dropna(subset=['feature'])

    no_train_uc1 = int(2 * len(uc1_data_total) / 3)
    no_train_uc2 = int(2 * len(uc2_data_total) / 3)
    no_train_other = int(2 * len(other_data_total) / 3)

    uc1_uc2_data_test = pd.concat([
        uc1_data_total[no_train_uc1:],
        uc2_data_total[no_train_uc2:],
        other_data_total[no_train_other:]
    ])

    with open("models/ic_for_uc1_2.pkl", "rb") as file:
        clf = pickle.load(file)

    predicted = list(clf.predict(uc1_uc2_data_test["feature"]))
    predicted_prob = clf.predict_proba(uc1_uc2_data_test["feature"])
    uc1_uc2_data_test.insert(2, "prediction", predicted)
    uc1_uc2_data_test.insert(3, "prediction_prob", [list(x) for x in list(predicted_prob)])
    a = []
    for index, item in uc1_uc2_data_test.iterrows():
        if item["target"] != item["prediction"]:
            a.append((item["feature"], item["target"], item["prediction"], item["prediction_prob"]))

    print(str(int(100 - (len(a) / len(uc1_uc2_data_test) * 100))) + "%")
    return str(int(100 - (len(a) / len(uc1_uc2_data_test) * 100))) + "%"


def predict():
    with open("models/ic_for_uc1_2.pkl", "rb") as file:
        clf = pickle.load(file)
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
            if any("scontent" in str(x) for x in attachments):
                found_user_message = False
                turn = 0
                found_image = False
                found_uc = ""
                for index, item in sub_df.iterrows():
                    if "scontent" in str(item["attachments"]):
                        found_image = True

                    if item["sender_name"] != 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp':
                        found_user_message = True
                    if found_user_message:
                        user_message = item["user_message"]
                        if item["sender_name"] == 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp':
                            turn += 1
                            found_image = False
                            found_uc = ""
                        if str(user_message) == "nan":
                            continue

                        user_message_correction = do_correction(user_message)
                        test_data = []
                        test_data.append({"feature": user_message_correction})
                        df_test = pd.DataFrame(test_data)
                        predicted = list(clf.predict(df_test["feature"]))
                        if predicted[0] == "uc_1" or predicted[0] == "uc_2":
                            found_uc = predicted[0]
                        else:
                            fb_conversation_by_month.at[index, "prediction"] = predicted[0]

                        fb_conversation_by_month.at[index, "turn"] = turn
                        # if predicted[0] == "uc_1" or predicted[0] == "uc_2":
                        #     break
                        if found_image and found_uc != "":
                            fb_conversation_by_month.at[index, "prediction"] = found_uc
                            break

        fb_conversation_by_month.to_csv("result/uc1_uc2_month_" + month, index=False)


def count_uc1_uc2():
    for month in ["1", "2", "3", "4", "5", "6"]:
        conversation_by_month = pd.read_csv("result/uc1_uc2_month_" + month)
        total_conversations = len(set(conversation_by_month["conversation_id"]))
        first_turn_each_conv = conversation_by_month[
            (conversation_by_month["turn"] == 0) | (conversation_by_month["turn"] == 1)]
        conversation_uc1 = first_turn_each_conv[first_turn_each_conv["prediction"] == "uc_1"]
        conversation_uc2 = first_turn_each_conv[first_turn_each_conv["prediction"] == "uc_2"]

        print("Uc1 in month " + month + ": " + str(len(conversation_uc1)) + "/" + str(total_conversations) + " ("
              + str(int(len(conversation_uc1) / total_conversations * 100)) + "%)")
        print("Uc2 in month " + month + ": " + str(len(conversation_uc2)) + "/" + str(total_conversations) + " ("
              + str(int(len(conversation_uc2) / total_conversations * 100)) + "%)")


def count_uc1_uc2_bot_chat(df):
    total_conversations = len(set(df["id"]))
    conversation_uc1 = df[df["prediction"] == "uc_1"]
    conversation_uc2 = df[df["prediction"] == "uc_2"]

    print("Uc1 in 5: " + str(len(conversation_uc1)) + "/" + str(total_conversations) + " ("
          + str(int(len(conversation_uc1) / total_conversations * 100)) + "%)")
    print("Uc2 in month: " + str(len(conversation_uc2)) + "/" + str(total_conversations) + " ("
          + str(int(len(conversation_uc2) / total_conversations * 100)) + "%)")


def predict_bot_chat():
    with open("models/ic_for_uc1_2.pkl", "rb") as file:
        clf = pickle.load(file)

    fb_conversation_by_month = pd.read_csv("temporary_data/bot_conversation_5.csv")
    fb_conversation_by_month.insert(9, "prediction", "")
    conversation_ids = list(fb_conversation_by_month["id"])
    conversation_ids = sorted(set(conversation_ids), key=conversation_ids.index)
    for conversation_id in conversation_ids:
        sub_df = fb_conversation_by_month[fb_conversation_by_month["id"] == conversation_id]
        user_message = list(sub_df["message"])
        if any("scontent" in str(x) for x in user_message):
            for index, item in sub_df.iterrows():
                user_message = item["message"]
                if "scontent" in user_message:
                    user_message = user_message[246:]
                    user_message = user_message.replace('\n', "")

                user_message_correction = do_correction(user_message)
                test_data = []
                test_data.append({"feature": user_message_correction})
                df_test = pd.DataFrame(test_data)
                predicted = list(clf.predict(df_test["feature"]))
                fb_conversation_by_month.at[index, "prediction"] = predicted[0]
                if predicted[0] == "uc_1" or predicted[0] == "uc_2":
                    break
    return fb_conversation_by_month


def main():
    a = predict_bot_chat()
    count_uc1_uc2_bot_chat(a)
    predict()
    count_uc1_uc2()
    # get_accuracy()


main()
