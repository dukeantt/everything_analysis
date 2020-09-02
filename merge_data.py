import pandas as pd
import os
import ast
import json


def main():
    file_path = "chatlog_data/august"
    files = os.listdir(file_path)
    month_info_list = []
    for index, filename in enumerate(files):
        xls = pd.ExcelFile(file_path + "/" + filename)
        sheet_names = xls.sheet_names[:5]
        info_dict = dict((el, []) for el in sheet_names)
        # keys: ngay trong tuan
        # list bao gom: version, so conversation, so conversation gui anh, so message gui anh,  so message found exact, so conv found similar, so message found none, so message khong tl
        for sheet_name in sheet_names:
            df = pd.read_excel(xls, sheet_name)
            try:
                df = df.dropna(subset=["conv_id"])
            except:
                a = 0
            if len(df) == 0:
                continue
            no_converastions = df["conv_id"].drop_duplicates()
            no_messages_with_img = df[df["input_text"].str.contains("scontent")]
            no_conversations_with_img = no_messages_with_img.drop_duplicates(subset=["conv_id"])

            list_exact = []
            list_similar = []
            list_none = []
            version = ""
            for index, item in no_messages_with_img.iterrows():
                version = item["version"]
                cv_outputs = item["cv_outputs"]
                if cv_outputs is None or str(cv_outputs) == "nan" or str(cv_outputs) == "None":
                    list_none.append(item["conv_id"])
                elif cv_outputs is not None:
                    cv_outputs_list = ast.literal_eval(cv_outputs)
                    if cv_outputs_list[0] == 1 or cv_outputs_list[0] == 0:
                        list_exact.append(item["conv_id"])
                    elif cv_outputs_list[0] == 2:
                        list_similar.append(item["conv_id"])

            info_dict[sheet_name].append(version)
            info_dict[sheet_name].append(len(no_converastions))
            info_dict[sheet_name].append(len(no_conversations_with_img))
            info_dict[sheet_name].append(len(no_messages_with_img))
            info_dict[sheet_name].append(list_exact)
            info_dict[sheet_name].append(list_similar)
            info_dict[sheet_name].append(list_none)
            a = 0
        month_info_list.append(info_dict)

    with open('august_cv_info.json', 'w', encoding='utf-8') as f:
        json.dump(month_info_list, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    main()
