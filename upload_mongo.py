import pandas as pd
from pymongo import MongoClient


def upload_training_data():
    # Load csv dataset
    data_list = []
    for x in ["1", "2", "3", "4", "5", "6"]:
        data = pd.read_csv("temporary_data/uc2_data_" + x + ".csv")
        data.insert(2, "month", int(x))
        data_list.append(data)
    total_data = pd.concat(data_list)

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db['uc2_data']
    total_data.reset_index(inplace=True)
    data_dict = total_data.to_dict("records")

    # Insert collection
    collection.insert_many(data_dict)


def upload_chatlog():
    data_list = []
    for x in ["1", "2", "3", "4", "5", "6"]:
        data = pd.read_csv("temporary_data/fb_conversation_" + x + ".csv")
        data.insert(9, "month", int(x))
        data_list.append(data)
    total_data = pd.concat(data_list)

    # Connect to MongoDB
    client = MongoClient("mongodb+srv://ducanh:1234@ducanh.sa1mn.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client['chatlog_db']
    collection = db['fb_chatlog']
    total_data.reset_index(inplace=True)
    data_dict = total_data.to_dict("records")

    # Insert collection
    collection.insert_many(data_dict)


def main():
    # upload_chatlog()
    upload_training_data()

main()