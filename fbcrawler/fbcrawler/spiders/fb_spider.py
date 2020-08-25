import logging
import scrapy
import json
from datetime import date
from csv import DictWriter
import time
import datetime

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET,
    format='%(asctime)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_timestamp(timestamp: int, format: str):
    """

    :param timestamp:
    :param format: %Y-%m-%d %H:%M:%S
    :return:
    """
    readable_timestamp = datetime.datetime.utcfromtimestamp(timestamp).strftime(format)
    return readable_timestamp


class FbSpiderSpider(scrapy.Spider):
    name = 'fb_spider'
    access_token = "EAAm7pZBf3ed8BAJISrzp5gjX7QZCZCbwHHF0CbJJ2hnoqOdITf7RMpZCrpvaFJulpL8ptx73iTLKS4SzZAa6ub5liZAsp6dfmSbGhMoMKXy2tQhZAi0CcnPIxKojJmf9XmdRh376SFlOZBAnpSymsmUjR7FX5rC1BWlsTdhbDj0XbwZDZD"

    conversation_api = "https://graph.facebook.com/v6.0/1454523434857990?fields=conversations&access_token={token}"
    conversation_api = conversation_api.format(token=access_token)

    conversation_message_api = "https://graph.facebook.com/v6.0/{id}/messages?fields=from,to,message,created_time,attachments&access_token={token}"

    conversation_timestamp_year = "2019"

    # allowed_domains = ['facebook.com']
    # start_urls = [conversation_api]

    def start_requests(self):
        logger.info("Start request")
        url = self.conversation_api
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        logger.info("Parse")

        jsonresponse = json.loads(response.text)

        try:
            conversations = jsonresponse["conversations"]
        except:
            conversations = jsonresponse

        conversation_data = conversations["data"]
        next = conversations["paging"]["next"]

        updated_time = False
        for item in conversation_data:
            id = item["id"]
            link = item["link"]
            updated_time = item["updated_time"]
            url = self.conversation_message_api.format(id=id, token=self.access_token)
            yield scrapy.Request(url, callback=self.parse_conversation_message,
                                 cb_kwargs={'id': id, 'conversation_link': link, 'updated_time': updated_time})

        if updated_time[:4] == "2018":
            a = 0
        if updated_time and updated_time[:4] != "2018":
            logger.info("Parse again")

            yield scrapy.Request(next, callback=self.parse)

    def parse_conversation_message(self, response, id, conversation_link, updated_time):
        logger.info("Parse conversation message")

        jsonresponse = json.loads(response.text)
        data = jsonresponse["data"]
        shop_name = 'Shop Gấu & Bí Ngô - Đồ dùng Mẹ & Bé cao cấp'

        for item in data:
            year_created = item["created_time"][:4]
            if year_created == self.conversation_timestamp_year:
                message_from = item["from"]["name"]
                message_id = item["id"]
                created_time = item["created_time"]
                created_time_date = time.mktime(datetime.datetime.strptime(created_time[:10], "%Y-%m-%d").timetuple())
                created_time_month = get_timestamp(int(created_time_date), "%m")
                if created_time_month:
                    message_attachments = ""
                    if 'attachments' in item:
                        attachments_data = item["attachments"]["data"]
                        try:
                            for attachment in attachments_data:
                                message_attachments += attachment["image_data"]["url"] + ", "
                        except:
                            message_attachments = " "
                    sender_id = ""
                    to_sender_id = ""
                    if "id" in item["from"]:
                        sender_id = item["from"]["id"]
                    if "data" in item["to"] and len(item["to"]["data"]) > 0:
                        to_sender_id = item["to"]["data"][0]["id"]

                    sent_message = item["message"].encode('utf-16', 'surrogatepass').decode('utf-16')
                    if message_from != shop_name:
                        user_message = sent_message
                        shop_message = ""
                    else:
                        user_message = sent_message
                        shop_message = sent_message

                    yield {
                        'message_id': message_id,
                        'fb_conversation_id': id,
                        'sender_id': sender_id,
                        'to_sender_id': to_sender_id,
                        'sender_name': message_from,
                        'user_message': user_message,
                        'shop_message': shop_message,
                        'updated_time': updated_time,
                        'created_time': created_time,
                        'attachments': message_attachments,
                        'link': conversation_link
                    }
