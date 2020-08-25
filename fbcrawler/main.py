from scrapy import cmdline
from datetime import date

today = date.today()

file = "scrapy crawl fb_spider -o data/chatlog_fb/2019.json -t json"
cmdline.execute(file.split())


import ast

# with open('2019.json') as f:
#     data = ast.literal_eval(f.read())
#
# a = 0