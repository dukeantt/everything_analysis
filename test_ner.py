from pyvi import ViTokenizer, ViPosTagger
from heuristic_correction import *
from numpy import loadtxt

# correction -> segment -> remove stop word -> pos tag
with open("stop_words.txt", "r") as stop_words:
    content = stop_words.read()
    content_list = content.split("\n")

text = "E ơi bảng này bnhieeu"
# do correction
text = do_correction(text)

pos_tag = ViPosTagger.postagging(ViTokenizer.tokenize(text))
print(pos_tag)
