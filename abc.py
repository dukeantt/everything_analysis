from pyvi import ViTokenizer, ViPosTagger
from vncorenlp import VnCoreNLP
from heuristic_correction import *


def main(text):
    """
    correction -> segment -> remove stop word -> pos tag
    """
    rdrsegmenter = VnCoreNLP("vncorenlp/VnCoreNLP-1.1.1.jar", annotators="wseg", max_heap_size='-Xmx500m')
    text = "Shop còn bộ đồ chơi con vật không"
    #do correction
    text = do_correction(text)

    #word segemented
    word_segmented_text = rdrsegmenter.tokenize(text)
    word_segmentend_sentence = ""
    for item in word_segmented_text:
        word_segmentend_sentence += " ".join(item)

    pos_tag = ViPosTagger.postagging(ViTokenizer.tokenize(word_segmentend_sentence))
    b = 0