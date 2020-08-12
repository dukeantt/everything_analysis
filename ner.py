# pip install http://minio.dev.ftech.ai/denver-v0.0.1-e81f32ad/denver-0.0.1-py3-none-any.whl

from denver.datasets.ic_dataset import ICDataset
from denver.trainers.language_model_trainer import LanguageModelTrainer
from denver.models.ic import ULMFITClassifier
from denver.trainers.trainer import ModelTrainer
from denver.models.ner import FlairSequenceTagger
import pickle
import os
from spelling_correction.heuristic_correction import *

model_path_ner = 'models/vi_nerr.pt'
model_ner = FlairSequenceTagger(mode='inference', model_path=model_path_ner)

#  load model của ner hơi lâu (5') nên để loads hết đoạn ở trên rồi load ở dưới dùng jupyter ấy
# with open("models/model_ner.pkl", "rb") as model_file:
#     pickle.dump(model_ner, model_file)
print(do_correction("bạn có ghế an mastela  màu hồng gias tien"))
output = model_ner.process(sample=do_correction("em sẽ chuyển khoản cho chị 1.300k còn lại em thanh toán bằng tiền mặt nhé"))
from pprint import pprint
#
pprint(output)
