# pip install http://minio.dev.ftech.ai/denver-v0.0.1-e81f32ad/denver-0.0.1-py3-none-any.whl

from denver.datasets.ic_dataset import ICDataset
from denver.trainers.language_model_trainer import LanguageModelTrainer
from denver.models.ic import ULMFITClassifier
from denver.trainers.trainer import ModelTrainer
from denver.models.ner import FlairSequenceTagger
import os
from pprint import pprint
import pickle

model_path_ner = '../../../phucpx/tutorial/Denver/models/ner/vi_nerr.pt'
model_ner = FlairSequenceTagger(mode='inference', model_path=model_path_ner)
with open('models/model_ner.pkl', 'wb') as file:
    pickle.dump(model_ner, file)
#  load model của ner hơi lâu (5') nên để loads hết đoạn ở trên rồi load ở dưới dùng jupyter ấy

output = model_ner.process(sample="bạn có ghế an đăm màu hồng gias 2 triệu")
pprint(output)
