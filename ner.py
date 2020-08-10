# pip install http://minio.dev.ftech.ai/denver-v0.0.1-e81f32ad/denver-0.0.1-py3-none-any.whl
from denver.datasets.ic_dataset import ICDataset
from denver.trainers.language_model_trainer import LanguageModelTrainer
from denver.models.ic import ULMFITClassifier
from denver.trainers.trainer import ModelTrainer
from denver.models.ner import FlairSequenceTagger
import os
from pprint import pprint
import pickle


def export_ner_model():
    # export ner model
    model_path_ner = 'models/ner/vi_nerr.pt'
    model_ner = FlairSequenceTagger(mode='inference', model_path=model_path_ner)
    with open('models/model_ner.pkl', 'wb') as file:
        pickle.dump(model_ner, file)


def export_ic_model():
    # export ic model
    model_path_vi_ner = 'models/ic/vi_class.pkl'
    model_ic = ULMFITClassifier(mode="inference", model_path=model_path_vi_ner)
    return model_ic
    # with open('models/model_ic.pkl', 'wb') as file:
    #     pickle.dump(model_ic, file)


def get_ner_model():
    # get ner model
    with open("models/model_ner.pkl", "rb") as file:
        model_ner = pickle.load(file)
    return model_ner


def get_ic_model():
    # get ic model
    with open("models/model_ic.pkl", "rb") as file:
        model_ic = pickle.load(file)
    return model_ic


def test_ner():
    model_ner = get_ner_model()
    output = model_ner.process(sample="bạn có ghế an đăm màu hồng gias 2 triệu")
    pprint(output)


def test_ic():
    model_ic = export_ic_model()
    text = "bạn có ghế an đăm màu hồng gias 2 triệu"
    # output = model_ic.get_uncertainty_score(sample="bạn có ghế an đăm màu hồng gias 2 triệu", n_times=10)
    output = model_ic.predict(sample=text)
    pprint(output)


test_ic()