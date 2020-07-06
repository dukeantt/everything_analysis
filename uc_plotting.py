import pandas as pd
import plotly.graph_objects as go
from ast import literal_eval
import datetime
from collections import Counter
from collections import OrderedDict

def piechart():
    uc2_df = pd.read_csv("analyze_data/uc2_conversation_with_outcome.csv")
    total_uc2_conversation = len(set(uc2_df["id"]))