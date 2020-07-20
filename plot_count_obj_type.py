import plotly
import pickle

import plotly.graph_objects as go

with open("analyze_data/count_obj_type/count_obj_type.pkl", 'rb') as file:
    dict = pickle.load(file)


def plot():
    sorted_dict = {k: v for k, v in sorted(dict.items(), key=lambda item: item[1])}

    fig = go.Figure(
        [go.Bar(
            x=list(sorted_dict.values())[:-1],
            y=list(sorted_dict.keys())[:-1],
            orientation='h',
            text=list(sorted_dict.values())[:-1]
        ),
        ]
    )
    fig.update_layout(
        title='Object type rank',
        xaxis_title="times",
        yaxis_title="obj_type",
        height=1500,
        width=1000,
        uniformtext_minsize=8,
        uniformtext_mode='hide'
    )
    fig.update_traces(textposition='outside')

    fig.show()
