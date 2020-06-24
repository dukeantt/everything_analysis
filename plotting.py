import pandas as pd
import plotly.graph_objects as go
from ast import literal_eval
import datetime
from collections import Counter

all_convo = pd.read_csv("analyze_data/all_conversations_without_trash.csv")
success_convo = pd.read_csv("analyze_data/success_conversations.csv")


# def pie_chart():
#     success_rate = (len(success_convo) / len(all_convo)) * 100
#
#     print("All Conversations: " + str(len(all_convo)))
#     print("Successful Conversations: " + str(len(success_convo)))
#     print("Success rate: " + '{0:.2f}'.format(success_rate) + "%")
#
#     labels = ["Successfull Conversations", "All Conversations"]
#     values = [len(success_convo), len(all_convo)]
#     fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
#     fig.update_traces(hoverinfo='label', textinfo='value')
#     fig.show()


def line_bar_plot():
    # Line plot
    count_success_by_month = success_convo.groupby(["message_timestamp_month"]).count().reset_index()
    month_list = list(count_success_by_month["message_timestamp_month"])
    no_sender_id_each_month = list(count_success_by_month["sender_id"])

    fig = go.Figure(data=go.Scatter(x=month_list, y=no_sender_id_each_month, mode='lines+markers'))
    fig.update_layout(title='Successfull conversations by month',
                      xaxis_title='Month',
                      yaxis_title='Number of successful conversations')
    fig.show()

    # Bar plot
    count_success_by_date = success_convo.groupby(["message_timestamp_date"]).sum().reset_index()
    date_list = list(count_success_by_date["message_timestamp_date"])
    no_thank = list(count_success_by_date["thank"])
    no_handover = list(count_success_by_date["handover"])

    date_count = count_total_convo_on_date()
    total_conv_on_date = [date_count[x] for x in date_list]

    fig = go.Figure(data=[
        go.Bar(name='Thanks', x=date_list, y=no_thank, width=0.4,
               offset=-0.4),
        go.Bar(name='Order', x=date_list, y=no_handover, width=0.4,
               offset=-0.4)
    ])

    fig.update_layout(
        title='Successfull conversations by date',
        xaxis_title='Date',
        yaxis_title='Number of conversations',
        xaxis=dict(
            type='category',
        ),
        barmode='stack',
        autosize=False,
        width=1000,
        # updatemenus=[
        #     dict(
        #         type="buttons",
        #         direction="right",
        #         active=0,
        #         x=1,
        #         y=1.2,
        #         buttons=list([
        #             dict(label="None",
        #                  method="update",
        #                  args=[{"visible": [True, True, False]},
        #                        {"title": "Successfull conversations by date",
        #                         "annotations": []}]),
        #             dict(label="Compare with total",
        #                  method="update",
        #                  args=[{"visible": [True, True, True]},
        #                        {"title": "Successfull conversations by date on total"}]),
        #         ]),
        #     )
        # ]
    )
    fig.show()

    # Bar plot 2
    fig = go.Figure(data=[
        go.Bar(name='Thanks', x=date_list, y=no_thank, width=0.4,
               offset=-0.4),
        go.Bar(name='Order', x=date_list, y=no_handover, width=0.4,
               offset=-0.4)
    ])

    fig.add_trace(
        go.Bar(name='Total conversations', x=date_list, y=total_conv_on_date, base=0,
               width=0.4,
               offset=0.0,
               )
    )
    fig.update_layout(
        title='Successfull conversations by date on total',
        xaxis_title='Date',
        yaxis_title='Number of conversations',
        xaxis=dict(
            type='category',
        ),
        barmode='stack',
        autosize=False,
        width=1000,
    )
    fig.update_xaxes(tickangle=45)
    fig.show()


def sun_burst():
    success_rate = (len(success_convo) / len(all_convo)) * 100

    print("All Conversations: " + str(len(all_convo)))
    print("Successful Conversations: " + str(len(success_convo)))
    print("Success rate: " + '{0:.2f}'.format(success_rate) + "%")

    no_thank = len(success_convo[success_convo["thank"] == 1])
    no_handover = len(success_convo[success_convo["handover"] == 1])
    no_success = len(success_convo)
    no_all_convo = len(all_convo)
    fig = go.Figure(go.Sunburst(
        labels=["Success", "All conversation", "Thank", "Order"],
        parents=["", "", "Success", "Success"],
        values=[no_success, no_all_convo, no_thank, no_handover],
        insidetextorientation='radial'

    ))
    fig.update_traces(textinfo='value+label')

    fig.update_layout(
        title='Successful conversations on total conversations',
        margin=dict(t=0, l=0, r=0, b=0),
        showlegend=True
    )

    fig.show()


def count_total_convo_on_date():
    events_list = list(all_convo["events"])
    all_date = []
    for item in events_list:
        event = literal_eval(item)
        set([int(x["timestamp"]) for x in event])
        all_date += list(set(
            [datetime.datetime.utcfromtimestamp(int(x["timestamp"])).strftime('%m-%d') for x in literal_eval(item)]))

    date_counter = Counter(all_date)

    return date_counter

# line_bar_plot()
