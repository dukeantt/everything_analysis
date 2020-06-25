import pandas as pd
import plotly.graph_objects as go
from ast import literal_eval
import datetime
from collections import Counter
from collections import OrderedDict

all_convo = pd.read_csv("analyze_data/all_conversations_without_trash.csv")
success_convo = pd.read_csv("analyze_data/success_conversations.csv")
success_convo_timestamp = [x.replace("-", "") for x in list(success_convo["message_timestamp"])]
week_list = [datetime.datetime.strptime(x, '%Y%m%d').isocalendar()[1] for x in success_convo_timestamp]
success_convo.insert(6, 'week', week_list)


def line_bar_plot():
    # Line plot
    count_success_by_month = success_convo.groupby(["message_timestamp_month"]).count().reset_index()
    month_list = list(count_success_by_month["message_timestamp_month"])
    no_sender_id_each_month = list(count_success_by_month["sender_id"])

    fig = go.Figure(data=go.Scatter(x=month_list, y=no_sender_id_each_month, mode='lines+markers'))
    fig.update_layout(title='Successful conversations by month',
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
        width=900,
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
        title='Successful conversations by date on total',
        xaxis_title='Date',
        yaxis_title='Number of conversations',
        xaxis=dict(
            type='category',
        ),
        barmode='stack',
        autosize=False,
        width=950,
    )
    fig.update_xaxes(tickangle=45)
    fig.show()


def bar_plot_month():
    count_success_by_month = success_convo.groupby(["message_timestamp_month"]).count().reset_index()
    month_list = list(count_success_by_month["message_timestamp_month"])
    no_sender_id_each_month = list(count_success_by_month["sender_id"])
    month_count = count_total_convo_on_month()
    total_conv_on_date = [month_count[x] for x in [x.split("-")[1] for x in month_list]]

    fig = go.Figure(data=go.Bar(x=month_list, y=no_sender_id_each_month, text=no_sender_id_each_month))
    fig.update_traces(textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.update_layout(title='Successful conversations by month',
                      xaxis_title='Month',
                      yaxis_title='Number of successful conversations')
    fig.show()


    fig = go.Figure(data=[
        go.Bar(name="Successful conversations",x=month_list, y=no_sender_id_each_month, text=no_sender_id_each_month),
        go.Bar(name="Total", x=month_list, y=total_conv_on_date, text=total_conv_on_date),
    ])
    fig.update_traces(textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.update_layout(title='Successful conversations by month on total',
                      xaxis_title='Month',
                      yaxis_title='Number of conversations',
                      barmode='group',
                      )
    fig.show()


def bar_plot_week():
    # Bar plot week 1
    count_success_by_week = success_convo.groupby(["week"]).sum().reset_index()
    week_list = list(count_success_by_week["week"])
    no_thank = list(count_success_by_week["thank"])
    no_handover = list(count_success_by_week["handover"])
    week_count = count_total_convo_on_week()

    fig = go.Figure(data=[
        go.Bar(name='Thanks', x=week_list, y=no_thank),
        go.Bar(name='Order', x=week_list, y=no_handover, text=[sum(x) for x in zip(no_thank, no_handover)])
    ])
    fig.update_traces(textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.update_layout(
        title='Successful conversations by week',
        xaxis_title='Week',
        yaxis_title='Number of conversations',
        xaxis=dict(
            type='category',
        ),
        barmode='stack',
        autosize=False,
        width=900,
        height=500,
    )
    fig.show()

    # Bar plot week 2
    count_success_by_week = success_convo.groupby(["week"]).sum().reset_index()
    week_list = list(count_success_by_week["week"])
    no_thank = list(count_success_by_week["thank"])
    no_handover = list(count_success_by_week["handover"])
    week_count = count_total_convo_on_week()
    total_conv_on_week = [week_count[x] for x in week_list]

    fig = go.Figure(data=[
        go.Bar(name='Thanks', x=week_list, y=no_thank, width=0.4,
               offset=-0.4),
        go.Bar(name='Order', x=week_list, y=no_handover, width=0.4,
               offset=-0.4,
               text=[sum(x) for x in zip(no_thank, no_handover)])
    ])

    fig.add_trace(
        go.Bar(name='Total conversations', x=week_list, y=total_conv_on_week, base=0,
               width=0.4,
               offset=0.0,
               text=total_conv_on_week
               )
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.update_layout(
        title='Successful conversations by week on total',
        xaxis_title='Week',
        yaxis_title='Number of conversations',
        xaxis=dict(
            type='category',
        ),
        barmode='stack',
        autosize=False,
        width=950,
        height=800,
    )
    fig.show()


def sun_burst():
    success_rate = (len(success_convo) / len(all_convo)) * 100
    max_turn, min_turn, mean_turn, top_10_turn_happen_most, top_10_highest_no_turn, comback_customer = count_turn()
    print("All Conversations: " + str(len(all_convo)))
    print("Successful Conversations: " + str(len(success_convo)))
    print("Success rate: " + '{0:.2f}'.format(success_rate) + "%")
    print("Average number of turn: " + str(mean_turn))
    print("Comeback customers: " + str(comback_customer))

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


def count_total_convo_on_week():
    events_list = list(all_convo["events"])
    all_date = []
    for item in events_list:
        event = literal_eval(item)
        set([int(x["timestamp"]) for x in event])
        all_date += list(set(
            [datetime.datetime.utcfromtimestamp(int(x["timestamp"])).strftime('%Y-%m-%d') for x in literal_eval(item)]))
    date_counter = Counter(all_date)
    date_counter_list = [(x, y) for x, y in date_counter.items()]
    date_counter_list = sorted(date_counter_list, key=lambda x: x[0])
    week = None
    week_value = 0
    total_convo_on_week = []
    for index, item in enumerate(date_counter_list):
        if week is None:
            week = datetime.datetime.strptime(item[0], '%Y-%m-%d').isocalendar()[1]
        if week == datetime.datetime.strptime(item[0], '%Y-%m-%d').isocalendar()[1]:
            week_value += item[1]
        else:
            total_convo_on_week.append((week, week_value))
            week_value = 0
            week = datetime.datetime.strptime(item[0], '%Y-%m-%d').isocalendar()[1]
        if index == len(date_counter_list) - 1:
            total_convo_on_week.append((week, week_value))

    week_counter = {item[0]: item[1] for item in total_convo_on_week}

    return week_counter


def count_total_convo_on_month():
    events_list = list(all_convo["events"])
    all_month = []
    for item in events_list:
        event = literal_eval(item)
        all_month += list(set([datetime.datetime.utcfromtimestamp(int(x["timestamp"])).strftime('%m') for x in event]))
    month_counter = Counter(all_month)
    return month_counter


def count_turn():
    comeback_customer = 0
    events_list = list(all_convo["events"])
    all_turn = []
    for index, item in enumerate(events_list):
        event = literal_eval(item)
        all_user_turn = [x for x in event if x["event"] == "user"]
        all_turn_in_conv = [datetime.datetime.utcfromtimestamp(int(x["timestamp"])).strftime('%m-%d') for x in
                            all_user_turn]
        all_turn += list(Counter(all_turn_in_conv).values())
        if len(list(Counter(all_turn_in_conv).values())) > 1:
            comeback_customer += 1
    all_turn.remove(157)
    max_turn = max(all_turn)
    min_turn = min(all_turn)
    mean_turn = int(sum(all_turn) / len(all_turn))
    sorted_counter_turn_by_value = {k: v for k, v in
                                    sorted(dict(Counter(all_turn)).items(), key=lambda item: item[1], reverse=True)}
    sorted_counter_turn_by_key = OrderedDict(sorted_counter_turn_by_value)
    top_10_turn_happen_most = list(sorted_counter_turn_by_value.items())[:10]
    top_10_highest_no_turn = list(sorted_counter_turn_by_key.items())[:10]

    return max_turn, min_turn, mean_turn, top_10_turn_happen_most, top_10_highest_no_turn, comeback_customer


bar_plot_month()