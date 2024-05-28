from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource
from bokeh.layouts import column
import pandas as pd
import time
import os

source = ColumnDataSource(data=dict(x=[], y=[]))

p = figure(title="Real-Time Data", x_axis_label='x', y_axis_label='y')
p.line(x='x', y='y', source=source, line_width=2)

def update():
    while True:
        if os.path.exists("output/signal.txt"):
            with open("output/signal.txt", "r") as f:
                batch_file = f.readline().strip()
            if batch_file:
                # Read the latest batch data
                batch_df = pd.read_csv(f"output/csv/{batch_file}.csv")
                new_data = dict(
                    x=batch_df['column1'].tolist(),  # Adjust according to your column names
                    y=batch_df['column2'].tolist()   # Adjust according to your column names
                )
                source.stream(new_data, rollover=200)
                os.remove("output/signal.txt")
        time.sleep(1)

import threading
thread = threading.Thread(target=update)
thread.start()

curdoc().add_root(column(p))

from bokeh.server.server import Server
server = Server({'/': lambda doc: doc.add_root(column(p))}, port=5006)
server.start()
server.io_loop.add_callback(server.show, "/")
server.io_loop.start()
