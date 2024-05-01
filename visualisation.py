from dagster import op, get_dagster_logger, InputDefinition
from bokeh.plotting import figure, show, output_file, save
from bokeh.models import ColumnDataSource
from bokeh.layouts import gridplot
from bokeh.io import curdoc
import pandas as pd
import sqlalchemy

POSTGRES_URI = 'postgresql://username:password@localhost:5432/stockdata'

# Helper function to load data from PostgreSQL
def load_data_from_postgres(table_name):
    engine = sqlalchemy.create_engine(POSTGRES_URI)
    return pd.read_sql(table_name, engine)

def create_bokeh_visualizations(apple_data, nasdaq_data):
    logger = get_dagster_logger()

    # Time Series Plot
    p1 = figure(title="Apple vs NASDAQ Stock Prices Over Time", x_axis_type="datetime", plot_width=800, plot_height=400)
    p1.line(apple_data.index, apple_data['Close'], legend_label="Apple", line_color="blue")
    p1.line(nasdaq_data.index, nasdaq_data['Close'], legend_label="NASDAQ", line_color="green")
    
    # Histograms
    hist1, edges1 = np.histogram(apple_data['Close'].pct_change().dropna(), bins=50)
    hist2, edges2 = np.histogram(nasdaq_data['Close'].pct_change().dropna(), bins=50)
    p2 = figure(title="Histogram of Daily Returns", plot_width=800, plot_height=400)
    p2.quad(top=hist1, bottom=0, left=edges1[:-1], right=edges1[1:], fill_color="navy", line_color="white", alpha=0.5, legend_label="Apple")
    p2.quad(top=hist2, bottom=0, left=edges2[:-1], right=edges2[1:], fill_color="green", line_color="white", alpha=0.5, legend_label="NASDAQ")

    # Box Plots
    boxes = [apple_data['Close'], nasdaq_data['Close']]
    p3 = figure(title="Box Plot of Stock Prices", plot_width=800, plot_height=400)
    p3.quad(top=[box.quantile(0.75) for box in boxes], bottom=[box.quantile(0.25) for box in boxes], 
            left=[1, 2], right=[1.5, 2.5], fill_color="skyblue", line_color="black")
    p3.segment(x0=[1.25, 2.25], y0=[box.min() for box in boxes], x1=[1.25, 2.25], y1=[box.max() for box in boxes], line_color="black")

    # Combine all plots into a single layout
    grid = gridplot([[p1], [p2], [p3]], sizing_mode="scale_width")

    # Output to static HTML file (Alternatively, can use server_document to embed server session)
    output_file("/mnt/data/stock_visualizations.html")
    save(grid)

    logger.info("Visualizations created and saved to HTML file.")
    curdoc().add_root(grid)  # Necessary for serving this document as a standalone Bokeh application

@op(input_defs=[InputDefinition(name="run_visualization", dagster_type=bool, default_value=True)])
def visualization_op(context):
    if context.op_config['run_visualization']:
        apple_data = load_data_from_postgres('apple_stock')
        nasdaq_data = load_data_from_postgres('nasdaq_stock')
        create_bokeh_visualizations(apple_data, nasdaq_data)
    else:
        context.log.info("Visualization step was skipped based on configuration.")
