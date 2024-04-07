import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import csv
from airflow.operators.python import PythonOperator
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geodatasets import get_path
import numpy as np


# Define default arguments
default_args = {
    "owner": "your_name",  # Replace "your_name" with actual owner name
    "start_date": datetime(2022, 4, 10),
    "retries": 1,
}

unzip_dir = "/home/vasanthakumarg/bigdata/assisgnment2/climate_data/"
output_dir = "/home/vasanthakumarg/bigdata/assisgnment2/"
root_dir = "/home/vasanthakumarg/bigdata/assisgnment2/"

def create_images():
    """
    Function to create heatmap images for wind speed and temperature data.
    """

    def plot_images_helper(line):
        """
        Helper function to plot heatmaps for wind speed and temperature data.
        """
        month, data = eval(line.strip())
        df = pd.DataFrame(data)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
        )
        world = gpd.read_file(get_path("naturalearth.land"))

        # Plot heatmaps for each field
        fields = ["AverageWindSpeed", "AverageDryBulbTemperature"]
        for field in fields:
            ax = world.plot(color="white", edgecolor="black")
            gdf.plot(ax=ax, column=field, cmap="viridis", legend=True)
            ax.set_title(f"{field} data for the month {month}")
            plt.savefig(f"{root_dir}images/{month}_{field}_heatmap.png")

    def format_averages_file():
        """
        Function to format the monthly averages file.
        """
        with open("/home/vasanthakumarg/bigdata/assisgnment2/monthly.txt-00000-of-00001", "r") as file:
            data = [eval(line.strip()) for line in file]

        # Create a dictionary for each month
        months = {}
        for latitude_longitude, month_data in data:
            for month, values in month_data.items():
                if month not in months:
                    months[month] = {
                        "Latitude": [],
                        "Longitude": [],
                        "AverageWindSpeed": [],
                        "AverageDryBulbTemperature": [],
                    }
                months[month]["Latitude"].append(latitude_longitude[0])
                months[month]["Longitude"].append(latitude_longitude[1])
                months[month]["AverageWindSpeed"].append(values["AverageWindSpeed"])
                months[month]["AverageDryBulbTemperature"].append(
                    values["AverageDryBulbTemperature"]
                )
        with open("/home/vasanthakumarg/bigdata/assisgnment2/updated_monthly.txt", "w") as file:
            for month, data in months.items():
                file.write("(" + str((month, data)) + ")\n")

    format_averages_file()
    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | beam.Create(
                convert_textfiles_to_lines("/home/vasanthakumarg/bigdata/assisgnment2/updated_monthly.txt")
            )
            | beam.Map(plot_images_helper)
        )

def convert_textfiles_to_lines(file_pattern):
    """
    Function to convert text file lines to a list of strings.
    """
    with open(file_pattern, "r") as file:
        lines = file.readlines()
        return [line.strip() for line in lines]

def data_extraction():
    """
    Function to extract data from CSV files and filter required fields.
    """
    def extract_csv(csv_file):
        """
        Function to extract data from a CSV file.
        """
        csv_path = os.path.join(unzip_dir, csv_file)
        df = pd.read_csv(csv_path)

        # Extract required fields
        required_fields = [
            "HourlyWindSpeed",
            "HourlyDryBulbTemperature",
        ]  # Add more fields as needed

        # Drop rows with NaN values in the required fields
        df = df.dropna(subset=required_fields)

        # Filter the DataFrame based on the required fields
        filtered_df = df[["DATE"] + required_fields]

        # Extract Lat/Long values
        latitude_longitude = (df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0])

        # Create a tuple of the form <Lat, Lon, [[Date, Windspeed, BulbTemperature, RelativeHumidity, WindDirection], ...]>
        return latitude_longitude, filtered_df.values.tolist()

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Read from CSV file
        pcollection = (
            pipeline
            |  beam.Create(os.listdir(unzip_dir))
            |  beam.Map(extract_csv)
            |  beam.io.WriteToText(os.path.join(output_dir, "extract.txt"))
        )

def find_average():
    """
    Function to compute monthly averages.
    """
    output_file_path = "/home/vasanthakumarg/bigdata/assisgnment2/monthly.txt"

    def compute_average_func(line):
        """
        Function to compute monthly averages.
        """
        data_tuple = eval(
            line.strip()
        )  # Assuming the tuple format is preserved in the extract.txt file
        latitude_longitude = data_tuple[0]
        data_list = data_tuple[1]

        monthly_averages = {}
        for data_point in data_list:
            date_str, windspeed, bulb_temp = data_point
            month = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m")

            if month not in monthly_averages:
                monthly_averages[month] = {
                    "AverageWindSpeed": [],
                    "AverageDryBulbTemperature": [],
                }

            # Append values for each field, ignoring NaN values
            if not pd.isna(windspeed):
                monthly_averages[month]["AverageWindSpeed"].append(windspeed)

            if not pd.isna(bulb_temp):
                if not isinstance(bulb_temp, int):
                    bulb_temp = (
                        bulb_temp[: len(bulb_temp) - 1]
                        if bulb_temp[-1] == "s"
                        else bulb_temp
                    )
                    monthly_averages[month]["AverageDryBulbTemperature"].append(
                        float(bulb_temp)
                    )

        for month in monthly_averages.keys():
            monthly_averages[month]["AverageWindSpeed"] = sum(
                monthly_averages[month]["AverageWindSpeed"]
            ) / len(monthly_averages[month]["AverageWindSpeed"])
            monthly_averages[month]["AverageDryBulbTemperature"] = sum(
                monthly_averages[month]["AverageDryBulbTemperature"]
            ) / len(monthly_averages[month]["AverageDryBulbTemperature"])

        # Write the monthly averages with Lat/Long
        return latitude_longitude, monthly_averages

    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | beam.Create(
                convert_textfiles_to_lines("/home/vasanthakumarg/bigdata/assisgnment2/extract.txt-00000-of-00001")
            )
            | beam.Map(compute_average_func)
            | beam.io.WriteToText(output_file_path)
        )

# Convert the dictionary to a DataFrame for each month

dag = DAG("analytics_pipeline", default_args=default_args, schedule_interval=None)

sense_file = FileSensor(
    task_id="wait_for_file",
    filepath="/home/vasanthakumarg/bigdata/assisgnment2/data_archived.zip",
    timeout=5,
    dag=dag,
)

unzip = BashOperator(
    task_id="unzip",
    bash_command="unzip -d /home/vasanthakumarg/bigdata/assisgnment2/climate_data /home/vasanthakumarg/bigdata/assisgnment2/data_archived.zip",
    dag=dag,
)

filter_data = PythonOperator(
    task_id="process_data_task",
    python_callable=data_extraction,
    provide_context=True,
    dag=dag,
)

compute_monthly_average = PythonOperator(
    task_id="compute_monthly_average",
    python_callable=find_average,
    provide_context=True,
    dag=dag,
)

plot_heatmaps = PythonOperator(
    task_id="plot_heatmaps",
    python_callable=create_images,
    provide_context=True,
    dag=dag,
)

sense_file >> unzip >> filter_data >> compute_monthly_average >> plot_heatmaps
