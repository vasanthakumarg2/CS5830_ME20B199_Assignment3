# Importing necessary modules and packages:
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os, zipfile
import random

# Defining a function to pick random files:
def pick_random_files():
    """
    Function to randomly select files from a list of links and save them to a file.
    """
    number_of_files = 5
    links = []
    with open("/home/vasanthakumarg/bigdata/assisgnment2/links.txt", "r") as file:
        for link in file:
            links.append(link)
    random_links = random.sample(links, number_of_files)
    with open("/home/vasanthakumarg/bigdata/assisgnment2/random_links.txt", "w") as file:
        for link in random_links:
            file.write(base_url + link)

# Defining a function to zip a folder:
def zip_folder(folder_to_zip, zip_file_name):
    """
    Function to zip a folder.
    
    Parameters:
        folder_to_zip (str): Path of the folder to be zipped.
        zip_file_name (str): Name of the zip file to create.
    """
    # Create a zip file
    with zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the folder and add each file to the zip
        for root, _, files in os.walk(folder_to_zip):
            for file in files:
                file_path = os.path.join(root, file)
                # Write the file to the zip with its relative path
                zipf.write(file_path, os.path.relpath(file_path, folder_to_zip))


default_args = {
    "owner": "your_name",  # Replace "your_name" with actual owner name
    "start_date": datetime(2022, 4, 10),
    "retries": 1,
}


dag = DAG("task1_data_fetch", default_args=default_args, schedule_interval=None)

base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/"


# Define tasks
def zip_folder_task():
    """
    Task to zip a folder.
    """
    zip_folder("/home/vasanthakumarg/bigdata/assisgnment2/data", "/home/vasanthakumarg/bigdata/assisgnment2/data_compressed.zip")

# Gets the link from the website and puts them in a "links.txt" file"
extract_csv_files = BashOperator(
    task_id="extract_csv_links",
    bash_command= "curl -s https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/ | grep -o -E 'href=\"([^\"]+)\"' | awk -F'\"' '{print $2}' > /home/vasanthakumarg/bigdata/assisgnment2/links.txt",
    dag=dag,
)
pick_random_files = PythonOperator(
    task_id="pick_random_files",
    python_callable=pick_random_files,
    dag=dag,
)
download_csv_files = BashOperator(task_id="download_csv_files", bash_command=r"cd /home/vasanthakumarg/bigdata/assisgnment2/data && xargs -n 1 curl -O -J < /home/vasanthakumarg/bigdata/assisgnment2/random_links.txt --create-dirs -P ./data", dag=dag)
archive_data = PythonOperator(
    task_id="compress_data", python_callable=zip_folder_task, dag=dag
)
# Set task dependencies
extract_csv_files >> pick_random_files >> download_csv_files >> archive_data
