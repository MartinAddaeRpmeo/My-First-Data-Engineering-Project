import pandas as pd
import xml.etree.ElementTree as ET
import sqlite3
import glob
from datetime import datetime, time

target_file = "Transformed_csv.csv"
log_file = "logging_file.txt"

def extract_from_csv(file_to_process):
    data = pd.read_csv(file_to_process)
    return data

def extract_from_json(file_to_process):
    data = pd.read_json(file_to_process, lines=True)
    return data

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        data = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return data

def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])
    
    for csvfile in glob.glob("*.csv"):
        data = pd.DataFrame(extract_from_csv(csvfile))
        extracted_data = pd.concat([extracted_data, data], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        data = pd.DataFrame(extract_from_json(jsonfile))
        extracted_data = pd.concat([extracted_data, data], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        data = pd.DataFrame(extract_from_xml(xmlfile))
        extracted_data = pd.concat([extracted_data, data], ignore_index=True)
    return extracted_data

def transform(data):
    data["height"] = round(data["height"] * 0.0254, 2)
    data["weight"] = round(data["weight"] * 0.45359237, 2)
    return data

def load(data):
    data.to_csv(target_file)
    my_db = sqlite3.connect("my_db.db")
    data.to_sql("Person", my_db, if_exists="replace", index=False)

def logging(message):
    time = datetime.now()
    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    timestamp = time.strftime(timestamp_format)

    with open(log_file, "a") as f:
        f.write(timestamp + "," + message + "\n")

# Add if __name__ == "__main__"
if __name__ == "__main__":
    logging("ETL Process Started")

    logging("Extract Process Started")
    data = extract()
    logging("Extract Process Finished")

    logging("Transform Process Started")
    data = transform(data)
    logging("Transform Process Finished")

    logging("Load Process Started")
    if data.empty:
        load(None)
    else:
        load(data)
    logging("Load Process Finished")

    logging("ETL Process Finished")
