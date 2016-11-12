import urllib
import json
import pandas as pd
import os


def write_file(file_name,data):
    with open(file_name,'a') as f:
        f.write(data)

def parse_city(delim):
    """

    :return:
    """

    base_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/data"
    out_path =  "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/processed_data"
    out_name = os.path.join(out_path, 'meetup_cities.dat')

    for file in os.listdir(base_path):
        if file.startswith("meetup_cities"):
            full_name = os.path.join(base_path,file)

            with open(full_name,'r') as f:
                response = json.loads(f.read())
                results = response["results"]
                for record in results:
                    data = str(record["city"]) + delim + str(record["country"]) + delim + \
                           str(record["lat"]) + delim + str(record["lon"]) + delim + str(record["member_count"]) + \
                           delim + str(record["ranking"]) + delim + str(record["zip"]) + '\n'
                    write_file(out_name,data)

def parse_group(delim):
    """

    :return:
    """
    base_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/data"
    out_path =  "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/processed_data"
    out_name = os.path.join(out_path, 'meetup_groups.dat')

    for file in os.listdir(base_path):
        if file.startswith("meetup_groups"):
            full_name = os.path.join(base_path,file)
            with open(full_name,'r') as f:
                response = json.loads(f.read())
                for record in response:
                    data = str(record["id"]) + delim + str(record["name"]) + delim + \
                           unicode(record["description"]).encode('utf8').replace('\n',' ') + str(record["city"]) + delim + \
                           str(record["country"]) + delim + str(record["created"]) + delim + str(record["category"]["id"]) + delim + \
                           str(record["join_mode"]) + delim + str(record["visibility"]) + delim + str(record["urlname"]) + delim +\
                           str(record["members"]) + '\n'
                    write_file(out_name,data)


def parse_events(delim):
    """

    :return:
    """
    base_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/data"
    out_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/processed_data"
    out_name = os.path.join(out_path, 'meetup_events.dat')

    for file in os.listdir(base_path):
        if file.startswith("meetup_events"):
            full_name = os.path.join(base_path,file)
            with open(full_name,'r') as f:
                response = json.loads(f.read())
                for record in response:
                    data = str(record["id"]) + delim + str(record["group"]["id"]) + delim + \
                           str(record["waitlist_count"]) + delim + str(record["yes_rsvp_count"]) + \
                           delim + str(record.get("duration","")) + delim + str(record["time"]) + \
                           str(record["utc_offset"]) + delim + record["name"] + '\n'
                    write_file(out_name,data)


def parse_categories(delim):
    """

    :return:
    """
    base_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/data"
    out_path = "/Volumes/Seagate Backup Plus Drive/Bala_Personal_Backup/Learning/Indiana University/BAA-2016/project/dev/processed_data"
    out_name = os.path.join(out_path, 'meetup_categories.dat')

    for file in os.listdir(base_path):
        if file.startswith("categories"):
            full_name = os.path.join(base_path,file)
            with open(full_name,'r') as f:
                response = json.loads(f.read())
                for record in response["results"]:
                    data = str(record["id"]) + delim + str(record["name"]) + '\n'
                    write_file(out_name,data)


def main():
    delim = '\001\005\001'
    parse_categories(delim)


if __name__ == "__main__":
    main()

