from __future__ import unicode_literals

import requests
import json
import time
import codecs
import sys
import happybase

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)


categories = ["2","34"]
api_key = "779381934495d33c6f40555a801d4a"
group_url = "http://api.meetup.com/2/groups"
page = 40
offset = 0
city_table = "meetup_cities"
group_table = "meetup_groups"
localhost = '108.161.128.86'


def get_results(url,params):
    request = requests.get(url, params=params)
    data = request.json()
    return data




def get_groups():
    per_page = page
    group_offset = offset
    city_flag = "1"
    group_flag = "0"
    connection = happybase.Connection(localhost)
    city_con = connection.table(city_table)
    group_con = connection.table(group_table)
    row_key = b'1018090'
    # row_key = b'501'
    for i in range(10):
        for key, data in city_con.scan(row_start=row_key, limit=3000):
            country = data['city_details:country']
            temp = key
            processed = data['city_details:processed']
            if processed == '0':
                for (categoty) in categories:
                    results_we_got = per_page
                    while (results_we_got == per_page):
                        if country == 'us':
                            # print country
                            response = get_results(group_url,
                                                   {"sign": "true", "country": country,
                                                    "city": data['city_details:city_name'],
                                                    "state": data['city_details:state'],
                                                    # "city": "Phoenix",
                                                    # "state": "AZ",
                                                    "key": api_key,
                                                    "page": per_page, "offset": group_offset,
                                                    "category_id": categoty})

                        else:
                            # print country
                            response = get_results(group_url,
                                                   {"sign": "true", "country": country,
                                                    "city": data['city_details:city_name'],
                                                    # "city": "Mumbai",
                                                    "key": api_key,
                                                    "page": per_page, "offset": group_offset, "category_id": categoty})
                        time.sleep(0.5)
                        group_offset += 1
                        results_we_got = response['meta']['count']
                        exception_count = 0
                        print results_we_got, key
                        for group in response['results']:
                            # print key, group['city']
                            # print group['category']['id'],group['name'],group['city'],country,key
                            # print group['description'].encode('ascii', 'ignore').encode('utf8').replace('\n', ' ')
                            try:
                                desc = group['description'].encode('ascii', 'ignore').encode('utf8').replace('\n', ' ')
                            except Exception as e:
                                desc = 'Not Available'
                                exception_count += 1
                                pass

                            group_con.put(str(group['id']), {'group_details:grp_name': group['name'],
                                                             'group_details:rating': str(group['rating']),
                                                             'group_details:category': str(group['category']['id']),
                                                             'group_details:city': group['city'],
                                                             'group_details:create_date': str(group['created']),
                                                             'group_details:desc': desc,
                                                             # 'group_details:last_event_date': group['state'],
                                                             'group_details:lat': str(group['lat']),
                                                             'group_details:lon': str(group['lon']),
                                                             'group_details:processed': str(group_flag)})


                            city_con.put(str(key), {'city_details:processed': str(city_flag)})
                    time.sleep(0.5)
        row_key = temp.encode()

    # print temp




def main():
    # state =''
    # country = sys.argv[1]
    # if country == 'US':
    #     state = sys.argv[2]
    # get_groups(country, state=state)
    get_groups()



if __name__ == "__main__":
    main()