from __future__ import unicode_literals

import requests
import json
import time
import codecs
import sys
import happybase

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

countries = ["IN"]
api_key = "779381934495d33c6f40555a801d4a"
city_url = "https://api.meetup.com//2/cities"
group_url = "http://api.meetup.com/2/groups"
event_url = "https://api.meetup.com/2/events"
page = 1
offset = 0



def get_results(url,params):
    request = requests.get(url, params=params)
    data = request.json()
    return data


def get_cities():
    per_page = page
    city_offset = offset
    table_name = "meetup_cities_test"
    connection = happybase.Connection('108.161.128.86')
    table = connection.table(table_name)
    for (country) in countries:
        results_we_got = per_page
        while (results_we_got == per_page):
            response = get_results(city_url,
                {"sign": "true", "country": country, "key": api_key,
                 "page": per_page, "offset": city_offset})
            time.sleep(1)
            city_offset += 1
            results_we_got = response['meta']['count']
            for city in response['results']:
                if country == 'US':
                    table.put(city['id'], {'city_details:city_name': city['city'],
                                           'city_details:country': city['country'],
                                           'city_details:lat': city['lat'],
                                           'city_details:lon': city['lon'],
                                           'city_details:member_count': city['member_count'],
                                           'city_details:rank': city['rank'],
                                           'city_details:state': city['state'],
                                           'city_details:zip': city['zip']})
                else:
                    # For India, there is no "state"
                    table.put(city['id'], {'city_details:city_name': city['city'],
                                           'city_details:country': city['country'],
                                           'city_details:lat': city['lat'],
                                           'city_details:lon': city['lon'],
                                           'city_details:member_count': city['member_count'],
                                           'city_details:rank': city['rank'],
                                           'city_details:zip': city['zip']})

        time.sleep(1)




def main():
    get_cities()


if __name__ == "__main__":
    main()