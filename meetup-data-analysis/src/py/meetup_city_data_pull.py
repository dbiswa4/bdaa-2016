from __future__ import unicode_literals

import requests
import json
import time
import codecs
import sys
import happybase

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)
api_key = "779381934495d33c6f40555a801d4a"
city_url = "https://api.meetup.com//2/cities"
page = 10
offset = 0
city_table = "meetup_cities"
localhost = '108.161.128.86'


def get_results(url,params):
    request = requests.get(url, params=params)
    data = request.json()
    return data


def get_cities(country, state):
    per_page = page
    city_offset = offset
    connection = happybase.Connection(localhost)
    table = connection.table(city_table)
    flag = "0"
    results_we_got = per_page
    while (results_we_got == per_page):
        response = get_results(city_url,
                               {"sign": "true", "country": country, "state":state, "key": api_key,
                                "page": per_page, "offset": city_offset})
        time.sleep(0.5)
        city_offset += 1
        results_we_got = response['meta']['count']
        for city in response['results']:
            # Print statement for testing
            # print city['country'],state, city['city']
            if country == 'US':
                table.put(str(city['id']), {'city_details:city_name': city['city'],
                                            'city_details:country': city['country'],
                                            'city_details:lat': str(city['lat']),
                                            'city_details:lon': str(city['lon']),
                                            'city_details:member_count': str(city['member_count']),
                                            'city_details:rank': str(city['ranking']),
                                            'city_details:state': city['state'],
                                            'city_details:zip': city['zip'],
                                            'city_details:processed': flag})
            else:
                # For India, there is no "state" from response
                table.put(str(city['id']), {'city_details:city_name': city['city'],
                                            'city_details:country': city['country'],
                                            'city_details:lat': str(city['lat']),
                                            'city_details:lon': str(city['lon']),
                                            'city_details:member_count': str(city['member_count']),
                                            'city_details:rank': str(city['ranking']),
                                            'city_details:zip': city['zip'],
                                            'city_details:processed': flag})




def main():
    state =''
    country = sys.argv[1]
    if country == 'US':
        state = sys.argv[2]
    get_cities(country, state=state)



if __name__ == "__main__":
    main()