from __future__ import unicode_literals

import requests
import json
import time
import codecs
import sys

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)


def main():
    countries = ["US"]
    api_key = "779381934495d33c6f40555a801d4a"
    # Get your key here https://secure.meetup.com/meetup_api/key/
    for (country) in countries:
        per_page = 10
        results_we_got = per_page
        offset = 0
        while (results_we_got == per_page):
            # Meetup.com documentation here: http://www.meetup.com/meetup_api/docs/2/groups/
            response = get_results(
                {"sign": "true", "country": "US", "city": "Sunnyvale", "key": api_key,
                 "page": per_page, "offset": offset, "category_id": "34", "state":"CA"})
            time.sleep(1)
            offset += 1
            #print 'response:\n', response
            results_we_got = response['meta']['count']
            for group in response['results']:
                category = ""
                if "category" in group:
                    category_id = group['category']['id']

                print ",".join(map(unicode, [group['id'], group['name'].replace(",", " "), group['rating'],
                                             category_id, group['category']['name'], group['city'],group['created'],
                                             group['lat'], group['lon'], group['country']]))

        time.sleep(1)


def get_results(params):
    request = requests.get("http://api.meetup.com/2/groups", params=params)


    data = request.json()

    return data

if __name__ == "__main__":
    main()