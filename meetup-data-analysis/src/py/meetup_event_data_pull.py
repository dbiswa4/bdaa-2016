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
group_url = "http://api.meetup.com/2/groups"
event_url = "https://api.meetup.com/2/events"
page = 40
offset = 0
group_table = "meetup_groups"
event_table = "meetup_events"
localhost = '108.161.128.86'


def get_results(url,params):
    request = requests.get(url, params=params)
    data = request.json()
    return data




def get_events():
    per_page = page
    event_offset = offset
    group_flag = "1"
    connection = happybase.Connection(localhost)
    group_con = connection.table(group_table)
    event_con = connection.table(event_table)
    row_key = b'10040742'
    status = "past"
    for i in range(10):
        for key, data in group_con.scan(row_start=row_key, limit=3000):
            temp = key
            processed = data['group_details:processed']
            if processed == '0':
                results_we_got = per_page
                while (results_we_got == per_page):
                    response = get_results(event_url,
                                           {"sign": "true", "key": api_key, "page": per_page,
                                            "offset": event_offset, "group_id": key, "status": status})
                    time.sleep(0.5)
                    event_offset += 1
                    exception_count = 0
                    results_we_got = response['meta']['count']
                    for event in response['results']:
                        # print group
                        # print "event" + event['id']
                        # print "group" + str(event['group']['id'])
                        try:
                            desc = event['description'].encode('ascii', 'ignore').encode('utf8').replace('\n', ' ')
                        except Exception as e:
                            desc = 'Not Available'
                            exception_count += 1
                            pass

                        event_con.put(str(event['id']), {'event_details:event_name': event['name'],
                                                         'event_details:event_dt': str(event['time']),
                                                         'event_details:event_create_dt': str(event['created']),
                                                         'event_details:desc': desc,
                                                         'event_details:attendance_count': str(event['headcount']),
                                                         'event_details:yes_rsvp_count': str(event['yes_rsvp_count']),
                                                         'event_details:grp_id': str(event['group']['id'])})

                        group_con.put(str(key), {'group_details:processed': str(group_flag)})
                        time.sleep(0.5)
        row_key = temp.encode()

    # print temp




def main():
    # state =''
    # country = sys.argv[1]
    # if country == 'US':
    #     state = sys.argv[2]
    # get_groups(country, state=state)
    get_events()



if __name__ == "__main__":
    main()