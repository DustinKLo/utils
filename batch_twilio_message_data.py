import os
import csv
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from twilio.rest import Client

account_sid = os.environ["TWILIO_ACCOUNT_SID"]
auth_token = os.environ["TWILIO_AUTH_TOKEN"]
client = Client(account_sid, auth_token)

count = 0

headers = [
    'sid',
    'message_type',
    'direction',
    'body',
    'date_created',
    'date_updated',
    'account_sid',
    'messaging_service_sid',
    'to',
    'from',
    'from_2',
    'status'
]

START_MONTH_YEAR = datetime(2018, 9, 5).date()
END_MONTH_YEAR = datetime(2018, 9, 20).date()
DATE_RANGE = []
while(START_MONTH_YEAR <= datetime(2018, 5, 1).date()):
    DATE_RANGE.append(START_MONTH_YEAR)
    START_MONTH_YEAR = START_MONTH_YEAR + relativedelta(months=+1)
    
for start_of_month in DATE_RANGE:
    counter = 0
    end_of_month = start_of_month + relativedelta(months=+1)
    file_name = "twilio_outbound_{}_to_{}.csv".format(start_of_month, end_of_month)
    print(file_name)
    
    with open(file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter="|")

        writer.writerow(headers)
        print('wrote headers')

        messages = client.messages.stream(date_sent_after=start_of_month, date_sent_before=end_of_month)
        print('stream connected')

        for message in messages:
            if message.body:
                row = (
                    message.sid,
                    "sms",
                    message.direction,
                    str(message.body).encode("utf-8"),
                    message.date_created,
                    message.date_updated,
                    message.account_sid,
                    message.messaging_service_sid,
                    message.to,
                    message.from_,
                    None, # message.from_[2:],
                    message.status,
                )
                writer.writerow(row)
                csv_file.flush()

            counter += 1
            if counter % 25000 == 0:
                print('{} messages pulled at timestamp {}'.format(counter, message.date_created))
                # break

