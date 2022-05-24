import os
import time
from datetime import datetime
from dotenv import load_dotenv
import httplib2
import apiclient
from oauth2client.service_account import ServiceAccountCredentials
from sql import get_data, insert_data, create_bd, update_data, change_data
from Telegram import send_telegram
import schedule


load_dotenv()


def get_sheets():
    CREDENTIALS_FILE = 'creds.json'
    spreadsheet_id = os.getenv("FILE")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)
    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=os.getenv("RANGE"),
        majorDimension='ROWS'
    ).execute()
    return tuple(tuple(x) for x in values['values'])


def check_rows():
    values = get_sheets()
    bd = get_data()
    new_rows = []
    if len(values) > len(bd):
        list_from_bd = [item[1] for item in bd]
        list_from_sheet = [int(item[1]) for item in values]
        new_row_id = set(list_from_sheet)-set(list_from_bd)
        for row in values:
            if int(row[1]) in new_row_id:
                new_rows.append(row)
        return "INSERT", new_rows
    elif len(values) < len(bd):
        list_from_bd = [item[1] for item in bd]
        list_from_sheet = [item[1] for item in values]
        delete_row_id = set(list_from_bd)-set(list_from_sheet)
        return "DELETE", delete_row_id
    else:
        return False


def check_changes():
    values = get_sheets()
    bd_data = (map(str, x) for x in get_data())
    changes = []
    for i, item in enumerate(bd_data):
        if not tuple(item) == values[i]:
            changes.append(values[i])
    return changes


def check_date():
    bd = get_data()
    for item in bd:
        if datetime.strptime(item[3], '%d.%m.%Y').strftime('%d.%m.%Y') == datetime.now().strftime('%d.%m.%Y'):
            send_telegram(f'У заказа {item[1]} сегодня истекает срок поставки')


def main_loop():
    create_bd()
    insert_data(get_sheets())
    schedule.every().day.at(os.getenv("TIME")).do(check_date)
    while True:
        schedule.run_pending()
        if check_rows():
            change_data(*check_rows())
        elif check_changes():
            update_data(check_changes())
        time.sleep(10)


if __name__ == '__main__':
    main_loop()
