import sqlite3
import requests
import json
import sys
import time
import datetime
import logging

conn = sqlite3.connect('ctpl.db')
c = conn.cursor()

def setup_logging():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    handler = logging.FileHandler('scraper.log')
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)


def db_create_table():
    c.execute("CREATE TABLE IF NOT EXISTS scheduled_qty(tsp INTEGER, "
              "tsp_name TEXT, "
              "loc TEXT, "
              "loc_name TEXT, "
              "gas_day TEXT, "
              "cycle TEXT, "
              "sched_qty INTEGER, "
              "qty_avail REAL, "
              "design_cap REAL, "
              "oper_cap REAL, "
              "posting_dt TEXT, "
              "timestamp INTEGER)")

def db_update_table(gas_day, data):
    unix = time.time()
    # date = str(datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S'))

    # print(data['loc'])
    sql = """SELECT posting_dt FROM scheduled_qty WHERE loc = '%s' ORDER BY timestamp DESC LIMIT 1""" % (data['loc'])
    c.execute(sql)
    rows = c.fetchall()

    if len(rows) == 0:
        print("No record %s found in database...adding." % (data['loc']))
        c.execute(
            "INSERT INTO scheduled_qty(tsp, tsp_name, loc, loc_name, gas_day, cycle, sched_qty, qty_avail, oper_cap, posting_dt, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (data['tsp'], data['tsP_NAME'], data['loc'], data['loC_NAME'], gas_day, data['cycle'], data['scheD_QTY'],
             data['qtY_AVAIL'], data['opeR_CAP'], data['postinG_DT_TIME'], unix))
        conn.commit()
        return

    if data['postinG_DT_TIME'] != rows[0][0]:
        print("New record found for %s...updating." % (data['loc']))
        c.execute(
            "INSERT INTO scheduled_qty(tsp, tsp_name, loc, loc_name, gas_day, cycle, sched_qty, qty_avail, oper_cap, posting_dt, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (data['tsp'], data['tsP_NAME'], data['loc'], data['loC_NAME'], gas_day, data['cycle'], data['scheD_QTY'],
             data['qtY_AVAIL'], data['opeR_CAP'], data['postinG_DT_TIME'], unix))
        conn.commit()

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    handler = logging.FileHandler('ctpl.log')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    logger.info('Starting CTPL scraper')
    db_create_table()

    url = "http://lngconnectionapi.cheniere.com/api/Capacity/GetCapacity?tspNo=200&beginDate=null&cycleId=null&locationId=0"

    try:
        jsonData = requests.get(url)
        jsonData.raise_for_status()
    except requests.exceptions.BaseHTTPError as err:
        logger.error('unable to connect to API', exc_info=True)
        sys.exit(1)

    #     jsonData.raise_for_status()
    # except requests.exceptions.BaseHTTPError as err:
    #     print(err)
    #     sys.exit(1)

    payload = jsonData.json()
    with open('ctpl.json', 'w') as outfile:
        json.dump(payload, outfile, sort_keys=True, indent=4, ensure_ascii=False)

    gas_day = payload['beginDate']

    print(gas_day)
    # print(str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))

    [db_update_table(gas_day, records) for records in payload['report']]
    # [print(records) for records in payload['report']]

    # for key in payload:
    #     value = payload[key]
    #     print("The key and value are ({}) = ({})".format(key, value))

    c.close()
    conn.close()
    logger.info('Completed')


if __name__ == "__main__":
    main()
