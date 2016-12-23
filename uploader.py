import csv
import sqlite3
import time
import sys

__author__ = 'Paul Lamb <callmepauley@yahoo.com>'

conn = sqlite3.connect('ngpl.db')
c = conn.cursor()


tsp = 6931794
tsp_name = "NATURAL GAS PIPELINE CO."
loc = "46622"
loc_name = "SPLIQ/NGPL SABINE PASS LIQUEFACTION"
cycle = "EVENING"

unix = time.time()
recs = []

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


def db_update_table(data):
    unix = time.time()
    # date = str(datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S'))

    print("Adding record: %s" % (data[4]))
    c.execute(
        "INSERT INTO scheduled_qty(tsp, tsp_name, loc, loc_name, gas_day, cycle, sched_qty, qty_avail, oper_cap, posting_dt, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (data['tsp'], data['tsP_NAME'], data['loc'], data['loC_NAME'], gas_day, data['cycle'], data['scheD_QTY'],
        data['qtY_AVAIL'], data['opeR_CAP'], data['postinG_DT_TIME'], unix))
    conn.commit()

    #     return
    #
    # if data['postinG_DT_TIME'] != rows[0][0]:
    #     print("New record found for %s...updating." % (data['loc']))
    #     c.execute(
    #         "INSERT INTO scheduled_qty(tsp, tsp_name, loc, loc_name, gas_day, cycle, sched_qty, qty_avail, oper_cap, posting_dt, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
    #         (data['tsp'], data['tsP_NAME'], data['loc'], data['loC_NAME'], gas_day, data['cycle'], data['scheD_QTY'],
    #          data['qtY_AVAIL'], data['opeR_CAP'], data['postinG_DT_TIME'], unix))
    #     conn.commit()

# try:
#     f = open('Evening.csv', 'r')
# except Exception as e:
#     print(str(e))
#     sys.exit(1)

with open('Evening.csv', 'r') as csvfile:
    f = csv.reader(csvfile, dialect='excel')
    for row in f:
        rec =[]
        gas_day = row[0].split("-")
        # print(gas_day[1]+"/"+gas_day[2]+"/"+gas_day[0])
        # print(row[6])

        rec.append(tsp)
        rec.append(tsp_name)
        rec.append(row [1])
        rec.append(row[2])
        rec.append(gas_day[1]+"/"+gas_day[2]+"/"+gas_day[0])
        rec.append(cycle)
        rec.append(int(row[8].replace(',','')))     # scheduled qty
        rec.append(float(row[6].replace(',','')))         # design cap
        rec.append(float(row[7].replace(',','')))         # operating cap
        rec.append(3)               # post dt, format yyy-mm-ddThh:mm
        rec.append(int(time.time()))

        recs.append(rec)

print(recs)
print(len(recs))

db_create_table()


# [db_update_table(gas_day, records) for records in recs]
[db_update_table(records) for records in recs]


    # t.append(datetime.strptime(" ".join(line.split()[:2]), "%Y-%m-%d %H:%M"))
    # heights.append(float(line.split()[2]))
# f.close()
