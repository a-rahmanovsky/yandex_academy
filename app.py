from flask import Flask, request, Response
import sqlite3
import json
import sys, traceback
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np

app = Flask(__name__)


def get_citizen_info(import_id, citizen_id=-1):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    if citizen_id == -1:
        sql = "select * from citizens where import_id = ?"
        cursor.execute(sql, [import_id])
        citizens = cursor.fetchall()
        data = []
        for citizen in citizens:
            citizen_json = {"citizen_id": citizen[1], "town": citizen[2], "street": citizen[3], "building": citizen[4],
                            "appartement": citizen[5], "name": citizen[6], "birth_date": citizen[7],
                            "gender": citizen[8]}
            sql = "select * from relatives where import_id=? and citizen_id = ?"
            cursor.execute(sql, [import_id, citizen_json["citizen_id"]])
            relatives = cursor.fetchall()
            data_relative = []
            for relative in relatives:
                data_relative.append(relative[2])
            citizen_json["relatives"] = data_relative
            data.append(citizen_json)
        return data
    else:
        sql = "select * from citizens where import_id = ? and citizen_id = ?"
        cursor.execute(sql, [import_id, citizen_id])
        citizen = cursor.fetchall()
        citizen = citizen[0]
        citizen_json = {"citizen_id": citizen[1], "town": citizen[2], "street": citizen[3], "building": citizen[4],
                        "appartement": citizen[5], "name": citizen[6], "birth_date": citizen[7],
                        "gender": citizen[8]}
        sql = "select * from relatives where import_id=? and citizen_id = ?"
        cursor.execute(sql, [import_id, citizen_id])
        relatives = cursor.fetchall()
        data_relative = []
        for relative in relatives:
            data_relative.append(relative[2])
        citizen_json["relatives"] = data_relative
        return citizen_json

def make_relatives_correct(import_id, citizen_id, new_relatives):
    # Получить всех родственников этого человека
    # Пройтись по ним и если в новом списке нет текущего родственника - удалить его и удалить обраточку
    # Пройтись по ним и если в новом списке есть родственник, которого нет в текущем - добавить его и добавить обраточку

    def delete_relatives(import_id, citizen_id1, citizen_id2):
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        sql = "delete from relatives where import_id = ? and citizen_id = ? and relative_id = ?"
        cursor.execute(sql, [import_id, citizen_id1, citizen_id2])
        cursor.execute(sql, [import_id, citizen_id2, citizen_id1])
        conn.commit()

    def add_relatives(import_id, citizen_id1, citizen_id2):
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        sql = "insert into relatives values (?, ?, ?)"
        cursor.execute(sql, [import_id, citizen_id1, citizen_id2])
        cursor.execute(sql, [import_id, citizen_id2, citizen_id1])
        conn.commit()

    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    sql = "select relative_id from relatives where import_id = ? and citizen_id = ?"
    cursor.execute(sql, [import_id, citizen_id])
    current_relatives = cursor.fetchall()
    current_relatives = [i[0] for i in current_relatives]
    for relative in current_relatives:
        if relative not in new_relatives:
            delete_relatives(import_id, citizen_id, relative)

    for relative in new_relatives:
        if relative not in current_relatives:
            add_relatives(import_id, citizen_id, relative)



@app.route('/imports', methods=['POST'])
def imports_data():
    try:
        citizens = json.loads(request.get_data().decode('utf-8'))['citizens']
        sql = "select max(import_id) from citizens"
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        cursor.execute(sql)
        max_import_id = cursor.fetchall()
        cur_import_id = max_import_id[0][0] + 1 if max_import_id[0][0] else 1
        for citizen in citizens:
            sql = "insert into citizens values (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(sql, [cur_import_id, citizen["citizen_id"], citizen["town"],
                                 citizen["street"], citizen["building"], citizen["appartement"],
                                 citizen["name"], citizen["birth_date"], citizen["gender"]])
            for relative in citizen["relatives"]:
                sql = "insert into relatives values (?, ?, ?)"
                cursor.execute(sql, [cur_import_id, citizen["citizen_id"], relative])
        conn.commit()
    except:
        return Response(status=400)

    return Response(json.dumps({"data": {"import_id": cur_import_id}}), status=201)


@app.route('/imports/<import_id>/citizens')
def return_data(import_id):
    try:
        data = get_citizen_info(import_id)
    except:
        print(traceback.print_exc(file=sys.stdout))
        return Response(status=400)

    return Response(json.dumps({"data": data}).encode('utf-8'), status=200)


@app.route('/imports/<import_id>/citizens/<citizen_id>', methods=['PATCH'])
def update_data(import_id, citizen_id):
    try:
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        data = json.loads(request.get_data().decode('utf-8'))
        for key, value in data.items():
            sql = ""
            if key == "town":
                sql = "update citizens set town = ? where import_id = ? and citizen_id = ?"
            if key == "street":
                sql = "update citizens set street = ? where import_id = ? and citizen_id = ?"
            if key == "building":
                sql = "update citizens set building = ? where import_id = ? and citizen_id = ?"
            if key == "appartement":
                sql = "update citizens set appartement = ? where import_id = ? and citizen_id = ?"
            if key == "name":
                sql = "update citizens set name = ? where import_id = ? and citizen_id = ?"
            if key == "birth_date":
                sql = "update citizens set birth_date = ? where import_id = ? and citizen_id = ?"
            if key == "gender":
                sql = "update citizens set gender = ? where import_id = ? and citizen_id = ?"
            if key != "relatives" and len(sql) > 0:
                cursor.execute(sql, [value, import_id, citizen_id])

        if "relatives" in data:
            make_relatives_correct(import_id, citizen_id, data['relatives'])
        conn.commit()
        data = get_citizen_info(import_id, citizen_id)
    except:
        return Response(400)
    return Response(json.dumps({"data": data}), status=200)


@app.route('/imports/<import_id>/citizens/birthdays', methods=['GET'])
def birthday_stats(import_id):
    try:
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        sql = "select citizen_id, relative_id from relatives where import_id = ?"
        data = cursor.execute(sql, [import_id])
        data = data.fetchall()
        relatives = {}
        for el in data:
            if el[0] in relatives:
                relatives[el[0]].append(el[1])
            else:
                relatives[el[0]] = []
                relatives[el[0]].append(el[1])

        sql = "select citizen_id, birth_date from citizens where import_id = ?"
        births = cursor.execute(sql, [import_id])
        births = births.fetchall()
        month_by_id = {}
        for el in births:
            month_by_id[el[0]] = int(el[1].split('.')[1])

        result = {}
        for i in range(1, 13):
            result[i] = []
            for citizen in relatives:  # Прохожу по всем людям из отношений
                cnt_presents = 0
                for relative in relatives[citizen]:  # Прохожу по всем родственникам человека
                    if month_by_id[relative] == i:  # Проверяю текущий месяц
                        cnt_presents += 1
                if cnt_presents > 0:
                    result[i].append({
                        "citizen_id": citizen,
                        "presents": cnt_presents
                    })
    except:
        return Response(status=400)

    return Response(json.dumps({"data": result}).encode('utf-8'), status=200)


@app.route('/imports/<import_id>/towns/stat/percentile/age')
def town_age_stat(import_id):
    try:
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        sql = "select town, birth_date from citizens where import_id = ?"
        cursor.execute(sql, [import_id])
        data = cursor.fetchall()
        towns = {}
        for town_age in data:
            if town_age[0] in towns:
                day, month, year = list(map(int, town_age[1].split('.')))
                towns[town_age[0]].append(relativedelta(datetime.today(), datetime(year, month, day)).years)
            else:
                day, month, year = list(map(int, town_age[1].split('.')))
                towns[town_age[0]] = [relativedelta(datetime.today(), datetime(year, month, day)).years]

        percentiles = []

        for town in towns:
            percentile = {
                "town": town,
                "p50": np.percentile(towns[town], 50, interpolation='linear'),
                "p75": np.percentile(towns[town], 75, interpolation='linear'),
                "p99": np.percentile(towns[town], 99, interpolation='linear')

            }
            percentiles.append(percentile)
    except:
        return Response(400)

    return Response(json.dumps({"data": percentiles}), status=200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
