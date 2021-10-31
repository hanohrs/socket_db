#!/usr/bin/env python3

import socket
import sqlite3
import threading
import time
import traceback
from datetime import datetime

from constants import *

lock = threading.Lock()
data_from_db = {}  # : dict[int, tuple[bool, bytes]]
data_to_db = {}  # : dict[int, tuple[bool, bytes]]
next_chunk_ids = {}  # : dict[int, int]
clientside_conn_ids = set()  # : set[int]
dead_serverside_conn_ids = set()  # : set[int]


def sync_with_db():
    print(f"db synchronizing. to: {list(data_to_db.keys())}, dead_svr_conns: {list(dead_serverside_conn_ids)}")
    global data_from_db, next_chunk_ids, clientside_conn_ids
    with lock:
        with sqlite3.connect(DBNAME, DB_TIMEOUT) as con:
            try:
                # con.set_trace_callback(print)
                cur = con.cursor()
                new_data_from_db = data_from_db.copy()
                new_next_chunk_ids = next_chunk_ids.copy()
                keys_of_data = []
                # data from db
                for (conn_id, chunk_id, chunk_data) in cur.execute("""\
                    SELECT conn_id, chunk_id, chunk_data
                    FROM data
                    WHERE direction = ?
                    ORDER BY conn_id, chunk_id
                """, (DIRECTION_TO_SERVER,)):
                    down, data = new_data_from_db.get(conn_id, (False, b""))
                    down = down or not chunk_data
                    data = data + chunk_data
                    new_data_from_db[conn_id] = down, data
                    keys_of_data.append((conn_id, DIRECTION_TO_SERVER, chunk_id))
                cur.executemany("DELETE FROM data WHERE conn_id = ? AND direction = ? AND chunk_id = ?", keys_of_data)
                # data to db
                for (conn_id, (down, data)) in data_to_db.items():
                    next_chunk_id = new_next_chunk_ids.get(conn_id, SEQ_MIN_VALUE)
                    params = [(conn_id, DIRECTION_TO_CLIENT, next_chunk_id, data)]
                    if down and data:
                        params.append((conn_id, DIRECTION_TO_CLIENT, next_chunk_id + 1, b""))
                    cur.executemany("INSERT INTO data VALUES (?, ?, ?, ?)", params)
                    if conn_id not in dead_serverside_conn_ids:
                        new_next_chunk_ids[conn_id] = next_chunk_id + len(params)
                # clean up chunk id for dead connections
                for conn_id in dead_serverside_conn_ids:
                    if conn_id in new_next_chunk_ids:
                        del new_next_chunk_ids[conn_id]
                # conn_ids
                new_clientside_conn_ids = \
                    set(map(lambda t: t[0], cur.execute("SELECT conn_id FROM clientside_conn_ids")))
                db_dead_serverside_conn_ids = \
                    set(map(lambda t: t[0], cur.execute("SELECT conn_id FROM dead_serverside_conn_ids")))
                if dead_serverside_conn_ids != db_dead_serverside_conn_ids:
                    cur.execute("DELETE FROM dead_serverside_conn_ids")
                    cur.executemany(
                        "INSERT INTO dead_serverside_conn_ids VALUES (?)",
                        map(lambda i: (i,), dead_serverside_conn_ids | db_dead_serverside_conn_ids)
                    )
                # commit
                con.commit()
                # update cache
                data_from_db = new_data_from_db
                data_to_db.clear()
                next_chunk_ids = new_next_chunk_ids
                clientside_conn_ids = new_clientside_conn_ids
                dead_serverside_conn_ids.clear()
            except Exception as e:  # sqlite3.Error:
                print(f"Rolling back. {e}")
                print(traceback.format_exc())
                con.rollback()
            print(
                f"db synchronized. "
                f"from: {list(data_from_db.keys())}, "
                f"sent conns: {next_chunk_ids}, "
                f"clientside_conn_ids: {list(clientside_conn_ids)}"
            )


def handle_connection(conn_id):
    print(f"connection {conn_id} started")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(SERVER_ADDRESS)
            s.settimeout(SLEEP_TIME)
            sending = True
            send_data_remains = False
            receiving = True
            while sending or receiving:
                if sending:  # to the server
                    # print(f"connection {conn_id} writing")
                    with lock:
                        data_from_db_pair = data_from_db.get(conn_id)
                        if data_from_db_pair:
                            del data_from_db[conn_id]
                    if data_from_db_pair:
                        down, data = data_from_db_pair
                        if data:
                            sent = s.send(data[:BUFSIZE])
                            send_data_remains = sent != len(data)
                            if send_data_remains:
                                with lock:
                                    new_data_from_db_pair = data_from_db.get(conn_id)
                                    if new_data_from_db_pair:
                                        data_from_db[conn_id] = \
                                            new_data_from_db_pair[0], new_data_from_db_pair[1][sent:]
                                    else:
                                        data_from_db[conn_id] = down, data[sent:]
                                down = False
                        if down and not send_data_remains:
                            s.shutdown(socket.SHUT_WR)
                            sending = False
                            print(f"{datetime.now().isoformat()} connection {conn_id} write shutdown")
                if receiving:  # receive from the server
                    # print(f"connection {conn_id} reading")
                    try:
                        chunk = s.recv(BUFSIZE)
                    except socket.timeout:  # BlockingIOError:
                        pass
                    else:
                        with lock:
                            down, data = data_to_db.get(conn_id, (False, b""))
                            data_to_db[conn_id] = down or not chunk, data + chunk
                            if not chunk:
                                receiving = False
                                print(f"{datetime.now().isoformat()} connection {conn_id} read shutdown")
                with lock:
                    alive = conn_id in clientside_conn_ids
                # print(f"connection {conn_id} is alive: {alive}")
                if not alive:
                    break
                time.sleep(SLEEP_TIME)
    finally:
        with lock:
            if conn_id in next_chunk_ids:
                del next_chunk_ids[conn_id]
            if conn_id in data_from_db:
                del data_from_db[conn_id]
            if conn_id in data_to_db:
                del data_to_db[conn_id]
            if conn_id in clientside_conn_ids:
                dead_serverside_conn_ids.add(conn_id)
        print(f"{datetime.now().isoformat()} connection {conn_id} closed")


def main():
    server_max_conn_id = SEQ_MIN_VALUE - 1
    while True:
        client_max_conn_id = max(clientside_conn_ids, default=SEQ_MIN_VALUE - 1)
        for conn_id in range(server_max_conn_id + 1, client_max_conn_id + 1):
            t = threading.Thread(target=handle_connection, args=(conn_id,))
            t.start()
        server_max_conn_id = max(server_max_conn_id, client_max_conn_id)
        sync_with_db()
        time.sleep(DB_SLEEP_TIME)


main()
