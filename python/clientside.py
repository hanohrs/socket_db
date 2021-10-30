#!/usr/bin/env python3

import socket
import sqlite3
import threading
import time
import traceback
from datetime import datetime

from constants import *

lock = threading.Lock()
min_noninserted_conn_id = SEQ_MIN_VALUE
next_conn_id = SEQ_MIN_VALUE
data_from_db = {}  # : dict[int, tuple[bool, bytes]]
data_to_db = {}  # : dict[int, tuple[bool, bytes]]
next_chunk_ids = {}  # : dict[int, int]
dead_serverside_conn_ids = set()  # : set[int]


def sync_with_db():
    print(f"db synchronizing. to: {list(data_to_db.keys())}")
    global data_from_db, min_noninserted_conn_id, next_chunk_ids, dead_serverside_conn_ids
    lock.acquire()
    try:
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
                """, (DIRECTION_TO_CLIENT,)):
                    down, data = new_data_from_db.get(conn_id, (False, b""))
                    down = down or not chunk_data
                    data = data + chunk_data
                    if conn_id in next_chunk_ids:
                        new_data_from_db[conn_id] = down, data
                    keys_of_data.append((conn_id, DIRECTION_TO_CLIENT, chunk_id))
                cur.executemany("DELETE FROM data WHERE conn_id = ? AND direction = ? AND chunk_id = ?", keys_of_data)
                # data to db
                for (conn_id, (down, data)) in data_to_db.items():
                    next_chunk_id = new_next_chunk_ids.get(conn_id, SEQ_MIN_VALUE)
                    params = [(conn_id, DIRECTION_TO_SERVER, next_chunk_id, data)]
                    if down and data:
                        params.append((conn_id, DIRECTION_TO_SERVER, next_chunk_id + 1, b""))
                    cur.executemany("INSERT INTO data VALUES (?, ?, ?, ?)", params)
                    new_next_chunk_ids[conn_id] = next_chunk_id + len(params)
                # conn_ids
                db_dead_serverside_conn_ids = \
                    set(map(lambda t: t[0], cur.execute("SELECT conn_id FROM dead_serverside_conn_ids")))
                new_dead_serverside_conn_ids = \
                    next_chunk_ids.keys() & \
                    (dead_serverside_conn_ids | (db_dead_serverside_conn_ids - dead_serverside_conn_ids))
                if new_dead_serverside_conn_ids != db_dead_serverside_conn_ids:
                    cur.execute("DELETE FROM dead_serverside_conn_ids")
                    cur.executemany(
                        "INSERT INTO dead_serverside_conn_ids VALUES (?)",
                        map(lambda i: (i,), new_dead_serverside_conn_ids)
                    )
                db_clientside_conn_ids = \
                    set(map(lambda t: t[0], cur.execute("SELECT conn_id FROM clientside_conn_ids")))
                if next_chunk_ids.keys() != db_clientside_conn_ids:
                    cur.execute("DELETE FROM clientside_conn_ids")
                    cur.executemany(
                        "INSERT INTO clientside_conn_ids VALUES (?)",
                        map(lambda i: (i,), next_chunk_ids.keys())
                    )
                # commit
                con.commit()
                # update cache
                min_noninserted_conn_id = next_conn_id
                data_from_db = new_data_from_db
                data_to_db.clear()
                next_chunk_ids = new_next_chunk_ids
                dead_serverside_conn_ids = new_dead_serverside_conn_ids
            except Exception as e:  # sqlite3.Error:
                print(f"Rolling back. {e}")
                print(traceback.format_exc())
                con.rollback()
            print(
                f"db synchronized. "
                f"from: {list(data_from_db.keys())}, "
                f"conns: {next_chunk_ids}, "
                f"dead_svr_conns: {list(dead_serverside_conn_ids)}"
            )
    finally:
        lock.release()


def handle_db():
    while True:
        sync_with_db()
        time.sleep(DB_SLEEP_TIME)


def get_conn_id():
    global next_conn_id
    lock.acquire()
    try:
        conn_id = next_conn_id
        next_chunk_ids[conn_id] = SEQ_MIN_VALUE
        next_conn_id = next_conn_id + 1
        return conn_id
    finally:
        lock.release()


def handle_client_socket(client_socket: socket.socket):
    try:
        with client_socket as s:
            s.settimeout(SLEEP_TIME)
            conn_id = get_conn_id()
            print(f"connection {conn_id} started")
            sending = True
            send_data_remains = False
            receiving = True
            while sending or receiving:
                if sending:  # to the client
                    # print(f"connection {conn_id} writing")
                    lock.acquire()
                    data_from_db_pair = data_from_db.get(conn_id)
                    if data_from_db_pair:
                        del data_from_db[conn_id]
                    lock.release()
                    if data_from_db_pair:
                        down, data = data_from_db_pair
                        if data:
                            sent = s.send(data[:BUFSIZE])
                            send_data_remains = sent != len(data)
                            if send_data_remains:
                                lock.acquire()
                                new_data_from_db_pair = data_from_db.get(conn_id)
                                if new_data_from_db_pair:
                                    data_from_db[conn_id] = new_data_from_db_pair[0], new_data_from_db_pair[1][sent:]
                                else:
                                    data_from_db[conn_id] = down, data[sent:]
                                lock.release()
                        if down and not send_data_remains:
                            s.shutdown(socket.SHUT_WR)
                            sending = False
                            print(f"{datetime.now().isoformat()} connection {conn_id} write shutdown")
                if receiving:  # from the client
                    # print(f"connection {conn_id} reading")
                    try:
                        chunk = s.recv(BUFSIZE)
                    except socket.timeout:  # BlockingIOError:
                        pass
                    else:
                        lock.acquire()
                        down, data = data_to_db.get(conn_id, (False, b""))
                        data_to_db[conn_id] = down or not chunk, data + chunk
                        if not chunk:
                            receiving = False
                            print(f"{datetime.now().isoformat()} connection {conn_id} read shutdown")
                        lock.release()
                lock.acquire()
                alive = conn_id not in dead_serverside_conn_ids
                lock.release()
                # print(f"connection {conn_id} is alive: {alive}")
                if not send_data_remains and not alive:
                    print(f"{datetime.now().isoformat()} connection {conn_id} is dead on the server side")
                    break
                if not receiving:
                    time.sleep(SLEEP_TIME)
    finally:
        lock.acquire()
        if conn_id in next_chunk_ids:
            del next_chunk_ids[conn_id]
        if conn_id in data_from_db:
            del data_from_db[conn_id]
        if conn_id in data_to_db:
            del data_to_db[conn_id]
        lock.release()
        print(f"{datetime.now().isoformat()} connection {conn_id} closed: {next_chunk_ids}")


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(LISTENING_ADDRESS)
server_socket.listen()
threading.Thread(target=handle_db).start()
while True:
    (client_socket, address) = server_socket.accept()
    threading.Thread(target=handle_client_socket, args=(client_socket,)).start()
