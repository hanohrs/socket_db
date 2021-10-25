#!/usr/bin/env python3

import socket
import sqlite3
import threading
import time
import traceback

from constants import *

lock = threading.Lock()
min_noninserted_conn_id = SEQ_MIN_VALUE
next_conn_id = SEQ_MIN_VALUE
data_from_db = {}  # : dict[int, tuple[bool, bytes]]
data_to_db = {}  # : dict[int, tuple[bool, bytes]]
next_chunk_ids = {}  # : dict[int, int]


def sync_with_db():
    global data_from_db, min_noninserted_conn_id, next_chunk_ids
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
                db_conn_ids = set(map(lambda t: t[0], cur.execute("SELECT conn_id FROM conn_ids")))
                for conn_id in list(new_next_chunk_ids):
                    if conn_id < min_noninserted_conn_id and conn_id not in db_conn_ids:
                        del new_next_chunk_ids[conn_id]
                cur.execute("DELETE FROM conn_ids")
                cur.executemany(
                    "INSERT INTO conn_ids VALUES (?)",
                    map(lambda conn_id: (conn_id,), new_next_chunk_ids.keys())
                )
                # commit
                con.commit()
                # update cache
                min_noninserted_conn_id = next_conn_id
                data_from_db = new_data_from_db
                data_to_db.clear()
                next_chunk_ids = new_next_chunk_ids
            except Exception as e:  # sqlite3.Error:
                print(f"Rolling back. {e}")
                print(traceback.format_exc())
                con.rollback()
            print(f"db synchronized. from: {list(data_from_db.keys())}, to: {list(data_to_db.keys())}, conns: {next_chunk_ids}")
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
            conn_id = get_conn_id()
            print(f"connection {conn_id} started")
            sending = True
            receiving = True
            while sending or receiving:
                if sending:  # to the client
                    lock.acquire()
                    data_from_db_pair = data_from_db.get(conn_id)
                    if data_from_db_pair:
                        del data_from_db[conn_id]
                    lock.release()
                    if data_from_db_pair:
                        down, data = data_from_db_pair
                        if data:
                            s.sendall(data)
                        if down:
                            sending = False
                if receiving:  # from the client
                    s.setblocking(False)
                    try:
                        chunk = s.recv(BUFSIZE)
                    except BlockingIOError:
                        continue
                    else:
                        lock.acquire()
                        down, data = data_to_db.get(conn_id, (False, b""))
                        data_to_db[conn_id] = down or not chunk, data + chunk
                        if not chunk:
                            receiving = False
                        lock.release()
                    finally:
                        s.setblocking(True)
                lock.acquire()
                alive = conn_id in next_chunk_ids.keys()
                lock.release()
                if not alive:
                    break
                time.sleep(SLEEP_TIME)
    finally:
        lock.acquire()
        if conn_id in next_chunk_ids:
            del next_chunk_ids[conn_id]
        if conn_id in data_from_db:
            del data_from_db[conn_id]
        lock.release()
        print(f"connection {conn_id} closed")


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(LISTENING_ADDRESS)
server_socket.listen()
threading.Thread(target=handle_db).start()
while True:
    (client_socket, address) = server_socket.accept()
    threading.Thread(target=handle_client_socket, args=(client_socket,)).start()
