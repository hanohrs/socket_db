#!/usr/bin/env python3

import sqlite3

from constants import DBNAME

con = sqlite3.connect(DBNAME)
cur = con.cursor()
cur.execute("PRAGMA journal_mode=WAL")
cur.execute("DROP TABLE IF EXISTS data")
cur.execute("DROP TABLE IF EXISTS dead_serverside_conn_ids")
cur.execute("DROP TABLE IF EXISTS clientside_conn_ids")
cur.execute("DROP TABLE IF EXISTS directions")
cur.execute("""\
    CREATE TABLE directions (
        direction_id int PRIMARY KEY,
        name text
    )
""")
cur.execute("""\
    CREATE TABLE clientside_conn_ids (
        conn_id int PRIMARY KEY
    )
""")
cur.execute("""\
    CREATE TABLE dead_serverside_conn_ids (
        conn_id int PRIMARY KEY
    )
""")
cur.execute("""\
    CREATE TABLE data (
        conn_id int,
        direction int REFERENCES directions,
        chunk_id int,
        chunk_data blob,
        PRIMARY KEY (conn_id, direction, chunk_id)
    )
""")
cur.execute("""\
    INSERT INTO directions
        VALUES
            (0, 'to server'),
            (1, 'to client')
""")
con.commit()
con.close()
