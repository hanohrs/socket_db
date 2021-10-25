# socket_db

* forward a TCP port over a SQLite3 database
* a typical use case is connecting two TCP networks that shares a file server
* not supporting TCP half-open
* not stable

## How to use

1. configure constants.py
2. run createdb.py
3. deploy files on the server and the client
   * DBNAME, which is the path to the database file, in constants.py may need to be changed
4. run serverside.py
5. run clientside.py
6. connect your TCP app to the LISTENING_ADDRESS on the client
