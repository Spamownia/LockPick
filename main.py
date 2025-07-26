     ==> Deploying...
==> Running 'python main.py'
Exception in thread Thread-1 (main_loop):
Traceback (most recent call last):
  File "/usr/local/lib/python3.13/threading.py", line 1043, in _bootstrap_inner
    self.run()
    ~~~~~~~~^^
  File "/usr/local/lib/python3.13/threading.py", line 994, in run
    self._target(*self._args, **self._kwargs)
    ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/render/project/src/main.py", line 188, in main_loop
    init_db()
    ~~~~~~~^^
  File "/opt/render/project/src/main.py", line 45, in init_db
    with get_conn() as conn:
         ~~~~~~~~^^
  File "/opt/render/project/src/main.py", line 36, in get_conn
    return psycopg2.connect(
           ~~~~~~~~~~~~~~~~^
        dbname=DB_NAME,
        ^^^^^^^^^^^^^^^
    ...<3 lines>...
        port=DB_PORT
        ^^^^^^^^^^^^
    )
    ^
  File "/opt/render/project/src/.venv/lib/python3.13/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory
	Is the server running locally and accepting connections on that socket?
[INFO] Inicjalizacja bazy danych...
 * Serving Flask app 'main'
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:3000
 * Running on http://10.201.208.183:3000
Press CTRL+C to quit
127.0.0.1 - - [26/Jul/2025 10:30:21] "HEAD / HTTP/1.1" 200 -
     ==> Your service is live 🎉
     ==> 
     ==> ///////////////////////////////////////////////////////////
     ==> 
     ==> Available at your primary URL https://lockpick.onrender.com
     ==> 
     ==> ///////////////////////////////////////////////////////////
127.0.0.1 - - [26/Jul/2025 10:30:31] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [26/Jul/2025 10:31:42] "HEAD / HTTP/1.1" 200 -
     ==> Detected service running on port 3000
     ==> Docs on specifying a port: https://render.com/docs/web-services#port-binding
127.0.0.1 - - [26/Jul/2025 10:36:44] "HEAD / HTTP/1.1" 200 -
