import oracledb
pool = None

def init_pool():
    global pool
    pool = oracledb.create_pool(
        user = "PRODN",
        password = "Prodn0123#",
        dsn = "localhost:1521/FREEPDB1",
        min = 1,
        max = 5,
        increment = 1,
    )

def get_connection():
    return pool.acquire()

def release_connection(conn):
    pool.release(conn)