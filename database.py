import sqlite3
from sqlite3 import Error
from flask import g
import os

DATABASE = 'database.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_one(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchone()
    cur.close()

    res = list(rv.values())
    return (res[0] if res else None)

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def query_insert(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = get_db().commit()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def query_dict(query, args=(), one=False):
        con = get_db()
        con.row_factory = dict_factory
        cur = con.cursor()
        cur.execute(query,args)
        return cur.fetchall()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def init_db():
    """ """

    os.remove(DATABASE)
    conn = create_connection(DATABASE)

    try:
        c = conn.cursor()
        c.execute(""" CREATE TABLE condor_history (
            JobID int, Args varchar, ClusterName varchar, Job varchar, Cmd varchar,
            GlobalJobId varchar primary key, Qdate datetime, JobStartDate datetime,
            CompletionDate datetime, JobFinishedHookDone datetime, JobStatus int,
            Out varchar, Owner varchar, Process varchar, RequestCpus int,
            ServerTime datetime, UserLog varchar, RequiresWholeMachine varchar,
            LastRemoteHost varchar , ExecutionTime real, ClusterId int,
            ParentId varchar, Portal varchar, ProcessId varchar
        )""")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    init_db()
