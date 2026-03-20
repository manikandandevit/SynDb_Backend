"""
Raw DB introspection: list tables/collections and fetch table structure + data.
Uses same connection payload as db_verify. Row limit applied for safety.
"""
import logging
import re
from django.conf import settings

# Safe table/collection name: alphanumeric and underscore only (no SQL injection)
SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z0-9_]+$")
# MongoDB collection names can include . and -
SAFE_MONGO_COLLECTION = re.compile(r"^[a-zA-Z0-9_.-]+$")

from .db_verify import _parse_port, _build_mongodb_uri, _safe_close

logger = logging.getLogger(__name__)
TIMEOUT_SEC = getattr(settings, "DB_VERIFY_TIMEOUT", 5)
TIMEOUT_MS = getattr(settings, "DB_VERIFY_TIMEOUT_MS", 5000)
RAW_ROW_LIMIT = getattr(settings, "DB_RAW_ROW_LIMIT", 1000)


def _conn(payload):
    """Normalize connection payload: type, host, port, username, password, database, service, connectionUri."""
    t = (payload.get("type") or "").strip().lower()
    if t == "postgresql":
        t = "postgres"
    return {
        "type": t,
        "host": (payload.get("host") or "").strip() or "localhost",
        "port": payload.get("port"),
        "username": (payload.get("username") or payload.get("user") or "").strip(),
        "password": payload.get("password") or "",
        "database": (payload.get("database") or "").strip(),
        "service": (payload.get("service") or "").strip(),
        "connectionUri": (payload.get("connectionUri") or payload.get("connection_uri") or "").strip(),
    }


# --- MySQL ---
def _mysql_list_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def _mysql_columns_and_rows(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM `{}` LIMIT %s".format(table_name.replace("`", "``")), (RAW_ROW_LIMIT,))
        columns = [{"name": d[0], "type": str(d[1])} for d in cur.description]
        rows = [list(row) for row in cur.fetchall()]
        return columns, rows
    finally:
        cur.close()


def raw_mysql_list_tables(payload):
    conn = None
    try:
        try:
            import pymysql
        except ImportError:
            return {"error": "PyMySQL is not installed. Activate venv and run: pip install PyMySQL"}
        c = _conn(payload)
        port = _parse_port(c["port"], 3306)
        conn = pymysql.connect(
            host=c["host"],
            port=port,
            user=c["username"] or "",
            password=c["password"],
            database=c["database"] or None,
            connect_timeout=TIMEOUT_SEC,
            charset="utf8mb4",
        )
        tables = _mysql_list_tables(conn)
        return {"tables": tables}
    except Exception as e:
        logger.debug("MySQL list tables failed: %s", type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "MySQL")


def raw_mysql_table(payload, table_name):
    if not table_name or not isinstance(table_name, str):
        return {"error": "Table name required"}
    if not SAFE_TABLE_NAME.match(table_name.strip()):
        return {"error": "Invalid table name"}
    table_name = table_name.strip()
    conn = None
    try:
        try:
            import pymysql
        except ImportError:
            return {"error": "PyMySQL is not installed."}
        c = _conn(payload)
        port = _parse_port(c["port"], 3306)
        conn = pymysql.connect(
            host=c["host"],
            port=port,
            user=c["username"] or "",
            password=c["password"],
            database=c["database"] or None,
            connect_timeout=TIMEOUT_SEC,
            charset="utf8mb4",
        )
        columns, rows = _mysql_columns_and_rows(conn, table_name)
        return {"columns": columns, "rows": rows}
    except Exception as e:
        logger.debug("MySQL table %s failed: %s", table_name, type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "MySQL")


# --- PostgreSQL ---
def _pg_list_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def _pg_columns_and_rows(conn, table_name):
    from psycopg2 import sql
    cur = conn.cursor()
    try:
        cur.execute(
            sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(table_name)),
            (RAW_ROW_LIMIT,),
        )
        columns = [{"name": d[0], "type": str(d[1]) if d[1] else "unknown"} for d in cur.description]
        rows = [list(row) for row in cur.fetchall()]
        return columns, rows
    finally:
        cur.close()


def raw_postgres_list_tables(payload):
    conn = None
    try:
        import psycopg2
        c = _conn(payload)
        port = _parse_port(c["port"], 5432)
        conn = psycopg2.connect(
            host=c["host"],
            port=port,
            user=c["username"] or "",
            password=c["password"],
            dbname=c["database"] or "",
            connect_timeout=TIMEOUT_SEC,
        )
        tables = _pg_list_tables(conn)
        return {"tables": tables}
    except Exception as e:
        logger.debug("PostgreSQL list tables failed: %s", type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "PostgreSQL")


def raw_postgres_table(payload, table_name):
    if not table_name or not isinstance(table_name, str):
        return {"error": "Table name required"}
    if not SAFE_TABLE_NAME.match(table_name.strip()):
        return {"error": "Invalid table name"}
    table_name = table_name.strip()
    conn = None
    try:
        import psycopg2
        c = _conn(payload)
        port = _parse_port(c["port"], 5432)
        conn = psycopg2.connect(
            host=c["host"],
            port=port,
            user=c["username"] or "",
            password=c["password"],
            dbname=c["database"] or "",
            connect_timeout=TIMEOUT_SEC,
        )
        columns, rows = _pg_columns_and_rows(conn, table_name)
        return {"columns": columns, "rows": rows}
    except Exception as e:
        logger.debug("PostgreSQL table %s failed: %s", table_name, type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "PostgreSQL")


# --- MongoDB ---
def _mongodb_db_from_uri_path(uri):
    """Read DB name from URI path without SRV DNS (e.g. ...net/payroll?opts -> payroll)."""
    if not uri or not isinstance(uri, str):
        return ""
    u = uri.strip().split("?")[0].split("#")[0]
    idx = u.find("://")
    if idx < 0:
        return ""
    rest = u[idx + 3 :]
    if "@" in rest:
        rest = rest.split("@", 1)[1]
    slash = rest.find("/")
    if slash < 0 or slash >= len(rest) - 1:
        return ""
    db = rest[slash + 1 :].strip("/")
    if not db:
        return ""
    return db.split("/")[0].strip()


def _mongodb_resolve_database(payload, client):
    """
    Database from: 1) payload.database 2) URI path (/dbname) 3) sole user DB
    4) multiple user DBs -> return ("choose", [names]).
    """
    explicit = (payload.get("database") or "").strip()
    if explicit:
        return explicit
    uri = _build_mongodb_uri(payload)
    if not uri:
        return None
    db_from_uri = _mongodb_db_from_uri_path(uri)
    if db_from_uri:
        return db_from_uri
    try:
        system = {"admin", "local", "config"}
        names = sorted(d for d in client.list_database_names() if d not in system)
        if len(names) == 1:
            return names[0]
        if len(names) > 1:
            return ("choose", names)
    except Exception as e:
        logger.debug("MongoDB list_database_names: %s", e)
    return None


def raw_mongodb_list_collections(payload):
    client = None
    try:
        from pymongo import MongoClient

        uri = _build_mongodb_uri(payload)
        if not uri:
            return {"error": "Provide connectionUri or host + username + password (+ database)."}
        client = MongoClient(uri, serverSelectionTimeoutMS=TIMEOUT_MS)
        resolved = _mongodb_resolve_database(payload, client)
        if resolved is None:
            return {
                "error": "Could not determine database. Add a Database name in Connection, or pick one below.",
                "needs_database": True,
                "databases": [],
            }
        if isinstance(resolved, tuple) and resolved[0] == "choose":
            return {
                "needs_database": True,
                "databases": resolved[1],
                "tables": [],
            }
        db_name = resolved
        db = client[db_name]
        names = list(db.list_collection_names())
        return {"tables": names, "database_used": db_name}
    except Exception as e:
        logger.debug("MongoDB list collections failed: %s", type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        try:
            if client is not None:
                client.close()
        except Exception:
            pass


def _mongo_field_overview(docs):
    """Top-level keys with inferred BSON-ish type labels from raw documents."""
    keys = set()
    for d in docs:
        if isinstance(d, dict):
            keys.update(d.keys())
    columns = []
    for k in sorted(keys):
        typ = "mixed"
        for d in docs:
            if not isinstance(d, dict) or k not in d:
                continue
            v = d[k]
            if v is None:
                continue
            if hasattr(v, "__class__") and v.__class__.__module__ == "bson.objectid":
                typ = "ObjectId"
            elif hasattr(v, "__class__") and "datetime" in v.__class__.__name__.lower():
                typ = "DateTime"
            elif isinstance(v, dict):
                typ = "Document"
            elif isinstance(v, list):
                typ = "Array"
            else:
                typ = type(v).__name__
            break
        columns.append({"name": k, "type": typ})
    return columns


def raw_mongodb_collection(payload, collection_name):
    if not collection_name or not isinstance(collection_name, str):
        return {"error": "Collection name required"}
    if not SAFE_MONGO_COLLECTION.match(collection_name.strip()):
        return {"error": "Invalid collection name"}
    collection_name = collection_name.strip()
    client = None
    try:
        import json
        from bson import json_util
        from pymongo import MongoClient

        uri = _build_mongodb_uri(payload)
        if not uri:
            return {"error": "Provide connectionUri or host + username + password."}
        client = MongoClient(uri, serverSelectionTimeoutMS=TIMEOUT_MS)
        resolved = _mongodb_resolve_database(payload, client)
        if resolved is None or (isinstance(resolved, tuple) and resolved[0] == "choose"):
            return {"error": "Select a MongoDB database first (View Raw), or add database to your connection."}
        db_name = resolved
        coll = client[db_name][collection_name]
        docs = list(coll.find().limit(RAW_ROW_LIMIT))
        if not docs:
            return {
                "format": "mongodb",
                "documents": [],
                "columns": [],
                "rows": [],
            }
        columns = _mongo_field_overview(docs)
        # Full nested JSON (ObjectId, Date, etc. as extended JSON)
        documents = json.loads(json_util.dumps(docs))
        column_names = [c["name"] for c in columns]
        rows = []
        for d in documents:
            if not isinstance(d, dict):
                rows.append([None] * len(column_names))
                continue
            row = []
            for k in column_names:
                v = d.get(k)
                if v is None:
                    row.append(None)
                elif isinstance(v, (dict, list)):
                    row.append(json.dumps(v, ensure_ascii=False))
                else:
                    row.append(v)
            rows.append(row)
        return {
            "format": "mongodb",
            "documents": documents,
            "columns": columns,
            "rows": rows,
        }
    except Exception as e:
        logger.debug("MongoDB collection %s failed: %s", collection_name, type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        try:
            if client is not None:
                client.close()
        except Exception:
            pass


# --- Oracle ---
def _oracle_list_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name FROM user_tables ORDER BY table_name
        """)
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def _oracle_columns_and_rows(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM "{}" WHERE ROWNUM <= :1'.format(table_name.replace('"', '""')), [RAW_ROW_LIMIT])
        columns = [{"name": d[0], "type": str(d[1]) if d[1] else "unknown"} for d in cur.description]
        rows = [list(row) for row in cur.fetchall()]
        return columns, rows
    finally:
        cur.close()


def raw_oracle_list_tables(payload):
    conn = None
    try:
        import oracledb
        c = _conn(payload)
        port = _parse_port(c["port"], 1521)
        dsn = oracledb.makedsn(c["host"], port, service_name=c["service"] or "ORCL")
        conn = oracledb.connect(
            user=c["username"] or "",
            password=c["password"],
            dsn=dsn,
        )
        tables = _oracle_list_tables(conn)
        return {"tables": tables}
    except Exception as e:
        logger.debug("Oracle list tables failed: %s", type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "Oracle")


def raw_oracle_table(payload, table_name):
    if not table_name or not isinstance(table_name, str):
        return {"error": "Table name required"}
    if not SAFE_TABLE_NAME.match(table_name.strip()):
        return {"error": "Invalid table name"}
    table_name = table_name.strip()
    conn = None
    try:
        import oracledb
        c = _conn(payload)
        port = _parse_port(c["port"], 1521)
        dsn = oracledb.makedsn(c["host"], port, service_name=c["service"] or "ORCL")
        conn = oracledb.connect(
            user=c["username"] or "",
            password=c["password"],
            dsn=dsn,
        )
        columns, rows = _oracle_columns_and_rows(conn, table_name)
        return {"columns": columns, "rows": rows}
    except Exception as e:
        logger.debug("Oracle table %s failed: %s", table_name, type(e).__name__)
        return {"error": str(e).strip()}
    finally:
        _safe_close(conn, "Oracle")


# --- Dispatcher ---
def raw_list_tables(payload):
    t = (payload.get("type") or "").strip().lower()
    if t == "postgresql":
        t = "postgres"
    if t == "mysql":
        return raw_mysql_list_tables(payload)
    if t == "postgres":
        return raw_postgres_list_tables(payload)
    if t == "mongodb":
        return raw_mongodb_list_collections(payload)
    if t == "oracle":
        return raw_oracle_list_tables(payload)
    return {"error": "Unsupported type. Use mysql, postgres, mongodb, or oracle."}


def raw_get_table(payload, table_name):
    t = (payload.get("type") or "").strip().lower()
    if t == "postgresql":
        t = "postgres"
    if t == "mysql":
        return raw_mysql_table(payload, table_name)
    if t == "postgres":
        return raw_postgres_table(payload, table_name)
    if t == "mongodb":
        return raw_mongodb_collection(payload, table_name)
    if t == "oracle":
        return raw_oracle_table(payload, table_name)
    return {"error": "Unsupported type."}
