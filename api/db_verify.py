"""
Production-grade DB connection verification.
- Uses timeouts from Django settings (env).
- Never logs credentials; returns only connected: bool and optional message.
- Closes connections immediately after check.
- MongoDB: credentials are URL-encoded to fix "bad auth" when password has special chars.
"""
import logging
from urllib.parse import quote_plus

from django.conf import settings

logger = logging.getLogger(__name__)
TIMEOUT_SEC = getattr(settings, "DB_VERIFY_TIMEOUT", 5)
TIMEOUT_MS = getattr(settings, "DB_VERIFY_TIMEOUT_MS", 5000)


def _safe_close(conn, label="connection"):
    try:
        if conn is not None:
            if hasattr(conn, "close"):
                conn.close()
    except Exception as e:
        logger.warning("Error closing %s: %s", label, str(e))


def _parse_port(port, default):
    """Parse port from JSON (str or int) to int."""
    if port is None:
        return default
    if isinstance(port, int) and 0 < port < 65536:
        return port
    try:
        p = int(port) if isinstance(port, str) and port.strip() else default
        return p if 0 < p < 65536 else default
    except (ValueError, TypeError):
        return default


def check_mysql(host, port, user, password, database):
    """Verify MySQL connection. Returns (connected: bool, message: str, error_detail: str|None)."""
    conn = None
    try:
        try:
            import pymysql
        except ImportError:
            return (
                False,
                "Not Connected",
                "PyMySQL is not installed in this Python environment. "
                "Activate the project venv (e.g. .\\venv\\Scripts\\Activate.ps1) then run: pip install PyMySQL",
            )
        host = (host or "").strip() or "localhost"
        port_num = _parse_port(port, 3306)
        database = (database or "").strip() or None  # None = no database selected (valid for connect)
        conn = pymysql.connect(
            host=host,
            port=port_num,
            user=(user or "").strip() or "",
            password=password or "",
            database=database,
            connect_timeout=TIMEOUT_SEC,
            charset="utf8mb4",
        )
        conn.ping(reconnect=False)
        return True, "Connected (Real DB)", None
    except Exception as e:
        logger.debug("MySQL verify failed: %s", type(e).__name__)
        err_msg = str(e).strip()
        if password and err_msg.find(password) != -1:
            err_msg = err_msg.replace(password, "***")
        return False, "Not Connected", err_msg
    finally:
        _safe_close(conn, "MySQL")


def check_postgres(host, port, user, password, database):
    """Verify PostgreSQL connection. Returns (connected: bool, message: str, error_detail: str|None)."""
    conn = None
    try:
        import psycopg2
        port_num = _parse_port(port, 5432)
        conn = psycopg2.connect(
            host=(host or "").strip() or "localhost",
            port=port_num,
            user=user or "",
            password=password or "",
            dbname=(database or "").strip() or "",
            connect_timeout=TIMEOUT_SEC,
        )
        conn.close()
        conn = None
        return True, "Connected (Real DB)", None
    except Exception as e:
        logger.debug("PostgreSQL verify failed: %s", type(e).__name__)
        err_msg = str(e).strip()
        if password and err_msg.find(password) != -1:
            err_msg = err_msg.replace(password, "***")
        return False, "Not Connected", err_msg
    finally:
        _safe_close(conn, "PostgreSQL")


def _build_mongodb_uri(payload):
    """
    Build or normalize MongoDB URI with URL-encoded credentials (fixes Atlas "bad auth" when
    password contains special characters like @, :, /, #).
    Accepts either connectionUri (will re-encode user/pass) or host, port, username, password, database.
    """
    raw_uri = (payload.get("connectionUri") or payload.get("connection_uri") or payload.get("uri") or "").strip()
    host = (payload.get("host") or "").strip()
    username = (payload.get("username") or payload.get("user") or "").strip()
    password = payload.get("password") or ""
    database = (payload.get("database") or "").strip()
    port = payload.get("port")

    # Prefer building from parts so password is always correctly encoded
    if host and username:
        port_num = _parse_port(port, 27017)
        user_enc = quote_plus(username) if username else ""
        pass_enc = quote_plus(password) if password else ""
        auth = f"{user_enc}:{pass_enc}@" if (user_enc or pass_enc) else ""
        # Atlas / cloud: host like cluster0.xxxx.mongodb.net -> use mongodb+srv (no port in URI)
        if "mongodb.net" in host or "mongodb.com" in host:
            scheme = "mongodb+srv"
            netloc = f"{auth}{host}" if auth else host
            path = f"/{database}" if database else ""
            return f"{scheme}://{netloc}{path}"
        scheme = "mongodb"
        netloc = f"{auth}{host}:{port_num}" if auth else f"{host}:{port_num}"
        path = f"/{database}" if database else ""
        return f"{scheme}://{netloc}{path}"

    if not raw_uri:
        return None

    # Sanitize: remove stray newlines/spaces from copy-paste (can cause "bad auth")
    uri = raw_uri.replace("\r", "").replace("\n", "").strip()

    # Atlas often requires authSource=admin for DB user auth; add if missing
    if "authSource=" not in uri and "mongodb" in uri:
        sep = "&" if "?" in uri else "?"
        uri = f"{uri}{sep}authSource=admin"

    return uri


def check_mongodb(uri):
    """Verify MongoDB connection via URI. Returns (connected: bool, message: str, error_detail: str|None)."""
    client = None
    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=TIMEOUT_MS)
        client.server_info()
        return True, "Connected (Real DB)", None
    except Exception as e:
        logger.debug("MongoDB verify failed: %s", type(e).__name__)
        return False, "Not Connected", str(e).strip()
    finally:
        try:
            if client is not None:
                client.close()
        except Exception as e:
            logger.warning("Error closing MongoDB client: %s", str(e))


def check_oracle(host, port, user, password, service):
    """Verify Oracle connection. Returns (connected: bool, message: str, error_detail: str|None)."""
    conn = None
    try:
        import oracledb
        port_num = _parse_port(port, 1521)
        dsn = oracledb.makedsn((host or "").strip() or "localhost", port_num, service_name=(service or "").strip() or "ORCL")
        conn = oracledb.connect(user=user or "", password=password or "", dsn=dsn)
        return True, "Connected (Real DB)", None
    except Exception as e:
        logger.debug("Oracle verify failed: %s", type(e).__name__)
        return False, "Not Connected", str(e).strip()
    finally:
        _safe_close(conn, "Oracle")


def verify_connection(db_type, payload):
    """
    Dispatch to the right checker. payload is a dict with keys per DB type.
    Returns dict: { "connected": bool, "message": str, "db_type": str }.
    """
    db_type = (payload.get("type") or db_type or "").strip().lower()
    if not db_type:
        return {"connected": False, "message": "Not Connected", "db_type": "unknown"}

    if db_type == "mysql":
        connected, message, error_detail = check_mysql(
            payload.get("host") or "",
            payload.get("port"),
            payload.get("username") or payload.get("user") or "",
            payload.get("password") or "",
            payload.get("database") or "",
        )
    elif db_type == "postgres":
        connected, message, error_detail = check_postgres(
            payload.get("host") or "",
            payload.get("port"),
            payload.get("username") or payload.get("user") or "",
            payload.get("password") or "",
            payload.get("database") or "",
        )
    elif db_type == "mongodb":
        uri = _build_mongodb_uri(payload)
        if not uri:
            return {"connected": False, "message": "Not Connected", "db_type": "mongodb", "error_detail": "Provide connection URI or host + username + password (+ database)."}
        connected, message, error_detail = check_mongodb(uri)
    elif db_type == "oracle":
        connected, message, error_detail = check_oracle(
            payload.get("host") or "",
            payload.get("port"),
            payload.get("username") or payload.get("user") or "",
            payload.get("password") or "",
            payload.get("service") or "",
        )
    else:
        return {"connected": False, "message": "Not Connected", "db_type": db_type}

    out = {"connected": connected, "message": message, "db_type": db_type}
    if error_detail:
        out["error_detail"] = error_detail
    return out
