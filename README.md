# Backend — Python Django + PostgreSQL

Django REST API with PostgreSQL.

## Setup

1. **Virtual environment (recommended)**

   ```bash
   cd backEnd
   python -m venv venv
   venv\Scripts\activate   # Windows
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **PostgreSQL**

   - Install PostgreSQL and create a database, e.g. `project_db`.
   - Copy `.env.example` to `.env` and set:

   ```env
   DB_NAME=project_db
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

4. **Migrations & run**

   ```bash
   python manage.py migrate
   python manage.py runserver
   ```

   API base: **http://127.0.0.1:8000/api/**

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/` | GET | API root (list of endpoints) |
| `/api/health/` | GET | Health check |
| `/api/credentials/verify/` | POST | Verify DB from Confidentials Datas (body: `{ "data": "connection_string" }`) |
| `/api/chat-messages/` | GET, POST | List/create chat messages |

## Env only (do not hardcode)

- Set all config in `.env` from `.env.example`. DB verify timeouts and optional MySQL defaults: `DB_VERIFY_TIMEOUT`, `DB_VERIFY_TIMEOUT_MS`, `DB_VERIFY_MYSQL_HOST`, `DB_VERIFY_MYSQL_PORT`, `DB_VERIFY_MYSQL_USER`.

## Admin

- Create superuser: `python manage.py createsuperuser`
- Admin: http://127.0.0.1:8000/admin/

## Deploy (Render) — admin CSS/JS

Production needs collected static files + WhiteNoise (already in `requirements.txt`).

**Build command** on Render:

```bash
pip install -r requirements.txt && python manage.py collectstatic --noinput
```

**Start command** (example):

```bash
python manage.py migrate && gunicorn config.wsgi:application --bind 0.0.0.0:$PORT
```
