import os
from typing import Any, Dict

import requests
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from .db_raw import raw_get_table, raw_list_tables
from .db_verify import verify_connection
from .models import AppLoginAccount


def _account_payload(account: AppLoginAccount):
    return {
        "id": account.pk,
        "email": account.email,
        "username": account.email,
    }


@api_view(["POST"])
@permission_classes([AllowAny])
def auth_login(request):
    """
    Login using **App login account** email + password (managed under API → App login accounts in admin).
    """
    body = request.data if isinstance(request.data, dict) else {}
    email = (body.get("email") or "").strip()
    password = body.get("password") or ""
    if not email or not password:
        return Response(
            {"error": "Email and password are required."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    account = AppLoginAccount.objects.filter(email__iexact=email, is_active=True).first()
    if account is None or not account.check_password(password):
        return Response(
            {"error": "Invalid email or password."},
            status=status.HTTP_401_UNAUTHORIZED,
        )

    account.ensure_api_token()
    return Response(
        {
            "token": account.api_token,
            "user": _account_payload(account),
        }
    )


@api_view(["POST"])
@permission_classes([AllowAny])
def auth_register(request):
    """Create an App login account (same as adding in admin)."""
    body = request.data if isinstance(request.data, dict) else {}
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    if not email or not password:
        return Response(
            {"error": "Email and password are required."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    if len(password) < 8:
        return Response(
            {"error": "Password must be at least 8 characters."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    if AppLoginAccount.objects.filter(email__iexact=email).exists():
        return Response(
            {"error": "An account with this email already exists."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    account = AppLoginAccount(email=email, is_active=True)
    account.set_password(password)
    account.save()
    account.ensure_api_token()
    return Response(
        {"token": account.api_token, "user": _account_payload(account)},
        status=status.HTTP_201_CREATED,
    )


@api_view(["POST"])
@permission_classes([AllowAny])
def auth_logout(request):
    """Invalidate app token (header: Authorization: Token <key>)."""
    auth = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
    if auth.lower().startswith("token "):
        key = auth[6:].strip()
    elif auth.lower().startswith("bearer "):
        key = auth[7:].strip()
    else:
        key = ""

    if not key:
        return Response({"detail": "Not logged in."}, status=status.HTTP_400_BAD_REQUEST)

    updated = AppLoginAccount.objects.filter(api_token=key).update(api_token=None)
    if updated:
        return Response({"detail": "Logged out."})
    return Response({"detail": "Not logged in."}, status=status.HTTP_400_BAD_REQUEST)


@api_view(["GET"])
@permission_classes([AllowAny])
def api_root(request):
    return Response(
        {
            "endpoints": {
                "health": "/api/health/",
                "auth_login": "/api/auth/login/",
                "auth_register": "/api/auth/register/",
                "auth_logout": "/api/auth/logout/",
                "credentials_verify": "/api/credentials/verify/",
                "raw_tables": "/api/raw/tables/",
                "raw_table": "/api/raw/table/",
                "chat": "/api/chat/",
            }
        }
    )


@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    return Response({"status": "ok"})


@api_view(["POST"])
@permission_classes([AllowAny])
def credentials_verify(request):
    payload = request.data if isinstance(request.data, dict) else {}
    db_type = (payload.get("type") or "").strip().lower()
    out = verify_connection(db_type, payload)
    return Response(out)


@api_view(["POST"])
@permission_classes([AllowAny])
def raw_tables(request):
    body = request.data if isinstance(request.data, dict) else {}
    conn = body.get("connection") if isinstance(body.get("connection"), dict) else body
    result = raw_list_tables(conn or {})
    return Response(result)


@api_view(["POST"])
@permission_classes([AllowAny])
def raw_table(request):
    body = request.data if isinstance(request.data, dict) else {}
    conn = body.get("connection") if isinstance(body.get("connection"), dict) else body
    table_name = body.get("table") or body.get("table_name") or ""
    result = raw_get_table(conn or {}, table_name)
    return Response(result)


def _openrouter_headers(api_key: str) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    # Optional: OpenRouter recommends these for better routing/analytics.
    site_url = os.environ.get("OPENROUTER_SITE_URL", "").strip()
    site_name = os.environ.get("OPENROUTER_SITE_NAME", "").strip()
    if site_url:
        headers["HTTP-Referer"] = site_url
    if site_name:
        headers["X-Title"] = site_name
    return headers


@api_view(["POST"])
@permission_classes([AllowAny])
def chat(request):
    """
    Minimal chat endpoint used by the frontend.
    If OPENROUTER_API_KEY is set, forwards to OpenRouter Chat Completions.
    """
    body: Dict[str, Any] = request.data if isinstance(request.data, dict) else {}
    question = (body.get("question") or "").strip()
    if not question:
        return Response({"error": "Question is required."}, status=400)

    api_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        return Response(
            {"error": "AI provider is not configured (missing OPENROUTER_API_KEY)."},
            status=501,
        )

    model = (
        (os.environ.get("OPENROUTER_Model_1") or "").strip()
        or (os.environ.get("OPENROUTER_MODEL_1") or "").strip()
        or "openai/gpt-4o-mini"
    )

    try:
        res = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=_openrouter_headers(api_key),
            json={
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a helpful assistant. Answer clearly and concisely.",
                    },
                    {"role": "user", "content": question},
                ],
                "temperature": 0.2,
            },
            timeout=20,
        )
        data = res.json() if res.content else {}
        if not res.ok:
            msg = (
                data.get("error", {}).get("message")
                if isinstance(data.get("error"), dict)
                else data.get("error") or data.get("message")
            )
            return Response({"error": msg or "AI request failed."}, status=502)

        choices = data.get("choices") or []
        content = ""
        if choices and isinstance(choices[0], dict):
            message = choices[0].get("message") or {}
            content = (message.get("content") or "").strip()
        return Response({"answer": content or ""})
    except Exception as e:
        return Response({"error": str(e)}, status=502)
