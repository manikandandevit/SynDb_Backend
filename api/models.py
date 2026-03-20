"""
App-facing login accounts (separate from Django admin Users).
Store email + hashed password here; admin creates/edits these records.
"""
import secrets

from django.contrib.auth.hashers import check_password, make_password
from django.db import models


class AppLoginAccount(models.Model):
    """Credentials used only by your React app login — not Django staff Users."""

    email = models.EmailField(unique=True, db_index=True)
    password = models.CharField(max_length=256, help_text="Stored hashed (set via Password field in admin).")
    api_token = models.CharField(
        max_length=64,
        unique=True,
        null=True,
        blank=True,
        db_index=True,
        editable=False,
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        verbose_name = "App login account"
        verbose_name_plural = "App login accounts"

    def __str__(self):
        return self.email

    def set_password(self, raw_password: str) -> None:
        self.password = make_password(raw_password)

    def check_password(self, raw_password: str) -> bool:
        return check_password(raw_password, self.password)

    def ensure_api_token(self) -> None:
        if not self.api_token:
            self.api_token = secrets.token_hex(32)
            self.save(update_fields=["api_token"])

    def clear_api_token(self) -> None:
        self.api_token = None
        self.save(update_fields=["api_token"])
