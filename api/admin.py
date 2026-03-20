"""
Admin: App login accounts (email/password for your app) + Token helper text.
"""
from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError
from django.utils.html import format_html
from rest_framework.authtoken.models import Token

from .models import AppLoginAccount

try:
    admin.site.unregister(Token)
except admin.sites.NotRegistered:
    pass


@admin.register(AppLoginAccount)
class AppLoginAccountAdmin(admin.ModelAdmin):
    """Create app users here — same email/password used on the landing login form."""

    class AppLoginAccountForm(forms.ModelForm):
        raw_password = forms.CharField(
            widget=forms.PasswordInput(render_value=False),
            required=False,
            label="Password (plain text)",
        )

        class Meta:
            model = AppLoginAccount
            fields = ("email", "is_active")

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if self.instance.pk:
                self.fields["raw_password"].help_text = "Leave blank to keep the current password."
            else:
                self.fields["raw_password"].required = True
                self.fields["raw_password"].help_text = "What the user types in your app (stored hashed)."

        def clean(self):
            cleaned = super().clean()
            if not self.instance.pk and not cleaned.get("raw_password"):
                raise ValidationError({"raw_password": "Password is required for a new account."})
            return cleaned

        def save(self, commit=True):
            instance = super().save(commit=False)
            raw = self.cleaned_data.get("raw_password")
            if raw:
                instance.set_password(raw)
            if commit:
                instance.save()
            return instance

    form = AppLoginAccountForm
    list_display = ("email", "is_active", "created_at", "token_preview")
    list_filter = ("is_active",)
    search_fields = ("email",)
    readonly_fields = ("password", "api_token", "created_at", "updated_at", "_hint")

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "_hint",
                    "email",
                    "raw_password",
                    "is_active",
                )
            },
        ),
        (
            "Stored values (read-only)",
            {"fields": ("password", "api_token", "created_at", "updated_at")},
        ),
    )

    @admin.display(description="Token")
    def token_preview(self, obj):
        if obj.api_token:
            return f"{obj.api_token[:12]}…"
        return "— (set on first login)"

    @admin.display(description="")
    def _hint(self, obj):
        return format_html(
            "<div style='max-width:720px;padding:12px 14px;background:#d4edda;border:1px solid #c3e6cb;"
            "border-radius:8px;line-height:1.5;color:#155724'>"
            "<strong>App login</strong> — இங்கே save பண்ணும் <strong>email</strong> மற்றும் "
            "<strong>password</strong> தான் உங்கள் web app landing page login-ல் use ஆகும். "
            "Django <strong>Users</strong> (staff/admin) இதிலிருந்து தனி."
            "</div>"
        )


@admin.register(Token)
class TokenAdmin(admin.ModelAdmin):
    """DRF tokens tied to Django Users (staff). App users use App login accounts instead."""

    list_display = ("key", "user", "created")
    ordering = ("-created",)
    fields = ("_help", "user")
    readonly_fields = ("_help",)

    @admin.display(description="")
    def _help(self, obj):
        return format_html(
            "<div style='max-width:720px;padding:14px 16px;background:#e7f3ff;border:1px solid #b6daff;"
            "border-radius:10px;margin-bottom:12px;line-height:1.55;color:#212529'>"
            "<strong>For your React app login</strong>, use "
            "<strong>API → App login accounts</strong> (email + password there).<br><br>"
            "This <strong>Token</strong> table is for Django <strong>User</strong> API tokens only."
            "</div>"
        )
