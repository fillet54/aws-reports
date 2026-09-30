from django.contrib import messages
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth import views as auth_views
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy

from .access import employee_required
from .forms import LoginForm, OwnPasswordChangeForm, SetUserPasswordForm, UserCreateForm, UserEditForm
from .models import User


class LoginView(auth_views.LoginView):
    template_name = "accounts/login.html"
    authentication_form = LoginForm
    redirect_authenticated_user = True


class PasswordChangeView(auth_views.PasswordChangeView):
    template_name = "accounts/password_change.html"
    form_class = OwnPasswordChangeForm
    success_url = reverse_lazy("home")

    def form_valid(self, form):
        messages.success(self.request, "Your password was changed.")
        return super().form_valid(form)


@employee_required
def user_list(request):
    users = User.objects.select_related("brand").order_by("role", "username")
    return render(request, "accounts/user_list.html", {"users": users})


@employee_required
def user_create(request):
    form = UserCreateForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        user = form.save()
        messages.success(request, f"User {user.username} created.")
        return redirect("accounts:user_list")
    return render(request, "accounts/user_form.html", {"form": form, "is_new": True})


def _editable_user(request, pk: int) -> User:
    target = get_object_or_404(User, pk=pk)
    # Only superusers may change superuser accounts (e.g. reset their password).
    if target.is_superuser and not request.user.is_superuser:
        raise PermissionDenied
    return target


@employee_required
def user_edit(request, pk: int):
    target = _editable_user(request, pk)
    form = UserEditForm(request.POST or None, instance=target, editing_self=target == request.user)
    if request.method == "POST" and form.is_valid():
        form.save()
        messages.success(request, f"User {target.username} updated.")
        return redirect("accounts:user_list")
    return render(request, "accounts/user_form.html", {"form": form, "is_new": False, "target": target})


@employee_required
def user_set_password(request, pk: int):
    target = _editable_user(request, pk)
    form = SetUserPasswordForm(target, request.POST or None)
    if request.method == "POST" and form.is_valid():
        form.save()
        if target == request.user:
            update_session_auth_hash(request, target)
        messages.success(request, f"Password for {target.username} updated.")
        return redirect("accounts:user_list")
    return render(request, "accounts/user_password.html", {"form": form, "target": target})
