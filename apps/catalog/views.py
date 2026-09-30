from django.contrib import messages
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from apps.accounts.access import employee_required, get_brand_for_user
from apps.sales.ingest import ingest_report
from apps.sales.models import RawReport
from apps.sales.sync import sync_brand

from .forms import BrandForm, ProductForm, UploadReportForm
from .models import Brand, Product


# ---------------------------------------------------------------------------
# Brands (employees only)
# ---------------------------------------------------------------------------


@employee_required
def brand_list(request):
    brands = Brand.objects.prefetch_related("marketplaces").order_by("name")
    return render(request, "catalog/brand_list.html", {"brands": brands})


@employee_required
def brand_create(request):
    form = BrandForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        brand = form.save()
        messages.success(request, f"Brand {brand.name} created.")
        return redirect("catalog:brand_edit", slug=brand.slug)
    return render(request, "catalog/brand_form.html", {"form": form, "is_new": True})


@employee_required
def brand_edit(request, slug: str):
    brand = get_object_or_404(Brand, slug=slug)
    form = BrandForm(request.POST or None, instance=brand)
    if request.method == "POST" and form.is_valid():
        brand = form.save()
        messages.success(request, "Brand saved.")
        return redirect("catalog:brand_edit", slug=brand.slug)
    return render(
        request,
        "catalog/brand_form.html",
        {
            "form": form,
            "is_new": False,
            "brand": brand,
            "upload_form": UploadReportForm(),
            "sync_runs": brand.sync_runs.all()[:10],
            "raw_reports": brand.raw_reports.all()[:10],
        },
    )


@employee_required
@require_POST
def brand_sync(request, slug: str):
    brand = get_object_or_404(Brand, slug=slug)
    try:
        result = sync_brand(brand)
    except Exception as exc:  # shown to the user; details are in the sync log
        messages.error(request, f"Sync failed: {exc}")
    else:
        messages.success(
            request,
            f"Synced {brand.name}: {result.reports} report(s), {result.new_versions} new or changed order(s).",
        )
    return redirect("catalog:brand_edit", slug=brand.slug)


@employee_required
@require_POST
def brand_upload(request, slug: str):
    brand = get_object_or_404(Brand, slug=slug)
    form = UploadReportForm(request.POST, request.FILES)
    if not form.is_valid():
        messages.error(request, "Choose a report file to upload.")
        return redirect("catalog:brand_edit", slug=brand.slug)

    upload = form.cleaned_data["report_file"]
    try:
        result = ingest_report(
            brand,
            upload.read(),
            source=RawReport.Source.UPLOAD,
            fetched_at=timezone.now(),
            original_filename=upload.name,
        )
    except ValueError as exc:
        messages.error(request, f"Couldn't import {upload.name}: {exc}")
    else:
        if result.duplicate:
            messages.info(request, f"{upload.name} was already imported; nothing changed.")
        else:
            messages.success(
                request,
                f"Imported {result.report.row_count} rows; {result.report.new_versions} new or changed order(s).",
            )
    return redirect("catalog:brand_edit", slug=brand.slug)


# ---------------------------------------------------------------------------
# Products (view: anyone with brand access; edit: employees)
# ---------------------------------------------------------------------------


def product_list(request, slug: str):
    brand = get_brand_for_user(request.user, slug)
    products = brand.products.all()
    q = request.GET.get("q", "").strip()
    if q:
        products = products.filter(
            Q(asin__icontains=q) | Q(title__icontains=q) | Q(amazon_title__icontains=q) | Q(sku__icontains=q)
        )
    return render(request, "catalog/product_list.html", {"brand": brand, "slug": slug, "products": products, "q": q})


@employee_required
def product_create(request, slug: str):
    brand = get_object_or_404(Brand, slug=slug)
    form = ProductForm(request.POST or None, brand=brand)
    if request.method == "POST" and form.is_valid():
        product = form.save()
        messages.success(request, f"Added {product.asin}.")
        return redirect("catalog:product_list", slug=brand.slug)
    return render(request, "catalog/product_form.html", {"form": form, "brand": brand, "slug": slug, "is_new": True})


@employee_required
def product_edit(request, slug: str, asin: str):
    brand = get_object_or_404(Brand, slug=slug)
    product = get_object_or_404(Product, brand=brand, asin=asin.upper())
    form = ProductForm(request.POST or None, instance=product, brand=brand)
    if request.method == "POST" and form.is_valid():
        form.save()
        messages.success(request, f"Saved {product.asin}.")
        next_url = request.POST.get("next", "")
        if next_url.startswith("/") and not next_url.startswith("//"):
            return redirect(next_url)
        return redirect("catalog:product_list", slug=brand.slug)
    return render(
        request,
        "catalog/product_form.html",
        {"form": form, "brand": brand, "slug": slug, "product": product, "is_new": False, "next": request.GET.get("next", "")},
    )


@employee_required
@require_POST
def product_delete(request, slug: str, asin: str):
    brand = get_object_or_404(Brand, slug=slug)
    product = get_object_or_404(Product, brand=brand, asin=asin.upper())
    product.delete()
    messages.success(request, f"Removed {asin}. It will be re-created if it appears in new order data.")
    return redirect("catalog:product_list", slug=brand.slug)
