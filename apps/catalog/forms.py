from django import forms

from apps.accounts.forms_base import StyledFormMixin

from .models import Brand, Product


class BrandForm(StyledFormMixin, forms.ModelForm):
    refresh_token = forms.CharField(
        label="SP-API refresh token",
        required=False,
        widget=forms.PasswordInput(render_value=False, attrs={"autocomplete": "off"}),
        help_text="Stored encrypted. Leave blank to keep the current token.",
    )
    clear_refresh_token = forms.BooleanField(label="Remove stored refresh token", required=False)

    class Meta:
        model = Brand
        fields = ["name", "slug", "marketplaces", "selling_partner_id", "sync_enabled"]
        widgets = {"marketplaces": forms.CheckboxSelectMultiple}
        help_texts = {"marketplaces": "Stores this brand sells in."}

    def save(self, commit=True):
        brand = super().save(commit=False)
        token = self.cleaned_data.get("refresh_token")
        if self.cleaned_data.get("clear_refresh_token"):
            brand.set_refresh_token("")
        elif token:
            brand.set_refresh_token(token.strip())
        if commit:
            brand.save()
            self.save_m2m()
        return brand


class ProductForm(StyledFormMixin, forms.ModelForm):
    class Meta:
        model = Product
        fields = ["asin", "title", "sku", "category", "unit_cost", "launch_date", "notes"]
        widgets = {"launch_date": forms.DateInput(attrs={"type": "date"})}

    def __init__(self, *args, brand=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.brand = brand
        if brand is not None and not self.instance.pk:
            self.instance.brand = brand
        if self.instance.pk:
            # The ASIN is the product's identity; it can't be changed after creation.
            self.fields["asin"].disabled = True

    def clean_asin(self):
        asin = self.cleaned_data["asin"].strip().upper()
        if (
            not self.instance.pk
            and Product.objects.filter(brand=self.brand, asin=asin).exists()
        ):
            raise forms.ValidationError("This brand already has that ASIN.")
        return asin

    def save(self, commit=True):
        product = super().save(commit=False)
        if self.brand is not None:
            product.brand = self.brand
        if commit:
            product.save()
        return product


class UploadReportForm(StyledFormMixin, forms.Form):
    report_file = forms.FileField(
        label="Orders report",
        help_text="An 'All Orders' flat file (.txt / .tsv) exported from Seller Central.",
    )
