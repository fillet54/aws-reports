from django import forms


class StyledFormMixin:
    """Adds daisyUI classes to every widget so templates can render fields generically."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            widget = field.widget
            if isinstance(widget, (forms.CheckboxInput,)):
                css = "checkbox checkbox-primary"
            elif isinstance(widget, forms.CheckboxSelectMultiple):
                css = "checkbox checkbox-primary checkbox-sm"
            elif isinstance(widget, (forms.Select, forms.SelectMultiple)):
                css = "select select-bordered w-full"
            elif isinstance(widget, forms.Textarea):
                css = "textarea textarea-bordered w-full"
                widget.attrs.setdefault("rows", 3)
            elif isinstance(widget, forms.ClearableFileInput) or isinstance(widget, forms.FileInput):
                css = "file-input file-input-bordered w-full"
            else:
                css = "input input-bordered w-full"
            widget.attrs["class"] = f"{widget.attrs.get('class', '')} {css}".strip()
