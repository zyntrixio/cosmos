from flask_wtf import FlaskForm
from wtforms import BooleanField


class DeleteRetailerActionForm(FlaskForm):
    acceptance = BooleanField(
        label="I understand what will be deleted and I wish to proceed",
        render_kw={"class": "form-check-input"},
    )
