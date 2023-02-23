from flask_wtf import FlaskForm
from wtforms import BooleanField, IntegerField, SelectField, validators

from cosmos.rewards.enums import PendingRewardMigrationActions


class EndCampaignActionForm(FlaskForm):
    handle_pending_rewards = SelectField(
        label="Pending Reward", coerce=PendingRewardMigrationActions, render_kw={"class": "form-control"}
    )
    transfer_balance = BooleanField(label="Transfer balance?", render_kw={"class": "form-check-input"})
    convert_rate = IntegerField(
        label="Balance conversion rate %",
        validators=[validators.NumberRange(min=1, max=100)],
        default=100,
        render_kw={"class": "form-control"},
        description="Percentage of the current active balance to be transferred to the draft campaign.",
    )
    qualify_threshold = IntegerField(
        label="Qualify threshold %",
        validators=[validators.NumberRange(min=0, max=100)],
        default=0,
        render_kw={"class": "form-control"},
        description=(
            "Qualifies for conversion if the current balance is equal or more to the "
            "provided percentage of the target value (active campaign reward_goal)"
        ),
    )
