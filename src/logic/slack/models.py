from typing import Literal

from pydantic import BaseModel, Extra


class ChallengeModel(BaseModel):
    type: Literal["url_verification"]  # noqa: A003
    token: str
    challenge: str


class AppMentionModel(BaseModel):
    type: Literal["app_mention"]  # noqa: A003
    user: str
    text: str
    ts: str
    channel: str
    event_ts: str


class AppMentionWrapperModel(BaseModel, extra=Extra.allow):
    type: Literal["event_callback"]  # noqa: A003
    token: str
    team_id: str
    api_app_id: str
    event_id: str
    event_time: int
    event: AppMentionModel
