from pydantic import BaseModel, Extra
from typing import Literal

class ChallengeModel(BaseModel):
    token: str
    challenge: str
    type: str

class AppMentionModel(BaseModel):
    user: str
    text: str
    ts: str
    channel: str
    event_ts: str


class AppMentionWrapperModel(BaseModel, extra=Extra.allow):
    type: Literal["event_callback"]
    token: str
    team_id: str
    api_app_id: str
    event_id: str
    event_time: int
    event: AppMentionModel