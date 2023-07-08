import os
import re
from datetime import datetime
from collections.abc import Iterable

import arrow

# import openai_async
from loguru import logger
from pydantic import BaseModel
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web import SlackResponse

from src.util.process import search_string_for_datetime

CHANNEL_YKS_TIETOMALLINTAMINEN = "C2RTSBH2T"
CHANNEL_TIE_TARJOUSPYYNNOT = "CSFQ71ANA"
CHANNEL_KONSU_TESTAUS = "G0190SLGYHY"


class OfferListing(BaseModel):
    timestamp: str
    message: str
    url: str
    # deadline_humanized: str | None = None
    deadline: datetime | None = None


class Client:
    def __init__(self):
        self._client = WebClient(token=os.environ["SLACK_TOKEN_BOT"])

    ###################
    # General methods #
    ###################

    def fetch_messages(self, channel: str, newer_than: arrow.Arrow) -> SlackResponse:
        """
        Get Slack message history from a channel.
        """
        try:
            return self._client.conversations_history(
                channel=channel, oldest=newer_than.timestamp()
            )

        except SlackApiError as err:
            logger.error(err)
            return str(err)

    def filter_messages_by_reaction(
        self, response: SlackResponse, reaction_to_filter: str
    ):
        """
        Filter out messages that have a reaction.
        """
        return (
            msg
            for batch in response
            for msg in batch["messages"]
            if (
                "reactions" not in msg
                or reaction_to_filter
                not in (reaction["name"] for reaction in msg["reactions"])
            )
        )

    def post_message(self, channel: str, message_text: str, blocks=None) -> None:
        """
        Send a message to channel.
        """
        self._client.chat_postMessage(
            channel=channel,
            blocks=blocks,
            text=message_text,
            unfurl_links=False,
            unfurl_media=False,
        )

    def unformat(self, text: str) -> str:
        """
        Remove Slack formatting.
        """
        # Detect all sub-strings matching <(.*?)>
        # Within those sub-strings, format content starting with #C as a channel link
        # Format content starting with @U or @W as a user mention
        # Format content starting with !subteam as a user group mention
        # Format content starting with ! according to the rules for special mentions
        # For any other content within those sub-strings, format as a URL link
        # Once the format has been determined, check for a pipe (|) - if present, use the text following the pipe as the label for the link or mention.

        return re.sub(r"<[^|]*\|([^|]*)>", r"\1", text)

    ###################
    # Business logic  #
    ###################

    def fetch_unmarked_offers(
        self, channel: str, reaction_to_filter: str, oldest: arrow.Arrow
    ) -> Iterable[OfferListing]:
        """
        Get messages from channel, filter out those that are marked as resolved
        with :K:, try searching them for a deadline datetime value, yield values as
        OfferListing
        """

        response = list(
            self.filter_messages_by_reaction(
                self.fetch_messages(channel, oldest), reaction_to_filter
            )
        )

        for msg in response:
            if msg.get("type") == "message":
                dt = search_string_for_datetime(msg.get("text"))
                ts = msg.get("ts").replace(".", "")

                yield OfferListing(
                    message=self.unformat(msg["text"]),
                    timestamp=msg["ts"],
                    deadline=None if dt is None else dt.datetime,
                    # deadline_humanized=None if dt is None else dt.humanize(locale="fi"),
                    url=f"https://tietoa.slack.com/archives/{channel}/p{ts}",
                )


# {
# "ok": true,
# "type": "message",
# "message": {
#     "type": "message",
#     "text": "Hi there!",
#     "user": "W123456",
#     "ts": "1648602352.215969",
#     "team": "T123456",
#     "reactions": [
#         {
#             "name": "grinning",
#             "users": [
#                 "W222222"
#             ],
#             "count": 1
#         },
#         {
#             "name": "question",
#             "users": [
#                 "W333333"
#             ],
#             "count": 1
#         }
#     ],
#     "permalink": "https://xxx.slack.com/archives/C123456/p1648602352215969"
# },
# "channel": "C123ABC456"
# }


# blocks = [
#     {
#         "type": "header",
#         "text": {
#             "type": "plain_text",
#             "text": f"Viikkopalaveri {now.format('D.M.YYYY')}",
#             "emoji": True,
#         },
#     },
#     {
#         "type": "section",
#         "text": {
#             "type": "mrkdwn",
#             "text": "Huomenta @timpat ja tervetuloa viikkopalaveriin!",
#         },
#     },
#     {"type": "divider"},
#     {
#         "type": "section",
#         "fields": [
#             {
#                 "type": "mrkdwn",
#                 "text": "<https://app.slack.com/huddle/T1FB2571R/C2RTSBH2T|"
#                 ":headphones: Huddle>",
#             },
#             {
#                 "type": "mrkdwn",
#                 "text": "<https://miro.com/app/board/o9J_kjURPUs=/"
#                 "?share_link_id=181288254962|🗓️ Miro>",
#             },
#             {
#                 "type": "mrkdwn",
#                 "text": "<https://docs.google.com/document/d/"
#                 "1uRIynIL0bU0SHZZJtyNYSV-7Iub6m3Gy-cjo4GkrKXY/"
#                 "edit?usp=sharing|📋 Asialista>",
#             },
#             {
#                 "type": "mrkdwn",
#                 "text": "<https://severa-data-dashboard.vercel.app/"
#                 "sales|📊 Myynti>",
#             },
#             {
#                 "type": "mrkdwn",
#                 "text": "<https://tie_bot-1-n6951403.deta.app|"
#                 ":control_knobs: Ohjauspaneeli>",
#             },
#         ],
#     },
#     {"type": "divider"},
#     {
#         "type": "section",
#         "text": {
#             "type": "mrkdwn",
#             "text": formated,
#         },
#     },
#     {"type": "divider"},
#     {
#         "type": "section",
#         "text": {
#             "type": "mrkdwn",
#             "text": (
#                 f":bar_chart: *Yksikön KPI:t* "
#                 f"(work-in-progress!):```\n{'Laskutus (viim. 30 pv):': <38}"
#                 f"{slider_laskutus_tavoitteeseen} € \n"
#                 f"{'Projektikate (viim. 30 pv):': <38}"
#                 f"{slider_projektikannattavuus} € \n"
#                 f"{'Projektikannattavuus (viim. 30 pv):': <38}"
#                 f"{slider_projektikannattavuus_tuntihinta} €/h \n"
#                 f"{'Laskutusaste (viim. 30 pv):': <38}"
#                 f"{slider_laskutusaste} h \n"
#                 f"{f'Ennuste ({this_month_str}):': <38}"
#                 f"{slider_ennuste_tavoitteeseen_tässä} € \n"
#                 f"{f'Ennuste ({next_month_str}):': <38}"
#                 f"{slider_ennuste_tavoitteeseen_seuraavassa} € \n"
#                 f"{f'Myynti ({this_month_str}):': <38}"
#                 f"{slider_myynti_this} € \n"
#                 f"{f'Myynti ({next_month_str}):': <38}"
#                 f"{slider_myynti_next} € \n"
#                 f"{'Resursointi (seur. 30 pv):': <38}"
#                 f"{slider_allocations} h\n"
#                 f"{'Laskutusaste (resur., seur. 30 pv):': <38}"
#                 f"{slider_external_allocations} h\n"
#                 f"{f'Tunteja tarjottuna ({next_month_str}):': <38} "
#                 f"{sales[next_month_key]['work']['expected']:.1f} .. "
#                 f"{sales[next_month_key]['work']['maximum']:.1f} h\n```"
#             ),
#         },
#     },
#     {"type": "divider"},
#     {
#         "type": "section",
#         "text": {
#             "type": "mrkdwn",
#             "text": ":sparkles: Sisällä olevien tarjousten suolauslista:\n\n"
#             + suolaus,
#         },
#     }
