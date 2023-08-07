# import os
import re
from collections.abc import Iterable
from datetime import datetime

import arrow
import httpx
import openai_async
import pandas as pd

# import openai_async
from loguru import logger
from pydantic import BaseModel
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web import SlackResponse

import src.logic.slack.models
from src.config import settings
from src.logic.pressure.pressure import fetch_pressure
from src.logic.processing import load_merge_pivot
from src.logic.severa.client import fetch_invalid_salescases
from src.util.daterange import DateRange
from src.util.process import search_string_for_datetime


class OfferListing(BaseModel):
    timestamp: str
    message: str
    url: str
    # deadline_humanized: str | None = None
    deadline: datetime | None = None


class Client:
    def __init__(self):
        self._client = WebClient(token=settings.slack_token_bot)

    def user_by_id(self, user_id: str) -> str | None:
        """
        Get username from user id, or None if not known.
        """
        return self._users()[0].get(user_id, None)

    def user_by_name(self, user_name: str) -> str | None:
        """
        Get user id from username or None if not known.
        """
        return self._users()[1].get(user_name, None)

    def _users(
        self, _cache={}, _reversed={}  # noqa: B006
    ) -> tuple[dict[str, str], dict[str, str]]:
        """
        Get a dict of user IDs and their display names (ID -> name). Cached.
        """
        if _cache:
            return _cache, _reversed

        response = self._client.users_list()

        for batch in response:
            for user in batch["members"]:
                if user["deleted"] or user["is_bot"] or user["is_app_user"]:
                    continue

                if "real_name" not in user or not user["real_name"]:
                    logger.warning(user)

                username = user.get("real_name", "")

                if (
                    "display_name" in user["profile"]
                    and user["profile"]["display_name"]
                ):
                    username = user["profile"]["display_name"]

                _cache[user["id"]] = username
                _reversed[username] = user["id"]

        # Hardcoded special cases, ie. the onyl bot user we want to acknowledge
        _cache["U048USFG5B2"] = "tie_botti"
        _reversed["tie_botti"] = "U048USFG5B2"

        # Validation, in case the usernames are not unique
        for user_id, username in zip(_cache.keys(), _reversed.keys(), strict=True):
            if user_id != _reversed[username] or username != _cache[user_id]:
                logger.critical(user_id, _cache[user_id])
                logger.critical(_reversed[username], username)

        logger.info(_cache)

        return _cache, _reversed

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
            raise

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
            link_names=True,
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

        strip_urls_etc = re.sub(r"<[^|]*\|([^|]*)>", r"\1", text)
        strip_bold = re.sub(r"\*([^*]*)\*", r"\1", strip_urls_etc)
        strip_italics = re.sub(r"_([^_]*)_", r"\1", strip_bold)

        return strip_italics

    def user_ids_to_names(self, text: str) -> str:
        """
        Convert known Slack <@USER_ID>s to plain text names.
        """
        for user_id, name in self._users()[0].items():
            text = re.sub(rf"<@{user_id}>", f"@{name}", text)

        return text

    def names_to_user_ids(self, text: str) -> str:
        """
        Convert known plain text names to corresponding <@USER_ID>s.
        """
        for name, user_id in self._users()[1].items():
            text = re.sub(rf"@{name}\b", f"<@{user_id}>", text)

        return text

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
                if msg.get("subtype", "") == "tombstone":
                    continue

                dt = search_string_for_datetime(msg.get("text"))
                ts = msg.get("ts").replace(".", "")

                yield OfferListing(
                    message=self.unformat(msg["text"]),
                    timestamp=msg["ts"],
                    deadline=None if dt is None else dt.datetime,
                    # deadline_humanized=None if dt is None else dt.humanize(locale="fi"),
                    url=f"https://tietoa.slack.com/archives/{channel}/p{ts}",
                )

    ###################################
    # Events API / OpenAI integration #
    ###################################

    def fetch_replies(self, channel: str, ts: str) -> Iterable[dict[str, str]]:
        """
        Get replies by channel and thread id (timestamp) in OpenAI chat format.
        """
        batches = self._client.conversations_replies(channel=channel, ts=ts)

        messages = [message for batch in batches for message in batch["messages"]]

        if len(messages) == 1 and "thread_ts" in messages[0]:
            message = messages[0]
            yield from self.fetch_replies(channel, ts=message["thread_ts"])
        else:
            for reply in messages:
                user = self.user_by_id(reply["user"])
                message = self.user_ids_to_names(self.unformat(reply["text"]))

                yield {"role": "user", "content": f"@{user}: {message}"}

    async def process_app_mention_event(
        self, event: src.logic.slack.models.AppMentionWrapperModel
    ):
        """
        Respond to chat mentions with OpenAI.
        """
        ts = event.event.ts

        channel = event.event.channel

        replies = list(self.fetch_replies(channel, ts))

        chat = [
            {
                "role": "system",
                "content": "Olet @tie_botti, yrityksen Tietoa Finland Oy "
                "Tietomallinnus-yksikön hieman sarkastinen keskustelubotti, joka toimii Slackissä. "
                "Tietoa Finland Oy on Helsinkiläinen rakennusalan ja tietomallintamisen konsulttiyhtiö. "
                "Pyri käyttämään rentoa puhekieltä ja kevyttä ironiaa. ",
            },
            *replies,
        ]

        logger.debug(chat)

        openai_response = await openai_chat(messages=chat)
        formatted_response = self.names_to_user_ids(openai_response)

        logger.debug("posting message...")

        self._client.chat_postMessage(
            channel=channel,
            text=formatted_response,
            thread_ts=ts,
            unfurl_links=False,
            unfurl_media=False,
            link_names=True,
        )


async def format_pressure_as_slack_block():
    """
    Fetch last weeks pressure ('kiire') results and format them as a block.
    """
    now = arrow.utcnow().to("Europe/Helsinki")

    last_week_start = now.shift(weeks=-1).floor("week")

    try:
        readings = pd.DataFrame(
            [
                dict(model)
                for model in await fetch_pressure(
                    now.shift(weeks=-2).floor("week"),
                    now.shift(weeks=-1).ceil("week"),
                    None,
                )
            ]
        )

        readings["date"] = pd.to_datetime(readings.loc[:, "date"], utc=True)

        weekly = readings.groupby([pd.Grouper(key="date", freq="W")])[["x", "y"]].mean()
        diff = weekly.diff()

        def f(val, val_diff):
            return (
                f"*{val:.1%}*\t("
                + ("▲" if val_diff >= 0 else "▼")
                + f" {val_diff:+.1%})"
            )

        pressure_titles = (
            ":hammer_and_pick: Edellisen viikon kiireen määrä:\n:bomb: Edellisen viikon kiireen tuntu:\n"
            f"        ⤷ perustuu {len(readings[readings.date > pd.Timestamp(last_week_start.datetime)])} <https://tie.up.railway.app/kiire/|kyselyvastaukseen>"
        )
        pressure_text = (
            (
                f"{f(weekly.x.iloc[1], diff.x.iloc[1])}\n"
                f"{f(weekly.y.iloc[1], diff.y.iloc[1])}\n"
            )
            if len(weekly) > 1
            else (f"{weekly.x.iloc[0]:.1%}\n{weekly.y.iloc[0]:.1%}\n")
        )

        return {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": pressure_titles,
                },
                {
                    "type": "mrkdwn",
                    "text": pressure_text,
                },
            ],
        }
    except IndexError:
        return {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": ":hammer_and_pick: Edelliseltä viikolta ei <https://tie.up.railway.app/kiire/|kiirekyselyvastauksia>.",
                },
            ],
        }


async def format_salescases_as_slack_block():
    """
    Fetch invalid/incorrect salescases from Severa and format them as a block.
    """
    salescases_df = await fetch_invalid_salescases()

    salescases_text = ":sparkles: Sisällä olevien <https://tie.up.railway.app/severa/salescases|tarjousten suolauslista>:\n"
    for key, group in salescases_df.groupby("id"):
        if not group.empty:
            salescases_text += f"*{key}*:\n"
            for _row_num, row in group.iterrows():
                salescases_text += f"> <https://severa.visma.com/project/{row.guid}|{row['name']}>{' vaihe _' + row.phase + '_' if not pd.isna(row.phase) else ''} (@{row.soldby})\n"

    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": salescases_text,
        },
    }


async def format_kpi_totals_as_slack_block():
    """
    Fetch rolling KPI totals for the last n (default=30) days and format them as a block.
    """
    # We need 30 days to get an accurate windowed sum for current day,
    # 7 days more to get another estimate for the last week (to show difference),
    # so 37 days. Let's make that 40 just to be sure.
    span = DateRange(-40)
    windowed_data = await load_merge_pivot(span)
    windowed_data = windowed_data.reset_index().iloc[[-7, -1]]
    current_weeks_data = windowed_data.iloc[-1]
    diff_from_last_week = windowed_data.diff().iloc[-1]

    # date                      6 days 00:00:00
    # absences                             25.0
    # billing                           -1125.0
    # hour_cost                             0.0
    # maximum                               0.0
    # salesvalue                        -7075.0
    # workhours                           -66.5
    # workhours_productive                -34.0
    # workhours_unproductive              -32.5
    # total_hours                         -41.5
    # cost                              -1989.0
    # margin                              864.0
    # margin%                         -5.382964
    # laskutusaste

    # Hack for nice-aligning percentage formatting...
    current_weeks_data["margin%"] *= 100.0
    current_weeks_data["billing_rate"] *= 100.0
    diff_from_last_week["margin%"] *= 100.0
    diff_from_last_week["billing_rate"] *= 100.0

    kpi_totals_list = []

    PADDING = 10

    cols = [
        "billing",
        "cost",
        "margin",
        "margin%",
        "billing_rate",
        "salesvalue",
        "uncounted_hours",
    ]
    kpi_names = [
        "Laskutus",
        "Kulut",
        "Kate",
        "Kate-%",
        "Laskutusaste",
        "Tilaukset",
        "Tuntikirjauksia hukassa",
    ]
    formats = [
        "{: >{PADDING}_.2f} €",
        "{: >{PADDING}_.2f} €",
        "{: >{PADDING}_.2f} €",
        "{: >{PADDING}_.2f} %",
        "{: >{PADDING}_.2f} %",
        "{: >{PADDING}_.2f} €",
        "{: >{PADDING}_.1f} h",
    ]
    diff_formats = [
        "{: >+{PADDING}_.2f} €",
        "{: >+{PADDING}_.2f} €",
        "{: >+{PADDING}_.2f} €",
        "{: >+{PADDING}_.2f} %",
        "{: >+{PADDING}_.2f} %",
        "{: >+{PADDING}_.2f} €",
        "{: >+{PADDING}_.1f} h",
    ]

    max_len = max(len(name) for name in kpi_names) + 4

    for col, kpi_name, this_week_format, diff_format in zip(
        cols, kpi_names, formats, diff_formats, strict=True
    ):
        difference = diff_from_last_week[col]
        difference_text = ("▲   " if difference >= 0 else "▼   ") + diff_format.format(
            difference, PADDING=PADDING
        )
        kpi_totals_list += [
            f"{kpi_name + ':': <{max_len}} "
            + this_week_format.format(current_weeks_data[col], PADDING=PADDING).replace(
                "_", " "
            )
            + " " * 12
            + f"{difference_text}".replace("_", " ")
        ]

    header1 = "30 vrk liukuva summa"
    header2 = "vrt viime viikoon"
    header = f"{header1: >{max_len + PADDING + 3}}{header2: >{PADDING + 18}}"

    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f":bar_chart: Tunnuslukuja:\n```{header}\n"
            + "\n".join(kpi_totals_list)
            + "```",
        },
    }


async def format_offers_as_slack_block(slack: Client):
    """
    Get unmarked/open offers from Slack channel and format them as a block.
    """
    now = arrow.utcnow().to("Europe/Helsinki")

    unmarked_offers = slack.fetch_unmarked_offers(
        settings.channel_tie_tarjouspyynnot, "k", now.shift(months=-3).floor("month")
    )

    if unmarked_offers:
        newline = "\n"
        fi = "fi"
        # granularity = ["month", "day", "hour"]
        formatted_strs = (
            f"📣 Kanavan #tie_tarjouspyynnöt <https://{settings.railway_static_url}/slack/offers|käsittelemättömät viestit>:\n"
            + "\n".join(
                (
                    f"> *<{offer.url}|{arrow.get(float(offer.timestamp)).format('DD.MM.YYYY')}>* | "
                    f"{f'*DL _{arrow.get(offer.deadline).humanize(locale=fi)}_* |' if offer.deadline else ''} {offer.message.split(newline)[0]}"
                )
                for offer in unmarked_offers
            )
        )
    else:
        formatted_strs = (
            "📣 Kanavalla #tie_tarjouspyynnöt ei käsittelemättömiä viestejä ✨"
        )

    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": formatted_strs,
        },
    }


async def send_weekly_slack_update(channel: str | None = None):
    """
    Format and send the weekly 'Viikkopalaveri' message.
    """
    logger.debug(f"Sending weekly msg to {channel}.")
    if not channel:
        channel = settings.channel_yks_tietomallintaminen

    now = arrow.utcnow().to("Europe/Helsinki")
    slack = Client()

    # Forming the blocks:
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Viikkopalaveri {now.format('D.M.YYYY')}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Huomenta @timpat ja tervetuloa viikkopalaveriin!",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "<https://app.slack.com/huddle/T1FB2571R/C2RTSBH2T|"
                    ":headphones: Huddle>",
                },
                {
                    "type": "mrkdwn",
                    "text": "<https://miro.com/app/board/o9J_kjURPUs=/"
                    "?share_link_id=181288254962|🗓️ Miro>",
                },
                {
                    "type": "mrkdwn",
                    "text": "<https://docs.google.com/document/d/"
                    "1uRIynIL0bU0SHZZJtyNYSV-7Iub6m3Gy-cjo4GkrKXY/"
                    "edit?usp=sharing|📋 Asialista>",
                },
            ],
        },
        {"type": "divider"},
    ]

    blocks += [
        await format_offers_as_slack_block(slack),
        {"type": "divider"},
        await format_salescases_as_slack_block(),
        {"type": "divider"},
        await format_pressure_as_slack_block(),
        {"type": "divider"},
        await format_kpi_totals_as_slack_block(),
    ]

    slack.post_message(
        channel=channel,
        message_text=f"Viikkopalaveri {now.format('D.M.YYYY')}",
        blocks=blocks,
    )


async def send_weekly_slack_update_debug() -> None:
    """
    Sends the weekly 'Viikkopalaveri' message to a debugging channel.
    """
    await send_weekly_slack_update(settings.channel_tie_testaus)


###########
# OpenAI  #
###########


async def openai_chat(
    messages: list[dict[str, str]],
    model: str = "gpt-4",
    max_tokens: int = 1024 * 3,
    temp: float = 0.6,
    timeout: float = 240.0,
) -> str:
    try:
        response = await openai_async.chat_complete(
            settings.openai_api_key,
            timeout=timeout,
            payload={
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temp,
            },
        )
    except httpx.ReadTimeout:
        logger.error("httpx.ReadTimeout")
        return "httpx.ReadTimeout"

    response = response.json()

    logger.debug(f"{response=}")

    suffix = ""
    result = ""

    if "error" in response:
        err = response["error"]
        logger.error(f"Error: {err['type']} / {err['code']}\n{err['message']}")
        return f"[{err['code']}] {err['message']}"

    try:
        if response["choices"][0]["finish_reason"] == "length":
            logger.warning(f"OpenAI response cut off, {max_tokens} tokens reached")
            suffix = response["choices"][0]["message"]["content"] + "..."

        result = response["choices"][0]["message"]["content"] + suffix
    except Exception:
        logger.exception("Malformed response from OpenAI")
        result = "Undefined error, please refer to logs."
    else:
        logger.info(
            f"OpenAI response OK. Total tokens: {response['usage']['total_tokens']}."
        )

    return result.strip()


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
