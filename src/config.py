import arrow.locales
from pydantic import AnyHttpUrl, MongoDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

# This is a hack to monkey patch Arrow humanization to work with Finnish
arrow.locales.FinnishLocale.timeframes["week"] = {"past": "viikko", "future": "viikon"}
arrow.locales.FinnishLocale.timeframes["weeks"] = {
    "past": "{0} viikkoa",
    "future": "{0} viikon",
}
arrow.locales.FinnishLocale.timeframes["day"] = {"past": "päivä", "future": "päivän"}
arrow.locales.FinnishLocale.timeframes["days"] = {
    "past": "{0} päivää",
    "future": "{0} päivän",
}


class Settings(BaseSettings):
    # mongohost: str
    # mongopassword: str
    # mongoport: str
    mongo_url: MongoDsn
    severa_client_id: str
    severa_client_secret: str
    severa_client_scope: str
    severa_base_url: AnyHttpUrl
    slack_token_bot: str
    openai_api_key: str
    openai_api_org: str
    admin_password: str
    railway_static_url: str # not full url, just domain
    debug_mode: bool = False
    admin_password: str
    channel_yks_tietomallintaminen: str
    channel_tie_tarjouspyynnot: str
    channel_tie_testaus: str

    railway_git_author: str = ""
    railway_git_branch: str = ""
    railway_git_commit_message: str = ""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )


settings = Settings()
