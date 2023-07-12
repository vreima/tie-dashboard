from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # mongohost: str
    # mongopassword: str
    # mongoport: str
    mongo_url: str
    severa_client_id: str
    severa_client_secret: str
    severa_client_scope: str
    slack_token_bot: str
    openai_api_key: str
    openai_api_org: str
    admin_password: str
    railway_static_url: str
    debug_mode: bool = False


settings = Settings()
