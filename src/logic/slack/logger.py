from src.logic.slack.client import Client


def slack_logger(msg: str):
    client = Client()
    client.post_message("C05G8RMHNTW", msg)
