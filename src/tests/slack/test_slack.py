import pytest

from src.logic.slack.client import Client


@pytest.fixture
def slack_client(mocker):
    return Client()


class TestSlackClient:
    def test_users_cache(mocker):
        class MockedWebClient:
            def users_list():
                return [
                    {
                        "members": [
                            {
                                "deleted": False,
                                "is_bot": False,
                                "is_app_user": False,
                                "real_name": "member",
                                "profile": {},
                                "id": "U0123456",
                            }
                        ]
                    }
                ]

        mocker.patch(
            "src.logic.slack.client.WebClient",
            MockedWebClient,
        )

        Client()

        pass
