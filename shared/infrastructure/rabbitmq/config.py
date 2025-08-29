from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from shared.utils.parse import parse_bool_str

@dataclass(frozen=True)
class RabbitMQUrl:
    value: str

    @staticmethod
    def parse(url: str) -> Optional['RabbitMQUrl']:
        """
        Parses a string into a RabbitMQUrl instance if it is a valid rabbitmq URL.

        Args:
            url (str): The rabbitmq URL string to parse.

        Returns:
            RabbitMQUrl | None: A RabbitMQUrl instance if the string is a valid rabbitmq URL, 
                                or None if the string is None, empty, or invalid.
        """
        if url is None:
            return None
        match url.strip():
            case "":
                return None
            case url_stripped:
                result = urlparse(url_stripped)
                # If the URL does not have a scheme (e.g., amqp)
                # or a network location (e.g., www.example.com),
                # or if the scheme is not amqp, it's not a valid URL
                has_scheme_and_netloc = all([result.scheme, result.netloc])
                has_amqp_scheme = result.scheme in ["amqp"]
                if has_scheme_and_netloc and has_amqp_scheme:
                    return RabbitMQUrl(url_stripped)
                else:
                    return None

@dataclass(frozen=True)
class RabbitMQConfig:
    url: RabbitMQUrl
    publisher_confirms: bool

    @staticmethod
    def parse(url: str, publisher_confirms: str) -> Optional['RabbitMQConfig']:
        opt_url = RabbitMQUrl.parse(url)
        opt_publisher_confirms = parse_bool_str(publisher_confirms)
        if opt_url is None or opt_publisher_confirms is None:
            return None
        return RabbitMQConfig(url=opt_url, publisher_confirms=opt_publisher_confirms)
