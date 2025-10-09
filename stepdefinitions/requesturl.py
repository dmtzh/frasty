from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

from expression import Result

from shared.domaindefinition import StepDefinition
from shared.utils.string import strip_and_lowercase
from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

from .shared import HttpResponseData

class Url:
    """
    A class representing a URL.

    This class cannot be instantiated directly. Instead,
    use the `parse` static method to create an instance.

    Attributes:
        _url (str): The URL string.

    Methods:
        parse(url: str) -> Url | None: Creates a new Url instance from
        a string, or returns None if the string is None or empty.
    """

    def __init__(self, url: str):
        """
        Private constructor. Do not use directly.

        Args:
            url (str): The URL string.
        """
        if not isinstance(url, str):
            raise TypeError("URL must be a string")
        self._url = url

    @property
    def value(self):
        return self._url

    @staticmethod
    def parse(url: str) -> Url | None:
        """
        Parses a string into a Url instance if it is a valid URL.

        This method checks if the given URL string has a valid scheme 
        and network location, and if the scheme is either 'http' or 'https'. 
        If these conditions are met, it returns a new Url instance; otherwise, 
        it returns None.

        Args:
            url (str): The URL string to parse.

        Returns:
            Url | None: A Url instance if the string is a valid URL, 
                        or None if the string is None, empty, or invalid.
        """
        if url is None:
            return None
        match url.strip():
            case "":
                return None
            case url_stripped:
                result = urlparse(url_stripped)
                # If the URL does not have a scheme (e.g., http, https)
                # or a network location (e.g., www.example.com),
                # or if the scheme is not http or https, it's not a valid URL
                has_scheme_and_netloc = all([result.scheme, result.netloc])
                has_http_or_https_scheme = result.scheme in ["http", "https"]
                if has_scheme_and_netloc and has_http_or_https_scheme:
                    return Url(url_stripped)
                else:
                    return None

class HttpMethod(StrEnum):
    GET = "GET"
    POST = "POST"

    @staticmethod
    def parse(http_method: str) -> HttpMethod | None:
        if http_method is None:
            return None
        match strip_and_lowercase(http_method):
            case "get":
                return HttpMethod.GET
            case "post":
                return HttpMethod.POST
            case _:
                return None

@dataclass(frozen=True)
class RequestUrlInputData:
    url: str
    http_method: str

    @staticmethod
    def from_dict(data) -> Result[RequestUrlInputData, list[ValueErr]]:
        def validate_url() -> Result[str, list[ValueErr]]:
            if "url" not in data:
                return Result.Error([ValueMissing("url")])
            raw_url = data.get("url")
            if not isinstance(raw_url, str):
                return Result.Error([ValueInvalid("url")])
            opt_url = Url.parse(raw_url)
            return Result.Ok(raw_url) if opt_url is not None else Result.Error([ValueInvalid("url")])
        def validate_http_method() -> Result[str, list[ValueErr]]:
            if "http_method" not in data:
                return Result.Error([ValueMissing("http_method")])
            raw_http_method = data.get("http_method")
            if not isinstance(raw_http_method, str):
                return Result.Error([ValueInvalid("http_method")])
            opt_http_method = HttpMethod.parse(raw_http_method)
            return Result.Ok(raw_http_method) if opt_http_method is not None else Result.Error([ValueInvalid("http_method")])
        
        if not isinstance(data, dict):
            return Result.Error([ValueInvalid("data")])
        url_res = validate_url()
        http_method_res = validate_http_method()
        errors = url_res.swap().default_value([]) + http_method_res.swap().default_value([])
        match errors:
            case []:
                return Result.Ok(RequestUrlInputData(url=url_res.ok, http_method=http_method_res.ok))
            case _:
                return Result.Error(errors)
    
    @staticmethod
    def to_dict(data: RequestUrlInputData):
        return {
            "url": data.url,
            "http_method": data.http_method
        }

class RequestUrl(StepDefinition[None]):
    def __init__(self):
        super().__init__(config=None)
    
    @property
    def input_type(self) -> type:
        return RequestUrlInputData
    
    @property
    def output_type(self) -> type:
        return HttpResponseData
    
    @staticmethod
    def create():
        return Result[StepDefinition, list[ValueErr]].Ok(RequestUrl())
    
    @staticmethod
    def validate_input(data):
        validated_input = RequestUrlInputData.from_dict(data)
        return validated_input.map(RequestUrlInputData.to_dict)