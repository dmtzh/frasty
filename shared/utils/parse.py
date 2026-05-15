from __future__ import annotations
from collections.abc import Callable
from typing import Any
from expression import Result
from shared.utils.string import strip_and_lowercase

def parse_bool_str(s: str) -> bool | None:
    """
    Attempts to parse a string as a boolean value.

    Args:
        s (str): The string to parse.

    Returns:
        bool | None: The parsed boolean value, or None if the string is not a valid boolean representation.

    Examples:
        >>> parse_bool_str("true")
        True
        >>> parse_bool_str("false")
        False
        >>> parse_bool_str("yes")
        True
        >>> parse_bool_str("no")
        False
        >>> parse_bool_str("1")
        True
        >>> parse_bool_str("0")
        False
        >>> parse_bool_str("invalid")
        None
    """
    if not isinstance(s, str):
        return None
    bool_map = {
        "true": True,
        "yes": True,
        "1": True,
        "false": False,
        "no": False,
        "0": False
    }
    return bool_map.get(strip_and_lowercase(s), None)

def parse_from_dict[T](d: dict, key: str, parser: Callable[[Any], T | None]) -> Result[T, str]:
    if key not in d:
        return Result.Error(f"'{key}' key is missing")
    raw_value = d[key]
    return parse_value(raw_value, key, parser)

def parse_int(value) -> int | None:
    # Strict float handling: reject fractional parts
    if isinstance(value, float) and not value.is_integer():
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

class PositiveInt(int):
    """
    A strictly positive integer type (value > 0).
    Direct construction raises ValueError on invalid input.
    Use parse() for safe conversion that returns None on failure.
    """

    def __new__(cls, value: int) -> PositiveInt:
        # bool is a subclass of int in Python, so we explicitly reject it
        if isinstance(value, bool) or value <= 0:
            raise ValueError(f"Expected a positive integer, got {value!r}")
        return super().__new__(cls, value)

    @classmethod
    def parse(cls, value: object) -> PositiveInt | None:
        """
        Safely attempt to convert a value to a PositiveInt.
        Returns a PositiveInt instance on success, or None on any failure.
        """
        # 1. Reject booleans (Python's bool is a subclass of int)
        if isinstance(value, bool):
            return None
        # 2. Handle floats: reject fractional parts, convert whole floats safely
        if isinstance(value, float):
            if not value.is_integer():
                return None
            value = int(value)
        # 3. Attempt integer conversion (handles str, Decimal, etc.)
        opt_parsed_int = parse_int(value)
        # 4. Enforce positivity
        match opt_parsed_int:
            case None:
                return None
            case positive_int if positive_int > 0:
                return cls(positive_int)
            case _:
                return None

def parse_value[T, R](value: T, value_name: str, parser: Callable[[T], R | None]) -> Result[R, str]:
    opt_parsed_value = parser(value)
    match opt_parsed_value:
        case None:
            return Result.Error(f"invalid '{value_name}' value {value}")
        case parsed_value:
            return Result.Ok(parsed_value)

def parse_non_empty_str(value: Any, strip: bool = True) -> str | None:
    """
    Parse a string value. If the value is not a string, return None.
    If the value is empty string, return None.
    If strip is True, strip whitespace from the string and if it is empty, return None.
    
    Returns:
        str or None: a success result containing non empty string value, or None.
    """
    if not isinstance(value, str):
        return None
    match value.strip() if strip else value:
        case "":
            return None
        case non_empty_value:
            return non_empty_value