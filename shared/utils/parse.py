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

def parse_int(value) -> Result[int, str]:
    try:
        return Result.Ok(int(value))
    except ValueError:
        return Result.Error(f"invalid int value {value}")

def parse_value[T](value: Any, value_name: str, parser: Callable[[Any], T | None]) -> Result[T, str]:
    opt_parsed_value = parser(value)
    match opt_parsed_value:
        case None:
            return Result.Error(f"invalid '{value_name}' value {value}")
        case parsed_value:
            return Result.Ok(parsed_value)

def parse_non_empty_str(value: Any, value_name: str, strip: bool = True) -> Result[str, str]:
    """
    Parse a string value. If the value is not a string, return an error.
    If the value is empty string, return an error.
    If strip is True, strip whitespace from the string and if it is empty, return an error.
    
    Returns:
        Result[str, str]: a success result containing non empty string value, or an error.
    """
    if not isinstance(value, str):
        return Result.Error(f"invalid '{value_name}' value {value}")
    match value.strip() if strip else value:
        case "":
            return Result.Error(f"invalid '{value_name}' value {value}")
        case non_empty_value:
            return Result.Ok(non_empty_value)