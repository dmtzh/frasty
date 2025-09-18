from collections.abc import Callable
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
    if s is None:
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

def parse_from_dict[T](d: dict[str, str], key: str, parser: Callable[[str], T | None]) -> Result[T, str]:
    if key not in d:
        return Result.Error(f"{key} is missing")
    raw_value = d[key]
    opt_value = parser(raw_value)
    match opt_value:
        case None:
            return Result.Error(f"invalid {key} {raw_value}")
        case value:
            return Result.Ok(value)