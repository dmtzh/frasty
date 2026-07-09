from __future__ import annotations
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
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
    """
    Safely convert a value to a plain Python int.

    ...

    Returns None for any input that cannot be unambiguously converted to an int,
    including:
      - bool values (Python's bool is a subclass of int, so True/False would
        otherwise be accepted as 1/0)
      - bytes values (binary data, not string representations of numbers)
      - floats with a fractional part
      - strings with a fractional part
      - float('inf'), float('-inf'), float('nan')
      - Decimal('inf'), Decimal('-inf'), Decimal('nan')
      - extremely large Decimal values that overflow int conversion
      - None, lists, dicts, and other unsupported types

    OverflowError and InvalidOperation raised internally by int() / Decimal
    conversion are caught and result in None, so the caller never sees an
    exception from this function.
    """
    # 1. Reject booleans explicitly: Python's bool is a subclass of int,
    # so int(True) == 1 and int(False) == 0. We reject them to prevent
    # accidental acceptance of boolean values as integers.
    if isinstance(value, bool):
        return None

    # 2. Reject bytes explicitly: int(b"42") works in Python, but bytes represent
    # binary data, not string-encoded numbers. In a JSON-based pipeline, bytes
    # should never appear as numeric input.
    if isinstance(value, bytes):
        return None

    # 3. Strict float handling: reject fractional parts and special values (inf/nan).
    if isinstance(value, float):
        if not value.is_integer():
            return None

    # 4. Strict Decimal handling: reject fractional parts and special values.
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return None
        try:
            if value != value.to_integral_value():
                return None
        except InvalidOperation:
            return None

    try:
        return int(value)
    except (ValueError, TypeError, OverflowError, InvalidOperation):
        # ValueError   -> int("abc"), int("5.0"), int(float('nan'))
        # TypeError    -> int(None), int([]), int({})
        # OverflowError-> int(float('inf')), int(Decimal('inf')),
        #                 int(Decimal('1e1000000'))
        # InvalidOperation -> Decimal edge cases during conversion
        return None

class PositiveInt(int):
    """
    A strictly positive integer type (value > 0).

    This class is a thin wrapper around Python's built-in ``int`` that enforces
    the invariant "value > 0" at construction time. It is intended for use in
    configuration dataclasses (e.g. ``WaitBeforeProcessConfig.duration_ms``)
    where a positive integer is semantically required.

    Construction modes
    ------------------
    1. Direct construction via ``PositiveInt(value)``:
       - Accepts ONLY ``int`` values (not ``bool``, ``float``, ``str``, etc.).
       - Raises ``TypeError`` for non-int types.
       - Raises ``ValueError`` for non-positive values or booleans.
       - Use this mode when the caller is certain the value is a valid ``int``
         (e.g. after parsing/validation, or when working with already-validated data).

    2. Safe parsing via ``PositiveInt.parse(value)``:
       - Accepts ``int``, ``float`` (whole numbers only), ``str``, ``Decimal``.
       - Returns a ``PositiveInt`` instance on success, or ``None`` on any failure.
       - Never raises exceptions.
       - Use this mode when the input comes from untrusted sources
         (e.g. JSON configuration, user input).

    Important limitations
    ---------------------
    * ``PositiveInt`` inherits from ``int``. Arithmetic operations
      (``+``, ``-``, ``*``, etc.) return a plain ``int``, NOT a ``PositiveInt``.
      This means the "value > 0" invariant is NOT preserved across arithmetic.
      Example::

          x = PositiveInt(5)
          y = PositiveInt(3)
          z = x - y   # z == 2, but type(z) is int, not PositiveInt
          w = y - x   # w == -2, type(w) is int, and the invariant is violated

      If you need to preserve the invariant, re-validate after arithmetic:
      ``PositiveInt.parse(x - y)``.

    * ``isinstance(PositiveInt(5), int)`` returns ``True``. This is intentional
      for compatibility with code that expects plain ``int`` values.

    * Type checkers (mypy, pyright) cannot statically verify the "value > 0"
      invariant. This class provides runtime validation only.

    Edge cases handled by ``parse()``
    ---------------------------------
    * ``bool`` values are explicitly rejected (Python's ``bool`` is a subclass
      of ``int``, so ``True`` would otherwise be accepted as ``1``).
    * ``bytes`` values are explicitly rejected. Although Python's ``int(b"42")``
      returns 42, bytes represent binary data, not string-encoded numbers.
      In a JSON-based pipeline, bytes should never appear as numeric input.
    * ``float('inf')``, ``float('-inf')``, ``float('nan')`` return ``None``.
    * ``Decimal('inf')``, ``Decimal('nan')``, extremely large ``Decimal``
      values return ``None``.
    * Fractional floats (``5.5``) and fractional string literals (``"5.0"``)
      return ``None``. Whole floats (``5.0``) are accepted and converted to ``5``.
    * Negative values and zero return ``None``.

    Examples
    --------
    >>> PositiveInt(5)
    5
    >>> PositiveInt(0)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive integer, got 0
    >>> PositiveInt(-1)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive integer, got -1
    >>> PositiveInt(True)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive integer, got True

    >>> PositiveInt.parse(5)
    5
    >>> PositiveInt.parse("42")
    42
    >>> PositiveInt.parse(5.0)
    5
    >>> PositiveInt.parse(5.5) is None
    True
    >>> PositiveInt.parse("5.0") is None
    True
    >>> PositiveInt.parse(0) is None
    True
    >>> PositiveInt.parse(-1) is None
    True
    >>> PositiveInt.parse(True) is None
    True
    >>> PositiveInt.parse(float('inf')) is None
    True
    >>> PositiveInt.parse(float('nan')) is None
    True
    >>> PositiveInt.parse(None) is None
    True
    >>> PositiveInt.parse("invalid") is None
    True
    """

    def __new__(cls, value: int) -> PositiveInt:
        # 1. Reject booleans explicitly (bool is a subclass of int in Python).
        #    This must be checked BEFORE isinstance(value, int), because
        #    isinstance(True, int) returns True.
        if isinstance(value, bool):
            raise ValueError(f"Expected a positive integer, got {value!r}")
        
        # 2. Reject non-int types (str, float, None, bytes, Decimal, etc.).
        #    The constructor is strict: it accepts ONLY int values.
        #    For safe conversion from other types, use PositiveInt.parse().
        if not isinstance(value, int):
            raise TypeError(f"Expected an int, got {type(value).__name__}")
        
        # 3. Reject non-positive values.
        if value <= 0:
            raise ValueError(f"Expected a positive integer, got {value!r}")
        
        return super().__new__(cls, value)

    def __repr__(self) -> str:
        """
        Return a string representation that clearly identifies this as a PositiveInt.

        This is primarily useful for debugging and logging, where it's important
        to distinguish a validated PositiveInt from a plain int.

        Example:
            >>> repr(PositiveInt(42))
            'PositiveInt(42)'
            >>> str(PositiveInt(42))  # str() is unchanged, returns '42'
            '42'

        Note: This intentionally differs from int.__repr__, which would return '42'.
        If compatibility with code that expects repr(int) is required, remove this method.
        """
        return f"PositiveInt({int(self)})"

    def __str__(self) -> str:
        """
        Return the plain numeric string representation.

        This is necessary because overriding __repr__ in a subclass of int
        can cause __str__ and __format__ (used by f-strings) to fall back
        to the overridden __repr__ in some Python implementations.
        By explicitly defining __str__, we ensure that str(), f-strings,
        and format() return the plain numeric value.

        Example:
            >>> str(PositiveInt(42))
            '42'
            >>> f"{PositiveInt(42)}"
            '42'
            >>> format(PositiveInt(42))
            '42'
        """
        return str(int(self))

    @classmethod
    def parse(cls, value: object) -> PositiveInt | None:
        """
        Safely attempt to convert a value to a PositiveInt.

        Returns a PositiveInt instance on success, or None on any failure.
        This method never raises exceptions.

        See the class docstring for a full list of handled edge cases.
        """
        # 1. Reject booleans (Python's bool is a subclass of int)
        if isinstance(value, bool):
            return None

        # 2. Reject bytes explicitly: int(b"42") works in Python, but bytes
        # represent binary data, not string-encoded numbers.
        if isinstance(value, bytes):
            return None

        # 3. Handle floats: reject fractional parts and special values (inf/nan).
        if isinstance(value, float):
            if not value.is_integer():
                return None
            value = int(value)

        # 4. Handle Decimal: reject fractional parts and special values.
        if isinstance(value, Decimal):
            if value.is_nan() or value.is_infinite():
                return None
            try:
                if value != value.to_integral_value():
                    return None
            except InvalidOperation:
                return None
            try:
                value = int(value)
            except (OverflowError, InvalidOperation):
                return None

        # 5. Attempt integer conversion (handles str, other numeric types, etc.)
        opt_parsed_int = parse_int(value)

        # 6. Enforce positivity
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
