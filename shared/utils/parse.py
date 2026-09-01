from __future__ import annotations
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from expression import Result

from shared.utils.string import strip_and_lowercase
from shared.validation import ValueError as ValueErr, ValueMissing, ValueInvalid

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

def parse_dict_field[T](
    d: dict,
    key: str,
    parser: Callable[[Any], T | None],
) -> Result[T, ValueErr]:
    """
    Analog of parse_from_dict that returns typed validation errors
    (ValueMissing / ValueInvalid) instead of plain strings.

    Args:
        d: Source dictionary.
        key: Key to look up in the dictionary.
        parser: Safe parser function that returns T on success or None on failure.

    Returns:
        Result.Ok(T) if the key exists and the parser succeeds.
        Result.Error(ValueMissing(key)) if the key is absent from the dictionary.
        Result.Error(ValueInvalid(key, value)) if the parser returns None.

    Examples:
        >>> parse_dict_field({"name": "alice"}, "name", NonEmptyStr.parse)
        Result.Ok(NonEmptyStr('alice'))

        >>> parse_dict_field({}, "name", NonEmptyStr.parse)
        Result.Error(ValueMissing(name='name'))

        >>> parse_dict_field({"name": ""}, "name", NonEmptyStr.parse)
        Result.Error(ValueInvalid(name='name', value=''))

        >>> parse_dict_field({"count": -1}, "count", PositiveInt.parse)
        Result.Error(ValueInvalid(name='count', value=-1))
    """
    if key not in d:
        return Result.Error(ValueMissing(key))
    raw_value = d[key]
    opt_parsed_value = parser(raw_value)
    match opt_parsed_value:
        case None:
            return Result.Error(ValueInvalid(key, raw_value))
        case parsed_value:
            return Result.Ok(parsed_value)

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

class NonEmptyStr(str):
    """
    A non-empty string type (len(value) > 0).

    This class is a thin wrapper around Python's built-in ``str`` that enforces
    the invariant "len(value) > 0" at construction time. It is intended for use
    in configuration dataclasses (e.g. ``NormalizeTextConfig.field_name``,
    ``GetFromJsonQuery.query``) where a non-empty string is semantically required.

    Construction modes
    ------------------
    1. Direct construction via ``NonEmptyStr(value)``:
       - Accepts ONLY ``str`` values (not ``bytes``, ``int``, ``bool``, ``None``, etc.).
       - Raises ``TypeError`` for non-str types.
       - Raises ``ValueError`` for empty strings (len == 0).
       - Does NOT apply ``strip()`` — the string is stored as-is.
       - Use this mode when the caller is certain the value is a valid non-empty ``str``
         (e.g. after parsing/validation, or when working with already-validated data).

    2. Safe parsing via ``NonEmptyStr.parse(value, strip=True)``:
       - Accepts ``str`` values, rejects all other types.
       - If ``strip=True`` (default): applies ``strip()`` before validation.
         Rejects strings that become empty after stripping (e.g. ``"   "``).
       - If ``strip=False``: does not apply ``strip()``. Rejects only truly empty strings.
       - Returns a ``NonEmptyStr`` instance on success, or ``None`` on any failure.
       - Never raises exceptions.
       - Use this mode when the input comes from untrusted sources
         (e.g. JSON configuration, user input).

    Important limitations
    ---------------------
    * ``NonEmptyStr`` inherits from ``str``. String operations
      (``+``, ``*``, slicing, etc.) return a plain ``str``, NOT a ``NonEmptyStr``.
      This means the "len > 0" invariant is NOT preserved across operations.
      Example::

          x = NonEmptyStr("abc")
          y = NonEmptyStr("def")
          z = x + y   # z == "abcdef", but type(z) is str, not NonEmptyStr
          w = x * 0   # w == "", type(w) is str, and the invariant is violated
          s = x[1:]   # s == "bc", type(s) is str

      If you need to preserve the invariant, re-validate after operations:
      ``NonEmptyStr.parse(x + y)``.

    * The constructor does NOT apply ``strip()``. If you need to trim whitespace,
      use ``NonEmptyStr.parse(value, strip=True)`` or explicitly call ``strip()``
      before construction: ``NonEmptyStr(value.strip())``.

    * ``isinstance(NonEmptyStr("abc"), str)`` returns ``True``. This is intentional
      for compatibility with code that expects plain ``str`` values.

    * Type checkers (mypy, pyright) cannot statically verify the "len > 0"
      invariant. This class provides runtime validation only.

    Edge cases handled by ``parse()``
    ---------------------------------
    * ``bytes`` values are explicitly rejected. Although ``str(b"abc")`` works
      in Python (with encoding), bytes represent binary data, not text.
      In a JSON-based pipeline, bytes should never appear as string input.
    * ``bool``, ``int``, ``float``, ``None``, ``list``, ``dict`` are rejected.
    * Empty strings (``""``) return ``None``.
    * Strings containing only whitespace (``"   "``, ``"\\t\\n"``) return ``None``
      when ``strip=True`` (default), but are accepted when ``strip=False``.
    * Unicode whitespace (``\\u00A0``, ``\\u3000``, etc.) is handled correctly
      by ``str.strip()``.

    Examples
    --------
    >>> NonEmptyStr("abc")
    abc
    >>> NonEmptyStr("  abc  ")  # Constructor does NOT strip
    '  abc  '
    >>> NonEmptyStr("")
    Traceback (most recent call last):
        ...
    ValueError: Expected a non-empty string, got empty string
    >>> NonEmptyStr(123)
    Traceback (most recent call last):
        ...
    TypeError: Expected a str, got int

    >>> NonEmptyStr.parse("abc")
    abc
    >>> NonEmptyStr.parse("  abc  ", strip=True)
    abc
    >>> NonEmptyStr.parse("  abc  ", strip=False)
    '  abc  '
    >>> NonEmptyStr.parse("") is None
    True
    >>> NonEmptyStr.parse("   ", strip=True) is None
    True
    >>> NonEmptyStr.parse("   ", strip=False) is not None
    True
    >>> NonEmptyStr.parse(123) is None
    True
    >>> NonEmptyStr.parse(None) is None
    True
    >>> NonEmptyStr.parse(b"abc") is None
    True
    """

    def __new__(cls, value: str) -> NonEmptyStr:
        # 1. Reject non-str types explicitly.
        #    The constructor is strict: it accepts ONLY str values.
        #    For safe conversion from other types, use NonEmptyStr.parse().
        if not isinstance(value, str):
            raise TypeError(f"Expected a str, got {type(value).__name__}")

        # 2. Reject empty strings.
        #    Note: the constructor does NOT apply strip(). If you need to trim
        #    whitespace, use NonEmptyStr.parse(value, strip=True) or explicitly
        #    call strip() before construction.
        if len(value) == 0:
            raise ValueError("Expected a non-empty string, got empty string")

        return super().__new__(cls, value)

    def __repr__(self) -> str:
        """
        Return a string representation that clearly identifies this as a NonEmptyStr.

        This is primarily useful for debugging and logging, where it's important
        to distinguish a validated NonEmptyStr from a plain str.

        Example:
            >>> repr(NonEmptyStr("abc"))
            'NonEmptyStr(\\'abc\\')'
            >>> str(NonEmptyStr("abc"))  # str() is unchanged, returns 'abc'
            'abc'

        Note: This intentionally differs from str.__repr__, which would return 'abc'.
        If compatibility with code that expects repr(str) is required, remove this method.
        """
        return f"NonEmptyStr({str.__repr__(self)})"

    def __str__(self) -> str:
        """
        Return the plain string representation.

        This is necessary because overriding __repr__ in a subclass of str
        can cause __str__ and __format__ (used by f-strings) to fall back
        to the overridden __repr__ in some Python implementations.
        By explicitly defining __str__, we ensure that str(), f-strings,
        and format() return the plain string value.

        Example:
            >>> str(NonEmptyStr("abc"))
            'abc'
            >>> f"{NonEmptyStr('abc')}"
            'abc'
            >>> format(NonEmptyStr("abc"))
            'abc'
        """
        return str.__str__(self)

    @classmethod
    def parse(cls, value: object, strip: bool = True) -> NonEmptyStr | None:
        """
        Safely attempt to convert a value to a NonEmptyStr.

        Args:
            value: The value to parse. Must be a str.
            strip: If True (default), applies strip() before validation.
                   Rejects strings that become empty after stripping.
                   If False, does not apply strip(). Rejects only truly empty strings.

        Returns:
            A NonEmptyStr instance on success, or None on any failure.
            This method never raises exceptions.

        See the class docstring for a full list of handled edge cases.
        """
        # 1. Reject non-str types explicitly.
        if not isinstance(value, str):
            return None

        # 2. Apply strip if requested.
        processed = value.strip() if strip else value

        # 3. Reject empty strings.
        if len(processed) == 0:
            return None

        # 4. Construct and return.
        return cls(processed)

class PositiveFloat(float):
    """
    A strictly positive float type (value > 0).
f
    This class is a thin wrapper around Python's built-in ``float`` that enforces
    the invariant "value > 0" at construction time. It is intended for use in
    configuration dataclasses and domain models where a positive float is
    semantically required.

    Construction modes
    ------------------
    1. Direct construction via ``PositiveFloat(value)``:
       - Accepts ONLY ``float`` and ``int`` values (not ``bool``, ``str``, etc.).
       - Raises ``TypeError`` for non-float/non-int types.
       - Raises ``ValueError`` for non-positive values, booleans, inf, or nan.
       - Use this mode when the caller is certain the value is a valid ``float``
         (e.g. after parsing/validation, or when working with already-validated data).

    2. Safe parsing via ``PositiveFloat.parse(value)``:
       - Accepts ``float``, ``int``, ``str`` (representations of positive numbers).
       - Returns a ``PositiveFloat`` instance on success, or ``None`` on any failure.
       - Never raises exceptions.
       - Use this mode when the input comes from untrusted sources
         (e.g. JSON configuration, user input).

    Important limitations
    ---------------------
    * ``PositiveFloat`` inherits from ``float``. Arithmetic operations
      (``+``, ``-``, ``*``, etc.) return a plain ``float``, NOT a ``PositiveFloat``.
      This means the "value > 0" invariant is NOT preserved across arithmetic.
      Example::

          x = PositiveFloat(5.0)
          y = PositiveFloat(3.0)
          z = x - y   # z == 2.0, but type(z) is float, not PositiveFloat
          w = y - x   # w == -2.0, type(w) is float, and the invariant is violated

      If you need to preserve the invariant, re-validate after arithmetic:
      ``PositiveFloat.parse(x - y)``.

    * ``isinstance(PositiveFloat(3.14), float)`` returns ``True``. This is intentional
      for compatibility with code that expects plain ``float`` values.

    * Type checkers (mypy, pyright) cannot statically verify the "value > 0"
      invariant. This class provides runtime validation only.

    Edge cases handled by ``parse()``
    ---------------------------------
    * ``bool`` values are explicitly rejected (Python's ``bool`` is a subclass
      of ``int``, so ``True`` would otherwise be accepted as ``1.0``).

    * ``bytes`` values are explicitly rejected. Although Python's ``float(b"3.14")``
      returns 3.14, bytes represent binary data, not string-encoded numbers.
      In a JSON-based pipeline, bytes should never appear as numeric input.

    * ``Decimal`` values are explicitly rejected. Decimal support is not required
      for this pipeline, and implicit Decimal -> float conversion would introduce
      precision loss.

    * ``float('inf')``, ``float('-inf')``, ``float('nan')`` return ``None``.

    * Negative values and zero return ``None``.

    * Non-parseable strings (``"abc"``, ``""``) return ``None``.

    Examples
    --------
    >>> PositiveFloat(3.14)
    PositiveFloat(3.14)
    >>> PositiveFloat(42)
    PositiveFloat(42.0)
    >>> PositiveFloat(0.0)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive float, got 0.0
    >>> PositiveFloat(-1.0)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive float, got -1.0
    >>> PositiveFloat(True)
    Traceback (most recent call last):
        ...
    ValueError: Expected a positive float, got True
    >>> PositiveFloat("3.14")
    Traceback (most recent call last):
        ...
    TypeError: Expected a float or int, got str
    >>> PositiveFloat.parse(3.14)
    PositiveFloat(3.14)
    >>> PositiveFloat.parse(42)
    PositiveFloat(42.0)
    >>> PositiveFloat.parse("3.14")
    PositiveFloat(3.14)
    >>> PositiveFloat.parse("42")
    PositiveFloat(42.0)
    >>> PositiveFloat.parse(0.0) is None
    True
    >>> PositiveFloat.parse(-1.0) is None
    True
    >>> PositiveFloat.parse(True) is None
    True
    >>> PositiveFloat.parse(float('inf')) is None
    True
    >>> PositiveFloat.parse(float('nan')) is None
    True
    >>> PositiveFloat.parse(None) is None
    True
    >>> PositiveFloat.parse("invalid") is None
    True
    >>> from decimal import Decimal
    >>> PositiveFloat.parse(Decimal("3.14")) is None
    True
    """

    def __new__(cls, value: float | int) -> PositiveFloat:
        # 1. Reject booleans explicitly (bool is a subclass of int in Python).
        #    This must be checked BEFORE isinstance(value, int), because
        #    isinstance(True, int) returns True.
        if isinstance(value, bool):
            raise ValueError(f"Expected a positive float, got {value!r}")

        # 2. Reject non-float/non-int types (str, None, bytes, Decimal, etc.).
        #    The constructor is strict: it accepts ONLY float and int values.
        #    For safe conversion from other types, use PositiveFloat.parse().
        if not isinstance(value, (float, int)):
            raise TypeError(
                f"Expected a float or int, got {type(value).__name__}"
            )

        # 3. Reject special float values (inf/nan).
        if isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
            raise ValueError(f"Expected a positive float, got {value!r}")

        # 4. Convert int to float for uniform handling.
        float_value = float(value)

        # 5. Reject non-positive values.
        if float_value <= 0:
            raise ValueError(f"Expected a positive float, got {float_value!r}")

        return super().__new__(cls, float_value)

    def __repr__(self) -> str:
        """
        Return a string representation that clearly identifies this as a PositiveFloat.

        Example:
            >>> repr(PositiveFloat(3.14))
            'PositiveFloat(3.14)'
            >>> str(PositiveFloat(3.14))
            '3.14'
        """
        return f"PositiveFloat({float(self)})"

    def __str__(self) -> str:
        """
        Return the plain numeric string representation.

        This is necessary because overriding __repr__ in a subclass of float
        can cause __str__ and __format__ (used by f-strings) to fall back
        to the overridden __repr__ in some Python implementations.

        Example:
            >>> str(PositiveFloat(3.14))
            '3.14'
            >>> f"{PositiveFloat(3.14)}"
            '3.14'
            >>> format(PositiveFloat(3.14))
            '3.14'
        """
        return str(float(self))

    @classmethod
    def parse(cls, value: object) -> PositiveFloat | None:
        """
        Safely attempt to convert a value to a PositiveFloat.

        Returns a PositiveFloat instance on success, or None on any failure.
        This method never raises exceptions.

        All parsing and validation logic is self-contained within this method.
        No external helper function (e.g. parse_float) is used, because:
        - There are no consumers outside PositiveFloat (YAGNI)
        - NonEmptyStr.parse() follows the same self-contained pattern
        - Single point of truth eliminates risk of divergence

        See the class docstring for a full list of handled edge cases.
        """
        # 1. Reject booleans explicitly (bool is a subclass of int in Python).
        if isinstance(value, bool):
            return None

        # 2. Reject bytes explicitly: float(b"3.14") works in Python, but bytes
        # represent binary data, not string-encoded numbers.
        if isinstance(value, bytes):
            return None

        # 3. Reject Decimal explicitly: Decimal support is not required for this
        # pipeline. Accepting Decimal would introduce implicit precision loss.
        if isinstance(value, Decimal):
            return None

        # 4. Handle float: reject special values (inf/nan) and non-positive.
        if isinstance(value, float):
            if math.isinf(value) or math.isnan(value):
                return None
            if value <= 0:
                return None
            return cls(value)

        # 5. Handle int: reject non-positive, convert to float.
        if isinstance(value, int):
            if value <= 0:
                return None
            return cls(float(value))

        # 6. Handle str: attempt conversion, reject special values and non-positive.
        if isinstance(value, str):
            try:
                result = float(value)
            except ValueError:
                return None
            if math.isinf(result) or math.isnan(result):
                return None
            if result <= 0:
                return None
            return cls(result)

        # 7. All other types (None, list, dict, etc.) are rejected.
        return None