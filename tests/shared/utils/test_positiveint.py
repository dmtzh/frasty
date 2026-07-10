"""
Unit tests for PositiveInt class and parse_int helper function.

These tests cover:
  - Direct construction via PositiveInt(value)
  - Safe parsing via PositiveInt.parse(value)
  - Edge cases: bool, float, Decimal, inf, nan, very large numbers
  - Arithmetic behavior (documented limitation)
  - Integration with built-in int operations
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from shared.utils.parse import PositiveInt, parse_int

# ---------------------------------------------------------------------------
# parse_int() tests
# ---------------------------------------------------------------------------
class TestParseInt:
    """Tests for the parse_int helper function."""

    # --- Successful conversions ---
    @pytest.mark.parametrize(
        "value, expected",
        [
            (0, 0),
            (1, 1),
            (-5, -5),
            (10**18, 10**18),
            ("42", 42),
            ("-7", -7),
            ("0", 0),
            (5.0, 5),
            (-3.0, -3),
            (0.0, 0),
            (Decimal("10"), 10),
            (Decimal("-5"), -5),
            (Decimal("0"), 0),
            (Decimal("5.0"), 5),
        ],
    )
    def test_valid_inputs(self, value, expected):
        assert parse_int(value) == expected

    # --- Fractional floats and strings are rejected ---
    @pytest.mark.parametrize(
        "value",
        [
            5.5,
            -3.14,
            0.1,
            "5.0",
            "5.5",
            "-3.14",
            Decimal("5.5"),
            Decimal("-3.14"),
            Decimal("0.1"),
        ],
    )
    def test_fractional_values_rejected(self, value):
        assert parse_int(value) is None

    # --- Special float values ---
    @pytest.mark.parametrize(
        "value",
        [
            float("inf"),
            float("-inf"),
            float("nan"),
        ],
    )
    def test_special_float_values_rejected(self, value):
        """OverflowError from int(float('inf')) must be caught and return None."""
        assert parse_int(value) is None

    # --- Special Decimal values ---
    @pytest.mark.parametrize(
        "value",
        [
            Decimal("inf"),
            Decimal("-inf"),
            Decimal("nan"),
            Decimal("sNaN"),
        ],
    )
    def test_special_decimal_values_rejected(self, value):
        assert parse_int(value) is None

    # --- Unsupported types ---
    @pytest.mark.parametrize(
        "value",
        [
            None,
            True,
            False,
            [1, 2, 3],
            {"a": 1},
            object(),
        ],
    )
    def test_unsupported_types_rejected(self, value):
        assert parse_int(value) is None

    # --- Invalid strings ---
    @pytest.mark.parametrize(
        "value",
        [
            "abc",
            "",
            "  ",
            "42abc",
            "12.34.56",
        ],
    )
    def test_invalid_strings_rejected(self, value):
        assert parse_int(value) is None

    # --- Bytes are rejected ---
    @pytest.mark.parametrize(
        "value",
        [
            b"42",
            b"-7",
            b"0",
            b"5.0",
            b"abc",
            b"",
        ],
    )
    def test_bytes_rejected(self, value):
        """
        bytes are explicitly rejected even though int(b"42") works in Python.
        Bytes represent binary data, not string-encoded numbers.
        """
        assert parse_int(value) is None


# ---------------------------------------------------------------------------
# PositiveInt direct construction tests
# ---------------------------------------------------------------------------
class TestPositiveIntDirectConstruction:
    """Tests for PositiveInt(value) constructor."""

    @pytest.mark.parametrize("value", [1, 5, 100, 10**18])
    def test_valid_positive_int(self, value):
        result = PositiveInt(value)
        assert result == value
        assert isinstance(result, PositiveInt)
        assert isinstance(result, int)

    def test_constructor_raises_for_zero(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(0)

    def test_constructor_raises_for_negative(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(-1)
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(-100)

    def test_constructor_raises_for_bool(self):
        """bool is a subclass of int in Python, must be explicitly rejected."""
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(True)
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(False)

    def test_constructor_raises_for_non_int(self):
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt("5")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(5.5)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(None)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(b"42")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(Decimal("5"))  # type: ignore[arg-type]

    def test_constructor_rejects_fractional_floats(self):
        """
        Regression test: previously PositiveInt(5.5) silently truncated to PositiveInt(5).
        Now it must raise TypeError to prevent silent data loss.
        """
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(5.5)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(0.5)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected an int"):
            PositiveInt(-3.14)  # type: ignore[arg-type]

# ---------------------------------------------------------------------------
# PositiveInt.parse() tests
# ---------------------------------------------------------------------------
class TestPositiveIntParse:
    """Tests for PositiveInt.parse(value) safe conversion."""

    # --- Successful conversions ---
    @pytest.mark.parametrize(
        "value, expected",
        [
            (1, 1),
            (5, 5),
            (100, 100),
            (10**18, 10**18),
            ("42", 42),
            ("1", 1),
            (5.0, 5),
            (1.0, 1),
            (Decimal("10"), 10),
            (Decimal("5.0"), 5),
        ],
    )
    def test_valid_inputs(self, value, expected):
        result = PositiveInt.parse(value)
        assert result is not None
        assert result == expected
        assert isinstance(result, PositiveInt)

    # --- Zero and negative values ---
    @pytest.mark.parametrize("value", [0, -1, -100, "0", "-5", 0.0, -3.0, Decimal("0"), Decimal("-5")])
    def test_zero_and_negative_rejected(self, value):
        assert PositiveInt.parse(value) is None

    # --- Fractional values are rejected ---
    @pytest.mark.parametrize("value", [5.5, -3.14, 0.1, "5.0", "5.5", Decimal("5.5"), Decimal("3.14")])
    def test_fractional_values_rejected(self, value):
        assert PositiveInt.parse(value) is None

    # --- Special float values (inf, nan) ---
    @pytest.mark.parametrize(
        "value",
        [
            float("inf"),
            float("-inf"),
            float("nan"),
        ],
    )
    def test_special_float_values_rejected(self, value):
        """OverflowError from int(float('inf')) must be caught by parse()."""
        assert PositiveInt.parse(value) is None

    # --- Special Decimal values ---
    @pytest.mark.parametrize(
        "value",
        [
            Decimal("inf"),
            Decimal("-inf"),
            Decimal("nan"),
            Decimal("sNaN"),
        ],
    )
    def test_special_decimal_values_rejected(self, value):
        assert PositiveInt.parse(value) is None

    # --- Unsupported types ---
    @pytest.mark.parametrize(
        "value",
        [
            None,
            True,
            False,
            [1, 2, 3],
            {"a": 1},
            object(),
        ],
    )
    def test_unsupported_types_rejected(self, value):
        assert PositiveInt.parse(value) is None

    # --- Invalid strings ---
    @pytest.mark.parametrize("value", ["abc", "", "  ", "42abc", "0", "-5"])
    def test_invalid_strings_rejected(self, value):
        assert PositiveInt.parse(value) is None

    # --- Bytes are rejected ---
    @pytest.mark.parametrize(
        "value",
        [
            b"42",
            b"-7",
            b"0",
            b"5.0",
            b"abc",
            b"",
        ],
    )
    def test_bytes_rejected(self, value):
        """
        bytes are explicitly rejected even though int(b"42") works in Python.
        Bytes represent binary data, not string-encoded numbers.
        """
        assert PositiveInt.parse(value) is None

    # --- parse() never raises exceptions ---
    @pytest.mark.parametrize(
        "value",
        [
            float("inf"),
            float("nan"),
            Decimal("inf"),
            Decimal("nan"),
            None,
            object(),
            [1, 2, 3],
        ],
    )
    def test_parse_never_raises(self, value):
        """parse() must return None for any invalid input, never raise."""
        result = PositiveInt.parse(value)
        assert result is None


# ---------------------------------------------------------------------------
# Arithmetic behavior tests (documented limitation)
# ---------------------------------------------------------------------------
class TestPositiveIntArithmeticBehavior:
    """
    Tests documenting the known limitation: arithmetic operations on PositiveInt
    return a plain int, NOT a PositiveInt. The "value > 0" invariant is NOT
    preserved across arithmetic.
    """

    def test_addition_returns_int(self):
        x = PositiveInt(5)
        y = PositiveInt(3)
        result = x + y
        assert result == 8
        # Addition of two positive ints is always positive, but type is plain int
        assert type(result) is int
        assert not isinstance(result, PositiveInt)

    def test_subtraction_can_violate_invariant(self):
        x = PositiveInt(3)
        y = PositiveInt(5)
        result = x - y
        assert result == -2
        # The invariant "value > 0" is violated, but type is still plain int
        assert type(result) is int

    def test_multiplication_returns_int(self):
        x = PositiveInt(5)
        y = PositiveInt(3)
        result = x * y
        assert result == 15
        assert type(result) is int

    def test_division_returns_float(self):
        x = PositiveInt(10)
        result = x / 2
        assert result == 5.0
        assert isinstance(result, float)

    def test_floor_division_returns_int(self):
        x = PositiveInt(10)
        result = x // 3
        assert result == 3
        assert type(result) is int


# ---------------------------------------------------------------------------
# Integration with built-in int operations
# ---------------------------------------------------------------------------
class TestPositiveIntIntegration:
    """Tests verifying PositiveInt behaves as int in standard operations."""

    def test_isinstance_int(self):
        x = PositiveInt(5)
        assert isinstance(x, int)

    def test_equality_with_int(self):
        x = PositiveInt(5)
        assert x == 5
        assert 5 == x

    def test_comparison_with_int(self):
        x = PositiveInt(5)
        assert x > 3
        assert x < 10
        assert x >= 5
        assert x <= 5

    def test_hash_compatible_with_int(self):
        x = PositiveInt(5)
        assert hash(x) == hash(5)
        # Can be used as dict key interchangeably with int
        d = {5: "five"}
        assert d[x] == "five"

    def test_used_in_arithmetic_expressions(self):
        x = PositiveInt(5)
        assert x + 3 == 8
        assert 3 + x == 8
        assert x * 2 == 10
        assert x**2 == 25

    def test_str_and_repr(self):
        x = PositiveInt(42)
        # str() returns plain numeric string (explicitly defined)
        assert str(x) == "42"
        # repr() clearly identifies the type for debugging
        assert repr(x) == "PositiveInt(42)"
        # f-string uses __str__ (explicitly defined)
        assert f"{x}" == "42"
        # format() uses __str__
        assert format(x) == "42"
        # repr can be eval'd back to the same value (if class is in scope)
        assert eval(repr(x)) == x

    def test_repr_in_collections(self):
        """repr() is used when PositiveInt is inside a collection."""
        x = PositiveInt(5)
        y = PositiveInt(10)
        assert repr([x, y]) == "[PositiveInt(5), PositiveInt(10)]"
        assert repr({"key": x}) == "{'key': PositiveInt(5)}"

    def test_bool_truthiness(self):
        # PositiveInt is always > 0, so always truthy
        assert bool(PositiveInt(1)) is True
        assert bool(PositiveInt(100)) is True

    def test_bit_operations(self):
        x = PositiveInt(5)  # binary: 101
        y = PositiveInt(3)  # binary: 011
        assert x & y == 1  # binary: 001
        assert x | y == 7  # binary: 111
        assert x ^ y == 6  # binary: 110

    def test_can_be_index(self):
        x = PositiveInt(2)
        lst = ["a", "b", "c", "d"]
        assert lst[x] == "c"

    def test_can_be_range_argument(self):
        x = PositiveInt(5)
        r = range(x)
        assert list(r) == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Boundary and stress tests
# ---------------------------------------------------------------------------
class TestPositiveIntBoundary:
    """Boundary and stress tests."""

    def test_very_large_int(self):
        large = 10**100
        result = PositiveInt.parse(large)
        assert result is not None
        assert result == large

    def test_very_large_string(self):
        large_str = str(10**100)
        result = PositiveInt.parse(large_str)
        assert result is not None
        assert result == 10**100

    def test_minimum_valid_value(self):
        result = PositiveInt.parse(1)
        assert result is not None
        assert result == 1

    def test_string_with_leading_zeros(self):
        result = PositiveInt.parse("007")
        assert result is not None
        assert result == 7

    def test_string_with_whitespace(self):
        """int() strips whitespace from strings."""
        result = PositiveInt.parse("  42  ")
        assert result is not None
        assert result == 42

    def test_decimal_with_trailing_zeros(self):
        result = PositiveInt.parse(Decimal("5.000"))
        assert result is not None
        assert result == 5
