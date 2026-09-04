import math
from decimal import Decimal

import pytest

from shared.utils.parse import PositiveInt


class TestPositiveIntConstructor:
    """Tests for direct construction via PositiveInt(value)."""

    # --- Valid inputs ---

    def test_positive_int(self):
        result = PositiveInt(5)
        assert isinstance(result, PositiveInt)
        assert isinstance(result, int)
        assert int(result) == 5

    def test_large_positive_int(self):
        result = PositiveInt(10**18)
        assert int(result) == 10**18

    # --- Invalid: non-positive values ---

    def test_zero_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(0)

    def test_negative_int_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(-1)

    # --- Invalid: bool (subclass of int) ---

    def test_true_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(True)

    def test_false_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive integer"):
            PositiveInt(False)

    # --- Invalid: wrong types (TypeError) ---

    def test_float_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got float"):
            PositiveInt(5.0) # type: ignore[arg-type]

    def test_str_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got str"):
            PositiveInt("5") # type: ignore[arg-type]

    def test_none_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got NoneType"):
            PositiveInt(None) # type: ignore[arg-type]

    def test_bytes_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got bytes"):
            PositiveInt(b"42") # type: ignore[arg-type]

    def test_decimal_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got Decimal"):
            PositiveInt(Decimal("42")) # type: ignore[arg-type]

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got list"):
            PositiveInt([5]) # type: ignore[arg-type]

    def test_dict_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected an int, got dict"):
            PositiveInt({"value": 5}) # type: ignore[arg-type]


class TestPositiveIntParseSuccess:
    """Tests for PositiveInt.parse() with valid inputs."""

    def test_parse_positive_int(self):
        result = PositiveInt.parse(42)
        assert isinstance(result, PositiveInt)
        assert int(result) == 42

    def test_parse_whole_float(self):
        result = PositiveInt.parse(5.0)
        assert isinstance(result, PositiveInt)
        assert int(result) == 5

    def test_parse_positive_string_int(self):
        result = PositiveInt.parse("42")
        assert isinstance(result, PositiveInt)
        assert int(result) == 42

    def test_parse_positive_decimal_whole(self):
        result = PositiveInt.parse(Decimal("42"))
        assert isinstance(result, PositiveInt)
        assert int(result) == 42

    def test_parse_large_positive_int(self):
        result = PositiveInt.parse(10**18)
        assert isinstance(result, PositiveInt)
        assert int(result) == 10**18


class TestPositiveIntParseFailure:
    """Tests for PositiveInt.parse() with invalid inputs. All must return None."""

    # --- Non-positive values ---

    def test_parse_zero_int_returns_none(self):
        assert PositiveInt.parse(0) is None

    def test_parse_negative_int_returns_none(self):
        assert PositiveInt.parse(-1) is None

    def test_parse_zero_float_returns_none(self):
        assert PositiveInt.parse(0.0) is None

    def test_parse_negative_float_returns_none(self):
        assert PositiveInt.parse(-5.0) is None

    def test_parse_zero_string_returns_none(self):
        assert PositiveInt.parse("0") is None

    def test_parse_negative_string_returns_none(self):
        assert PositiveInt.parse("-5") is None

    def test_parse_zero_decimal_returns_none(self):
        assert PositiveInt.parse(Decimal("0")) is None

    def test_parse_negative_decimal_returns_none(self):
        assert PositiveInt.parse(Decimal("-5")) is None

    # --- Bool rejection ---

    def test_parse_true_returns_none(self):
        assert PositiveInt.parse(True) is None

    def test_parse_false_returns_none(self):
        assert PositiveInt.parse(False) is None

    # --- Bytes rejection ---

    def test_parse_bytes_returns_none(self):
        assert PositiveInt.parse(b"42") is None

    # --- Float edge cases ---

    def test_parse_fractional_float_returns_none(self):
        assert PositiveInt.parse(5.5) is None

    def test_parse_inf_returns_none(self):
        assert PositiveInt.parse(float("inf")) is None

    def test_parse_negative_inf_returns_none(self):
        assert PositiveInt.parse(float("-inf")) is None

    def test_parse_nan_returns_none(self):
        assert PositiveInt.parse(float("nan")) is None

    # --- Decimal edge cases ---

    def test_parse_fractional_decimal_returns_none(self):
        assert PositiveInt.parse(Decimal("5.5")) is None

    def test_parse_decimal_inf_returns_none(self):
        assert PositiveInt.parse(Decimal("inf")) is None

    def test_parse_decimal_nan_returns_none(self):
        assert PositiveInt.parse(Decimal("nan")) is None

    # --- String edge cases ---

    def test_parse_fractional_string_returns_none(self):
        """int('5.0') raises ValueError in Python — fractional strings are rejected."""
        assert PositiveInt.parse("5.0") is None

    def test_parse_empty_string_returns_none(self):
        assert PositiveInt.parse("") is None

    def test_parse_non_numeric_string_returns_none(self):
        assert PositiveInt.parse("abc") is None

    def test_parse_whitespace_only_string_returns_none(self):
        assert PositiveInt.parse("   ") is None

    # --- Unsupported types ---

    def test_parse_none_returns_none(self):
        assert PositiveInt.parse(None) is None

    def test_parse_list_returns_none(self):
        assert PositiveInt.parse([5]) is None

    def test_parse_dict_returns_none(self):
        assert PositiveInt.parse({"value": 5}) is None


class TestPositiveIntStringRepresentations:
    """Tests for __repr__, __str__, and format compatibility."""

    def test_repr_format(self):
        assert repr(PositiveInt(42)) == "PositiveInt(42)"

    def test_str_format(self):
        assert str(PositiveInt(42)) == "42"

    def test_fstring_uses_str(self):
        assert f"{PositiveInt(42)}" == "42"

    def test_format_uses_str(self):
        assert format(PositiveInt(42)) == "42"

    def test_repr_distinguishes_from_plain_int(self):
        assert repr(PositiveInt(42)) != repr(42)
        assert repr(PositiveInt(42)) == "PositiveInt(42)"
        assert repr(42) == "42"

    def test_str_matches_plain_int(self):
        assert str(PositiveInt(42)) == str(42)


class TestPositiveIntInheritanceAndArithmetic:
    """Tests for int inheritance and arithmetic behavior."""

    def test_isinstance_int(self):
        assert isinstance(PositiveInt(5), int)

    def test_equality_with_plain_int(self):
        assert PositiveInt(5) == 5

    def test_addition_returns_plain_int(self):
        result = PositiveInt(3) + 2
        assert type(result) is int
        assert not isinstance(result, PositiveInt)
        assert result == 5

    def test_subtraction_returns_plain_int(self):
        result = PositiveInt(5) - 3
        assert type(result) is int
        assert not isinstance(result, PositiveInt)
        assert result == 2

    def test_subtraction_can_violate_invariant(self):
        """Arithmetic does NOT preserve the > 0 invariant."""
        result = PositiveInt(3) - PositiveInt(5)
        assert result == -2
        assert type(result) is int

    def test_multiplication_returns_plain_int(self):
        result = PositiveInt(3) * 4
        assert type(result) is int
        assert result == 12

    def test_revalidate_after_arithmetic_success(self):
        """Demonstrates how to preserve invariant after arithmetic."""
        result = PositiveInt.parse(PositiveInt(5) - PositiveInt(3))
        assert isinstance(result, PositiveInt)
        assert int(result) == 2

    def test_revalidate_after_arithmetic_failure(self):
        """Re-validation catches invariant violation after arithmetic."""
        result = PositiveInt.parse(PositiveInt(3) - PositiveInt(5))
        assert result is None


