from decimal import Decimal

import pytest

from shared.utils.parse import PositiveFloat

class TestPositiveFloatConstructor:
    """Tests for direct construction via PositiveFloat(value)."""

    # --- Valid inputs ---

    def test_float_positive(self):
        result = PositiveFloat(3.14)
        assert isinstance(result, PositiveFloat)
        assert isinstance(result, float)
        assert result == 3.14

    def test_int_positive_converts_to_float(self):
        result = PositiveFloat(42)
        assert isinstance(result, PositiveFloat)
        assert result == 42.0

    def test_very_small_positive_float(self):
        result = PositiveFloat(0.0001)
        assert result == 0.0001

    def test_large_positive_float(self):
        result = PositiveFloat(1e308)
        assert result == 1e308

    # --- Invalid: non-positive values ---

    def test_zero_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(0.0)

    def test_negative_float_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(-1.0)

    def test_negative_int_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(-5)

    # --- Invalid: bool (subclass of int) ---

    def test_true_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(True)

    def test_false_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(False)

    # --- Invalid: special float values ---

    def test_inf_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(float("inf"))

    def test_negative_inf_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(float("-inf"))

    def test_nan_raises_value_error(self):
        with pytest.raises(ValueError, match="Expected a positive float"):
            PositiveFloat(float("nan"))

    # --- Invalid: wrong types (TypeError) ---

    def test_str_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got str"):
            PositiveFloat("3.14") # type: ignore[arg-type]

    def test_none_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got NoneType"):
            PositiveFloat(None) # type: ignore[arg-type]

    def test_bytes_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got bytes"):
            PositiveFloat(b"3.14") # type: ignore[arg-type]

    def test_decimal_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got Decimal"):
            PositiveFloat(Decimal("3.14")) # type: ignore[arg-type]

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got list"):
            PositiveFloat([3.14]) # type: ignore[arg-type]

    def test_dict_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected a float or int, got dict"):
            PositiveFloat({"value": 3.14}) # type: ignore[arg-type]

class TestPositiveFloatParseSuccess:
    """Tests for PositiveFloat.parse() with valid inputs."""

    def test_parse_positive_float(self):
        result = PositiveFloat.parse(3.14)
        assert isinstance(result, PositiveFloat)
        assert result == 3.14

    def test_parse_positive_int(self):
        result = PositiveFloat.parse(42)
        assert isinstance(result, PositiveFloat)
        assert result == 42.0

    def test_parse_positive_string_float(self):
        result = PositiveFloat.parse("3.14")
        assert isinstance(result, PositiveFloat)
        assert result == 3.14

    def test_parse_positive_string_int(self):
        result = PositiveFloat.parse("42")
        assert isinstance(result, PositiveFloat)
        assert result == 42.0

    def test_parse_string_with_whitespace(self):
        result = PositiveFloat.parse("  3.14  ")
        assert isinstance(result, PositiveFloat)
        assert result == 3.14

    def test_parse_scientific_notation_string(self):
        result = PositiveFloat.parse("1.5e2")
        assert isinstance(result, PositiveFloat)
        assert result == 150.0

    def test_parse_very_small_positive_float(self):
        result = PositiveFloat.parse(0.0001)
        assert isinstance(result, PositiveFloat)
        assert result == 0.0001

    def test_parse_large_positive_float(self):
        result = PositiveFloat.parse(1e308)
        assert isinstance(result, PositiveFloat)
        assert result == 1e308


class TestPositiveFloatParseFailure:
    """Tests for PositiveFloat.parse() with invalid inputs. All must return None."""

    # --- Non-positive values ---

    def test_parse_zero_float_returns_none(self):
        assert PositiveFloat.parse(0.0) is None

    def test_parse_zero_int_returns_none(self):
        assert PositiveFloat.parse(0) is None

    def test_parse_zero_string_returns_none(self):
        assert PositiveFloat.parse("0") is None

    def test_parse_negative_float_returns_none(self):
        assert PositiveFloat.parse(-1.0) is None

    def test_parse_negative_int_returns_none(self):
        assert PositiveFloat.parse(-5) is None

    def test_parse_negative_string_returns_none(self):
        assert PositiveFloat.parse("-3.14") is None

    # --- Bool rejection ---

    def test_parse_true_returns_none(self):
        assert PositiveFloat.parse(True) is None

    def test_parse_false_returns_none(self):
        assert PositiveFloat.parse(False) is None

    # --- Special float values ---

    def test_parse_inf_returns_none(self):
        assert PositiveFloat.parse(float("inf")) is None

    def test_parse_negative_inf_returns_none(self):
        assert PositiveFloat.parse(float("-inf")) is None

    def test_parse_nan_returns_none(self):
        assert PositiveFloat.parse(float("nan")) is None

    # --- String edge cases ---

    def test_parse_inf_string_returns_none(self):
        assert PositiveFloat.parse("inf") is None

    def test_parse_nan_string_returns_none(self):
        assert PositiveFloat.parse("nan") is None

    def test_parse_empty_string_returns_none(self):
        assert PositiveFloat.parse("") is None

    def test_parse_non_numeric_string_returns_none(self):
        assert PositiveFloat.parse("abc") is None

    def test_parse_whitespace_only_string_returns_none(self):
        assert PositiveFloat.parse("   ") is None

    # --- Unsupported types ---

    def test_parse_none_returns_none(self):
        assert PositiveFloat.parse(None) is None

    def test_parse_bytes_returns_none(self):
        assert PositiveFloat.parse(b"3.14") is None

    def test_parse_decimal_returns_none(self):
        assert PositiveFloat.parse(Decimal("3.14")) is None

    def test_parse_list_returns_none(self):
        assert PositiveFloat.parse([3.14]) is None

    def test_parse_dict_returns_none(self):
        assert PositiveFloat.parse({"value": 3.14}) is None

class TestPositiveFloatStringRepresentations:
    """Tests for __repr__, __str__, and format compatibility."""

    def test_repr_format(self):
        pf = PositiveFloat(3.14)
        assert repr(pf) == "PositiveFloat(3.14)"

    def test_str_format(self):
        pf = PositiveFloat(3.14)
        assert str(pf) == "3.14"

    def test_fstring_uses_str(self):
        pf = PositiveFloat(3.14)
        assert f"{pf}" == "3.14"

    def test_format_uses_str(self):
        pf = PositiveFloat(3.14)
        assert format(pf) == "3.14"

    def test_repr_distinguishes_from_plain_float(self):
        pf = PositiveFloat(42.0)
        plain = 42.0
        assert repr(pf) != repr(plain)
        assert repr(pf) == "PositiveFloat(42.0)"
        assert repr(plain) == "42.0"

    def test_str_matches_plain_float(self):
        pf = PositiveFloat(42.0)
        plain = 42.0
        assert str(pf) == str(plain)

    def test_repr_in_collections(self):
        """repr() is used when PositiveFloat is inside a collection."""
        x = PositiveFloat(3.14)
        y = PositiveFloat(42.0)
        assert repr([x, y]) == "[PositiveFloat(3.14), PositiveFloat(42.0)]"
        assert repr({"key": x}) == "{'key': PositiveFloat(3.14)}"

class TestPositiveFloatInheritanceAndArithmetic:
    """Tests for float inheritance and arithmetic behavior."""

    def test_isinstance_float(self):
        pf = PositiveFloat(3.14)
        assert isinstance(pf, float)

    def test_equality_with_plain_float(self):
        pf = PositiveFloat(3.14)
        assert pf == 3.14

    def test_addition_returns_plain_float(self):
        pf = PositiveFloat(3.0)
        result = pf + 1.0
        assert type(result) is float
        assert not isinstance(result, PositiveFloat)

    def test_subtraction_returns_plain_float(self):
        pf = PositiveFloat(5.0)
        result = pf - 3.0
        assert type(result) is float
        assert not isinstance(result, PositiveFloat)

    def test_subtraction_can_violate_invariant(self):
        """Arithmetic does NOT preserve the > 0 invariant."""
        pf = PositiveFloat(3.0)
        result = pf - 5.0
        assert result == -2.0
        assert type(result) is float

    def test_multiplication_returns_plain_float(self):
        pf = PositiveFloat(2.0)
        result = pf * 3.0
        assert type(result) is float
        assert result == 6.0

    def test_division_returns_plain_float(self):
        pf = PositiveFloat(6.0)
        result = pf / 2.0
        assert type(result) is float
        assert result == 3.0

    def test_revalidate_after_arithmetic(self):
        """Demonstrates how to preserve invariant after arithmetic."""
        pf = PositiveFloat(5.0)
        result = PositiveFloat.parse(pf - 3.0)
        assert isinstance(result, PositiveFloat)
        assert result == 2.0

    def test_revalidate_after_arithmetic_failure(self):
        """Re-validation catches invariant violation after arithmetic."""
        pf = PositiveFloat(3.0)
        result = PositiveFloat.parse(pf - 5.0)
        assert result is None
