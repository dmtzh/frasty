from shared.utils.parse import parse_dict_field, NonEmptyStr, PositiveInt
from shared.validation import ValueMissing, ValueInvalid


class TestParseDictFieldSuccess:
    """Tests for successful parsing scenarios."""

    def test_existing_key_with_valid_value(self):
        data = {"name": "alice"}
        result = parse_dict_field(data, "name", NonEmptyStr.parse)
        assert result.is_ok()
        assert result.ok == "alice"

    def test_positive_int_parser(self):
        data = {"count": 42}
        result = parse_dict_field(data, "count", PositiveInt.parse)
        assert result.is_ok()
        assert result.ok == 42

    def test_string_representation_of_number(self):
        data = {"count": "42"}
        result = parse_dict_field(data, "count", PositiveInt.parse)
        assert result.is_ok()
        assert result.ok == 42

    def test_returns_correct_type(self):
        data = {"name": "test"}
        result = parse_dict_field(data, "name", lambda v: v if type(v) is str else None)
        assert isinstance(result.ok, str)


class TestParseDictFieldMissingKey:
    """Tests for missing key scenarios → ValueMissing."""

    def test_missing_key_returns_value_missing(self):
        data = {"other": "value"}
        result = parse_dict_field(data, "name", NonEmptyStr.parse)
        assert result.is_error()
        err = result.error
        assert isinstance(err, ValueMissing)
        assert err.name == "name"

    def test_empty_dict_returns_value_missing(self):
        result = parse_dict_field({}, "key", NonEmptyStr.parse)
        assert result.is_error()
        assert isinstance(result.error, ValueMissing)
        assert result.error.name == "key"

    def test_none_value_is_not_missing(self):
        """Key exists but value is None → should be ValueInvalid, not ValueMissing."""
        data = {"name": None}
        result = parse_dict_field(data, "name", lambda v: v)
        assert result.is_error()
        assert isinstance(result.error, ValueInvalid)
        assert result.error.name == "name"
        assert result.error.value is None


class TestParseDictFieldInvalidValue:
    """Tests for invalid value scenarios → ValueInvalid with raw value."""

    def test_empty_string_for_non_empty_str(self):
        data = {"name": ""}
        result = parse_dict_field(data, "name", NonEmptyStr.parse)
        assert result.is_error()
        err = result.error
        assert isinstance(err, ValueInvalid)
        assert err.name == "name"
        assert err.value == ""

    def test_negative_int_for_positive_int(self):
        data = {"count": -1}
        result = parse_dict_field(data, "count", PositiveInt.parse)
        assert result.is_error()
        err = result.error
        assert isinstance(err, ValueInvalid)
        assert err.name == "count"
        assert err.value == -1


class TestParseDictFieldEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_key_with_special_characters(self):
        data = {"my-field.name": "value"}
        result = parse_dict_field(data, "my-field.name", NonEmptyStr.parse)
        assert result.is_ok()
        assert result.ok == "value"

    def test_numeric_string_key(self):
        data = {"123": "value"}
        result = parse_dict_field(data, "123", NonEmptyStr.parse)
        assert result.is_ok()
        assert result.ok == "value"

    def test_empty_string_key(self):
        data = {"": "value"}
        result = parse_dict_field(data, "", NonEmptyStr.parse)
        assert result.is_ok()
        assert result.ok == "value"

    def test_custom_parser_returning_none(self):
        """Custom parser that always returns None → ValueInvalid."""
        data = {"key": "any_value"}
        result = parse_dict_field(data, "key", lambda _: None)
        assert result.is_error()
        err = result.error
        assert isinstance(err, ValueInvalid)
        assert err.name == "key"
        assert err.value == "any_value"

    def test_custom_parser_returning_value(self):
        """Custom parser that transforms value."""
        data = {"key": "hello"}
        result = parse_dict_field(data, "key", lambda v: v.upper() if isinstance(v, str) else None)
        assert result.is_ok()
        assert result.ok == "HELLO"

    def test_value_invalid_preserves_original_value_type(self):
        """Ensure raw value is stored as-is without conversion."""
        data = {"count": {"nested": "dict"}}
        result = parse_dict_field(data, "count", PositiveInt.parse)
        assert result.is_error()
        err = result.error
        assert isinstance(err, ValueInvalid)
        assert err.value == {"nested": "dict"}
