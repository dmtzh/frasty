import pytest
from shared.action import ActionName


class TestActionNameConstructor:
    """Tests for the strict ActionName.__new__ constructor."""

    @pytest.mark.parametrize("valid_name", [
        "fetchdata",
        "get-content-from-html",
        "my_action-1",
        "a1_b2-c3",
        "x",
        "123",
        "___",
        "---",
    ])
    def test_constructor_accepts_valid_strings(self, valid_name: str):
        """Constructor should accept strings matching ^[a-z0-9_-]+$."""
        action_name = ActionName(valid_name)
        assert action_name == valid_name
        assert type(action_name) is ActionName

    @pytest.mark.parametrize("invalid_type", [
        123,
        12.34,
        None,
        b"bytes",
        ["list"],
        {"dict": "value"},
        True,
        False,
    ])
    def test_constructor_rejects_non_str_types(self, invalid_type):
        """Constructor must raise TypeError for any non-str input."""
        with pytest.raises(TypeError, match="Expected a str"):
            ActionName(invalid_type)

    @pytest.mark.parametrize("invalid_format", [
        "FetchData",          # Uppercase letters
        "my action",          # Space inside
        "action.name",        # Dot inside
        "action@name",        # Special character
        "action/name",        # Slash
        "",                   # Empty string
    ])
    def test_constructor_rejects_invalid_formats(self, invalid_format: str):
        """Constructor must raise ValueError for strings not matching ^[a-z0-9_-]+$."""
        with pytest.raises(ValueError, match="Expected a string matching"):
            ActionName(invalid_format)

    def test_constructor_does_not_normalize(self):
        """Constructor must NOT apply strip() or lowercase()."""
        # Spaces around should fail because constructor doesn't strip
        with pytest.raises(ValueError):
            ActionName(" fetchdata ")
        
        # Uppercase should fail because constructor doesn't lowercase
        with pytest.raises(ValueError):
            ActionName("FETCHDATA")


class TestActionNameParser:
    """Tests for the safe ActionName.parse() class method."""

    @pytest.mark.parametrize("raw_input, expected_value", [
        (" FetchData ", "fetchdata"),
        ("GET-CONTENT", "get-content"),
        ("  my_action-1  ", "my_action-1"),
        ("fetchdata", "fetchdata"),
        ("  UPPER_CASE  ", "upper_case"),
    ])
    def test_parser_normalizes_and_accepts(self, raw_input: str, expected_value: str):
        """Parser should strip, lowercase, and return ActionName if valid."""
        result = ActionName.parse(raw_input)
        assert result is not None
        assert result == expected_value
        assert type(result) is ActionName

    @pytest.mark.parametrize("invalid_input", [
        "my action",          # Space remains after strip/lower
        "action.name",        # Dot remains
        "action@name",        # Special char remains
        "",                   # Empty string
        "   ",                # Only whitespace (becomes empty after strip)
        123,                  # Wrong type
        None,                 # Wrong type
        b"bytes",             # Wrong type
        ["list"],             # Wrong type
    ])
    def test_parser_returns_none_for_invalid_input(self, invalid_input):
        """Parser should never raise exceptions, returning None instead."""
        result = ActionName.parse(invalid_input)
        assert result is None


class TestActionNameMagicMethods:
    """Tests for __repr__ and __str__ overrides."""

    def test_repr_includes_class_name(self):
        """repr() should clearly identify the object as ActionName."""
        name = ActionName("fetchdata")
        assert repr(name) == "ActionName('fetchdata')"

    def test_str_returns_plain_string(self):
        """str() should return the plain string value without wrapper."""
        name = ActionName("fetchdata")
        assert str(name) == "fetchdata"

    def test_f_string_returns_plain_string(self):
        """f-strings should use __str__ and return the plain value."""
        name = ActionName("get-content")
        assert f"{name}" == "get-content"
        assert f"Action is {name}" == "Action is get-content"


class TestActionNameInteroperability:
    """Tests for string inheritance and interoperability."""

    def test_isinstance_str(self):
        """ActionName should be recognized as a str for compatibility."""
        name = ActionName("test")
        assert isinstance(name, str)

    def test_can_be_used_as_dict_key(self):
        """ActionName should work seamlessly as a dictionary key."""
        name = ActionName("my_action")
        d: dict = {name: "value"}
        assert d["my_action"] == "value"
        assert d[name] == "value"

    def test_string_operations_return_plain_str(self):
        """String operations should return plain str (invariant not preserved)."""
        name1 = ActionName("action")
        name2 = ActionName("name")
        
        # Concatenation returns plain str
        concatenated = name1 + "_" + name2
        assert concatenated == "action_name"
        assert type(concatenated) is str
        assert type(concatenated) is not ActionName

    def test_equality_with_plain_str(self):
        """ActionName should be equal to a plain str with the same value."""
        name = ActionName("fetchdata")
        assert name == "fetchdata"
        assert "fetchdata" == name