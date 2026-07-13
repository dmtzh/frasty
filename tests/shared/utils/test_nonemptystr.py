"""
Unit tests for NonEmptyStr class.

These tests cover:
  - Direct construction via NonEmptyStr(value)
  - Safe parsing via NonEmptyStr.parse(value, strip)
  - __repr__ and __str__ behavior
  - String operations (documented limitation: return plain str)
  - Integration with built-in str operations
  - Boundary and edge cases (Unicode, long strings, special characters)
  - Consistency with the existing parse_non_empty_str function
"""
from __future__ import annotations

import json

import pytest

from shared.utils.parse import NonEmptyStr, parse_non_empty_str


# ---------------------------------------------------------------------------
# Direct construction tests
# ---------------------------------------------------------------------------
class TestNonEmptyStrDirectConstruction:
    """Tests for NonEmptyStr(value) constructor."""

    @pytest.mark.parametrize(
        "value",
        [
            "abc",
            "a",
            "  abc  ",  # Constructor does NOT strip — spaces are preserved
            "   ",      # Whitespace-only string is NOT empty, so it's accepted
            "\t\n",
            "👍",
            "\x00\x01",  # Non-printable characters are not whitespace
        ],
    )
    def test_valid_strings(self, value):
        result = NonEmptyStr(value)
        assert result == value
        assert isinstance(result, NonEmptyStr)
        assert isinstance(result, str)

    def test_constructor_raises_for_empty_string(self):
        with pytest.raises(ValueError, match="Expected a non-empty string"):
            NonEmptyStr("")

    @pytest.mark.parametrize(
        "value",
        [
            123,
            5.5,
            True,
            False,
            None,
            b"abc",
            [1, 2, 3],
            {"a": 1},
            object(),
        ],
    )
    def test_constructor_raises_for_non_str(self, value):
        """Constructor is strict: accepts ONLY str values."""
        with pytest.raises(TypeError, match="Expected a str"):
            NonEmptyStr(value)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Safe parsing tests
# ---------------------------------------------------------------------------
class TestNonEmptyStrParse:
    """Tests for NonEmptyStr.parse(value, strip) safe conversion."""

    # --- Successful conversions ---
    @pytest.mark.parametrize(
        "value, strip, expected",
        [
            ("abc", True, "abc"),
            ("abc", False, "abc"),
            ("  abc  ", True, "abc"),
            ("  abc  ", False, "  abc  "),
            ("\t\nabc\n\t", True, "abc"),
            ("\t\nabc\n\t", False, "\t\nabc\n\t"),
            ("a", True, "a"),
        ],
    )
    def test_valid_inputs(self, value, strip, expected):
        result = NonEmptyStr.parse(value, strip=strip)
        assert result is not None
        assert result == expected
        assert isinstance(result, NonEmptyStr)

    # --- Empty string is always rejected ---
    @pytest.mark.parametrize("strip", [True, False])
    def test_empty_string_rejected(self, strip):
        assert NonEmptyStr.parse("", strip=strip) is None

    # --- Whitespace-only strings ---
    @pytest.mark.parametrize(
        "value",
        [
            "   ",
            "\t",
            "\n",
            "\r\n",
            "\t\n\r ",
            "\u00A0",        # Non-breaking space
            "\u3000",        # Ideographic space
            "\u2000",        # En quad
            "\u2001",        # Em quad
            "\u2002",        # En space
            "\u2003",        # Em space
            "\u200A",        # Hair space
        ],
    )
    def test_whitespace_only_rejected_with_strip_true(self, value):
        """With strip=True (default), whitespace-only strings are rejected."""
        assert NonEmptyStr.parse(value, strip=True) is None

    @pytest.mark.parametrize(
        "value",
        [
            "   ",
            "\t",
            "\n",
            "\u00A0",
            "\u3000",
        ],
    )
    def test_whitespace_only_accepted_with_strip_false(self, value):
        """With strip=False, whitespace-only strings are accepted as non-empty."""
        result = NonEmptyStr.parse(value, strip=False)
        assert result is not None
        assert result == value

    # --- Non-str types are rejected ---
    @pytest.mark.parametrize(
        "value",
        [
            123,
            5.5,
            True,
            False,
            None,
            b"abc",      # bytes are explicitly rejected
            b"",
            [1, 2, 3],
            {"a": 1},
            object(),
        ],
    )
    def test_non_str_types_rejected(self, value):
        assert NonEmptyStr.parse(value) is None
        assert NonEmptyStr.parse(value, strip=False) is None

    # --- Default strip=True behavior ---
    def test_default_strip_is_true(self):
        """parse() without explicit strip arg should behave as strip=True."""
        assert NonEmptyStr.parse("  abc  ") == "abc"
        assert NonEmptyStr.parse("   ") is None

    # --- parse() never raises exceptions ---
    @pytest.mark.parametrize(
        "value",
        [
            float("inf"),
            float("nan"),
            None,
            object(),
            [1, 2, 3],
            {"a": 1},
            b"abc",
            123,
            True,
        ],
    )
    def test_parse_never_raises(self, value):
        """parse() must return None for any invalid input, never raise."""
        result = NonEmptyStr.parse(value)
        assert result is None


# ---------------------------------------------------------------------------
# __repr__ and __str__ tests
# ---------------------------------------------------------------------------
class TestNonEmptyStrRepr:
    """Tests for __repr__ and __str__ behavior."""

    def test_repr_format(self):
        x = NonEmptyStr("abc")
        assert repr(x) == "NonEmptyStr('abc')"

    def test_repr_with_quotes(self):
        x = NonEmptyStr("it's")
        # Python's str.__repr__ handles quote escaping
        assert "NonEmptyStr(" in repr(x)
        assert "it" in repr(x)

    def test_repr_with_special_chars(self):
        x = NonEmptyStr("a\nb")
        r = repr(x)
        assert r.startswith("NonEmptyStr(")
        # \\n should be escaped in repr
        assert "\\n" in r

    def test_str_returns_plain_value(self):
        x = NonEmptyStr("abc")
        assert str(x) == "abc"

    def test_fstring_uses_str(self):
        """Regression test: f-strings must use __str__, not __repr__."""
        x = NonEmptyStr("abc")
        assert f"{x}" == "abc"
        assert f"value: {x}" == "value: abc"

    def test_format_uses_str(self):
        x = NonEmptyStr("abc")
        assert format(x) == "abc"

    def test_repr_is_eval_compatible(self):
        """repr(x) should be eval-able back to an equivalent value."""
        x = NonEmptyStr("abc")
        reconstructed = eval(repr(x))  # noqa: S307
        assert reconstructed == x
        assert isinstance(reconstructed, NonEmptyStr)

    def test_repr_in_collections(self):
        """repr() is used when NonEmptyStr is inside a collection."""
        x = NonEmptyStr("a")
        y = NonEmptyStr("b")
        assert repr([x, y]) == "[NonEmptyStr('a'), NonEmptyStr('b')]"
        assert repr({"key": x}) == "{'key': NonEmptyStr('a')}"

    def test_json_serialization_uses_plain_str(self):
        """JSON serialization must produce plain string, not repr output."""
        x = NonEmptyStr("value")
        serialized = json.dumps({"key": x})
        assert serialized == '{"key": "value"}'
        assert "NonEmptyStr" not in serialized


# ---------------------------------------------------------------------------
# String operations (documented limitation)
# ---------------------------------------------------------------------------
class TestNonEmptyStrArithmeticBehavior:
    """
    Tests documenting the known limitation: string operations on NonEmptyStr
    return a plain str, NOT a NonEmptyStr. The "len > 0" invariant is NOT
    preserved across operations.
    """

    def test_concatenation_returns_str(self):
        x = NonEmptyStr("abc")
        y = NonEmptyStr("def")
        result = x + y
        assert result == "abcdef"
        assert type(result) is str
        assert not isinstance(result, NonEmptyStr)

    def test_multiplication_by_zero_violates_invariant(self):
        x = NonEmptyStr("abc")
        result = x * 0
        assert result == ""
        assert type(result) is str
        # The invariant "len > 0" is violated, but type is still plain str

    def test_multiplication_by_positive_returns_str(self):
        x = NonEmptyStr("a")
        result = x * 3
        assert result == "aaa"
        assert type(result) is str

    def test_slicing_returns_str(self):
        x = NonEmptyStr("abc")
        result = x[1:]
        assert result == "bc"
        assert type(result) is str

    def test_empty_slice_violates_invariant(self):
        x = NonEmptyStr("abc")
        result = x[0:0]
        assert result == ""
        assert type(result) is str

    def test_replace_returns_str(self):
        x = NonEmptyStr("abc")
        result = x.replace("a", "x")
        assert result == "xbc"
        assert type(result) is str

    def test_upper_returns_str(self):
        x = NonEmptyStr("abc")
        result = x.upper()
        assert result == "ABC"
        assert type(result) is str

    def test_lower_returns_str(self):
        x = NonEmptyStr("ABC")
        result = x.lower()
        assert result == "abc"
        assert type(result) is str

    def test_split_returns_list_of_str(self):
        x = NonEmptyStr("a,b,c")
        result = x.split(",")
        assert result == ["a", "b", "c"]
        assert isinstance(result, list)
        assert all(type(item) is str for item in result)


# ---------------------------------------------------------------------------
# Integration with built-in str operations
# ---------------------------------------------------------------------------
class TestNonEmptyStrIntegration:
    """Tests verifying NonEmptyStr behaves as str in standard operations."""

    def test_isinstance_str(self):
        x = NonEmptyStr("abc")
        assert isinstance(x, str)

    def test_equality_with_str(self):
        x = NonEmptyStr("abc")
        assert x == "abc"
        assert "abc" == x

    def test_comparison_with_str(self):
        x = NonEmptyStr("abc")
        assert x > "abb"
        assert x < "abd"
        assert x >= "abc"
        assert x <= "abc"

    def test_hash_compatible_with_str(self):
        x = NonEmptyStr("abc")
        assert hash(x) == hash("abc")
        # Can be used as dict key interchangeably with str
        d = {"abc": "value"}
        assert d[x] == "value"

    def test_used_as_dict_key(self):
        x = NonEmptyStr("key")
        d: dict = {x: "value"}
        assert d["key"] == "value"
        assert d[x] == "value"

    def test_used_in_set(self):
        x = NonEmptyStr("a")
        y = NonEmptyStr("b")
        s = {x, y, "a"}  # "a" duplicates x
        assert len(s) == 2

    def test_bool_truthiness(self):
        # NonEmptyStr is always non-empty, so always truthy
        assert bool(NonEmptyStr("a")) is True
        assert bool(NonEmptyStr("   ")) is True

    def test_concatenation_with_plain_str(self):
        x = NonEmptyStr("a")
        assert x + "b" == "ab"
        assert "b" + x == "ba"

    def test_in_operator(self):
        x = NonEmptyStr("abc")
        assert "a" in x
        assert "bc" in x
        assert "d" not in x

    def test_indexing(self):
        x = NonEmptyStr("abc")
        assert x[0] == "a"
        assert x[-1] == "c"
        # Indexing returns plain str
        assert type(x[0]) is str

    def test_iteration(self):
        x = NonEmptyStr("abc")
        assert list(x) == ["a", "b", "c"]

    def test_len(self):
        x = NonEmptyStr("abc")
        assert len(x) == 3

    def test_str_methods_work(self):
        x = NonEmptyStr("  abc  ")
        # Methods inherited from str work correctly
        assert x.strip() == "abc"
        assert x.startswith("  ")
        assert x.endswith("  ")
        assert x.count("a") == 1


# ---------------------------------------------------------------------------
# Boundary and edge cases
# ---------------------------------------------------------------------------
class TestNonEmptyStrBoundary:
    """Boundary and stress tests."""

    def test_very_long_string(self):
        long_str = "a" * 10**6
        result = NonEmptyStr.parse(long_str)
        assert result is not None
        assert result == long_str
        assert len(result) == 10**6

    def test_single_character(self):
        """Minimum valid string."""
        result = NonEmptyStr.parse("a")
        assert result is not None
        assert result == "a"

    def test_unicode_emoji(self):
        result = NonEmptyStr.parse("👍")
        assert result is not None
        assert result == "👍"

    def test_unicode_composite_emoji(self):
        """Composite emoji (family) is a single grapheme but multiple code points."""
        family = "👨‍👩‍👧"
        result = NonEmptyStr.parse(family)
        assert result is not None
        assert result == family

    def test_bom_character(self):
        """BOM (Byte Order Mark) is not whitespace, so it's preserved by strip()."""
        result = NonEmptyStr.parse("\ufeffabc")
        assert result is not None
        assert result == "\ufeffabc"

    def test_bom_only_string(self):
        """BOM alone is not whitespace, so it's accepted as non-empty."""
        result = NonEmptyStr.parse("\ufeff")
        assert result is not None
        assert result == "\ufeff"

    def test_null_character(self):
        """Null character is not whitespace."""
        result = NonEmptyStr.parse("\x00")
        assert result is not None
        assert result == "\x00"

    def test_multiline_string(self):
        result = NonEmptyStr.parse("a\nb\nc")
        assert result is not None
        assert result == "a\nb\nc"

    def test_internal_whitespace_preserved_with_strip(self):
        """strip() removes leading/trailing whitespace but preserves internal."""
        result = NonEmptyStr.parse("  a  b  ", strip=True)
        assert result is not None
        assert result == "a  b"

    def test_tabs_and_newlines_preserved_internally(self):
        result = NonEmptyStr.parse("\ta\tb\n", strip=True)
        assert result is not None
        assert result == "a\tb"

    def test_constructor_preserves_whitespace_exactly(self):
        """Constructor does NOT strip, preserves value exactly."""
        value = "  abc  "
        result = NonEmptyStr(value)
        assert result == value
        assert result.startswith("  ")
        assert result.endswith("  ")


# ---------------------------------------------------------------------------
# Consistency with parse_non_empty_str
# ---------------------------------------------------------------------------
class TestNonEmptyStrConsistencyWithParseNonEmptyStr:
    """
    Guarantee that NonEmptyStr.parse has IDENTICAL semantics to the existing
    parse_non_empty_str function. This is critical for backward compatibility
    during gradual migration of the codebase.
    """

    @pytest.mark.parametrize(
        "value, strip",
        [
            ("abc", True),
            ("abc", False),
            ("  abc  ", True),
            ("  abc  ", False),
            ("", True),
            ("", False),
            ("   ", True),
            ("   ", False),
            ("\t\n", True),
            ("\t\n", False),
            ("\u00A0", True),
            ("\u00A0", False),
            ("a", True),
            ("a", False),
        ],
    )
    def test_behavior_matches_for_str_inputs(self, value, strip):
        """For all str inputs, both functions must agree on accept/reject and value."""
        legacy_result = parse_non_empty_str(value, strip=strip)
        new_result = NonEmptyStr.parse(value, strip=strip)

        if legacy_result is None:
            assert new_result is None
        else:
            assert new_result is not None
            assert new_result == legacy_result
            assert isinstance(new_result, NonEmptyStr)

    @pytest.mark.parametrize(
        "value",
        [
            123,
            5.5,
            True,
            False,
            None,
            b"abc",
            [1, 2, 3],
            {"a": 1},
            object(),
        ],
    )
    def test_behavior_matches_for_non_str_inputs(self, value):
        """For all non-str inputs, both functions must return None."""
        legacy_result = parse_non_empty_str(value)
        new_result = NonEmptyStr.parse(value)
        assert legacy_result is None
        assert new_result is None

    def test_default_strip_parameter_matches(self):
        """Default strip parameter must be True for both functions."""
        legacy_result = parse_non_empty_str("  abc  ")
        new_result = NonEmptyStr.parse("  abc  ")
        assert legacy_result == "abc"
        assert new_result == "abc"
