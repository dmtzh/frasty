from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ValueMissing:
    """
    Validation error indicating that a required field is absent from the input.

    The `name` attribute uses dotted path notation (e.g., "config.threshold",
    "input_data.product.id") to represent hierarchical field paths from
    the validation root to the specific missing field.

    Use `with_prefix(prefix)` to prepend a prefix to the field name, joining
    with a dot separator. The prefix typically represents a parent context
    in the validation hierarchy (e.g., a nested config object, an indexed
    element in a list).

    Immutability:
        - Instances are frozen (immutable). `with_prefix` returns a new instance.
        - Safe to share across validation contexts.

    Examples:
        >>> ValueMissing("threshold").with_prefix("config")
        ValueMissing(name='config.threshold')
        >>> ValueMissing("id").with_prefix("input_data.product")
        ValueMissing(name='input_data.product.id')
    """

    name: str

    def with_prefix(self, prefix: str) -> 'ValueMissing':
        """
        Prepend a prefix to this error's field name, joined by a dot.

        The resulting name follows dotted path notation: "{prefix}.{self.name}".
        The dot separator is fixed and not configurable — this is a standard
        convention for hierarchical names in Python (cf. logging, JMESPath,
        JSONPath, Python imports).

        The `prefix` typically represents a parent context in the validation
        hierarchy: a parent field name (e.g., "config"), an indexed element
        (e.g., "input_data[3]"), or a composite path (e.g., "input_data.product").

        Args:
            prefix: The string to prepend to the field name. This becomes
                    the parent portion of the resulting dotted path.

        Returns:
            A new ValueMissing instance with name formatted as "{prefix}.{self.name}".

        Examples:
            >>> ValueMissing("threshold").with_prefix("config")
            ValueMissing(name='config.threshold')
            >>> ValueMissing("id").with_prefix("input_data[3]")
            ValueMissing(name='input_data[3].id')
            >>> ValueMissing("id").with_prefix("input_data.product")
            ValueMissing(name='input_data.product.id')

        Notes:
            - The original instance is not modified (frozen dataclass).
            - No validation is performed on `prefix`; it is used as-is.
            - If `prefix` is empty, the result will be ".{self.name}" — this
              is the caller's responsibility to avoid.
        """
        return ValueMissing(name=f"{prefix}.{self.name}")


@dataclass(frozen=True)
class ValueInvalid:
    """
    Validation error indicating that a field is present but has an invalid value.

    The `name` attribute uses dotted path notation (e.g., "config.threshold",
    "input_data.product.id") to represent hierarchical field paths from
    the validation root to the specific invalid field.

    The optional `value` attribute preserves the original invalid value for
    debugging and error reporting. It defaults to None for backward compatibility
    with code that constructs ValueInvalid without providing the value.

    Use `with_prefix(prefix)` to prepend a prefix to the field name, joining
    with a dot separator. The prefix typically represents a parent context
    in the validation hierarchy.

    Immutability:
        - Instances are frozen (immutable). `with_prefix` returns a new instance.
        - Safe to share across validation contexts.

    Examples:
        >>> ValueInvalid("threshold", -1).with_prefix("config")
        ValueInvalid(name='config.threshold', value=-1)
    """

    name: str
    value: Any = None  # optional, backward-compatible

    def with_prefix(self, prefix: str) -> 'ValueInvalid':
        """
        Prepend a prefix to this error's field name, joined by a dot.

        The resulting name follows dotted path notation: "{prefix}.{self.name}".
        The dot separator is fixed and not configurable — this is a standard
        convention for hierarchical names in Python (cf. logging, JMESPath,
        JSONPath, Python imports).

        The `value` attribute is preserved unchanged in the new instance.

        The `prefix` typically represents a parent context in the validation
        hierarchy: a parent field name (e.g., "config"), an indexed element
        (e.g., "input_data[3]"), or a composite path (e.g., "input_data.product").

        Args:
            prefix: The string to prepend to the field name. This becomes
                    the parent portion of the resulting dotted path.

        Returns:
            A new ValueInvalid instance with name formatted as "{prefix}.{self.name}"
            and the same `value` as the original.

        Examples:
            >>> ValueInvalid("threshold", -1).with_prefix("config")
            ValueInvalid(name='config.threshold', value=-1)
            >>> ValueInvalid("id", None).with_prefix("input_data[3]")
            ValueInvalid(name='input_data[3].id', value=None)
            >>> ValueInvalid("id", "abc").with_prefix("input_data.product")
            ValueInvalid(name='input_data.product.id', value='abc')

        Notes:
            - The original instance is not modified (frozen dataclass).
            - No validation is performed on `prefix`; it is used as-is.
            - If `prefix` is empty, the result will be ".{self.name}" — this
              is the caller's responsibility to avoid.
        """
        return ValueInvalid(name=f"{prefix}.{self.name}", value=self.value)


# Type alias for the union of validation errors.
type ValueError = ValueInvalid | ValueMissing


def with_prefix(prefix: str, errs: tuple[ValueError, ...]) -> tuple[ValueError, ...]:
    """
    Apply `with_prefix(prefix)` to every error in a tuple, returning a new tuple
    with all errors prefixed by the given prefix.

    This is a batch operation for nested validation: when validating a nested
    structure (e.g., a list of items, a nested config object), errors from
    inner validations must be prefixed with the parent context to form a
    complete dotted path.

    The dot separator is fixed and not configurable — this is a standard
    convention for hierarchical names in Python (cf. logging, JMESPath,
    JSONPath, Python imports).

    Args:
        prefix: The string to prepend to each error's field name. Typically
                represents a parent context in the validation hierarchy:
                a parent field name (e.g., "config"), an indexed element
                (e.g., "input_data[3]"), or a composite path (e.g., "input_data.product").
        errs: Tuple of validation errors (ValueMissing or ValueInvalid)
              to be prefixed. May be empty.

    Returns:
        A new tuple containing the same errors, each with `prefix` prepended
        to its `name` attribute via a dot separator. The order of errors is
        preserved. If `errs` is empty, returns an empty tuple.

    Examples:
        >>> errs = (ValueMissing("threshold"), ValueInvalid("count", -1))
        >>> with_prefix("config", errs)
        (ValueMissing(name='config.threshold'), ValueInvalid(name='config.count', value=-1))
        >>> with_prefix("input_data[3]", (ValueMissing("id"),))
        (ValueMissing(name='input_data[3].id'),)
        >>> with_prefix("config", ())
        ()

    Notes:
        - The original tuple and its errors are not modified (frozen dataclasses).
        - No validation is performed on `prefix`; it is used as-is.
        - If `prefix` is empty, all error names will start with "." — this
          is the caller's responsibility to avoid.
        - This function is typically used in `map_error` chains:
            validate_nested(data).map_error(lambda errs: with_prefix("config", errs))
    """
    return tuple(err.with_prefix(prefix) for err in errs)


@dataclass(frozen=True)
class InvalidId:
    """
    Validation error indicating that an identifier value is invalid.

    This is a distinct error type for ID-related validation failures,
    separate from general field validation (ValueMissing / ValueInvalid).
    Used in contexts where ID format, uniqueness, or referential integrity
    must be enforced (e.g., Crockford Base32 IDs with checksums).

    Immutability:
        - Instances are frozen (immutable).
        - Safe to share across validation contexts.
    """
    pass