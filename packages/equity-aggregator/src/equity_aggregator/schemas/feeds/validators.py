# feeds/validators.py

from collections.abc import Callable
from typing import TypeVar

from pydantic import BaseModel, FieldValidationInfo, field_validator

T = TypeVar("T", bound=BaseModel)


def required(*fields: str) -> Callable[[T], T]:
    """
    Class decorator to enforce that each field in `fields` is a non-empty string.

    Args:
        *fields (str): Field names to validate as required non-empty strings.

    Returns:
        Callable[[type[T]], type[T]]: Decorator for Pydantic model classes.

    Example:
        @required("name", "symbol")
        class YFinanceFeedData(BaseModel):
            ...
    """

    def decorator(cls: T) -> T:
        """
        Adds a Pydantic field validator to ensure specified fields are non-empty
        strings.

        Args:
            cls (type[T]): The Pydantic model class to decorate.

        Returns:
            type[T]: The decorated Pydantic model class with the validator applied.
        """

        @field_validator(*fields)
        @classmethod
        def _require_non_empty(
            _cls: T,
            value: object,
            info: FieldValidationInfo,
        ) -> object:
            """
            Validator to ensure the field value is a non-empty string.

            Args:
            cls: The Pydantic model class.
            value: The value of the field being validated.
            info: FieldValidationInfo containing field metadata.

            Returns:
            object: The validated value if non-empty.

            Raises:
            ValueError: If the value is None or an empty string.
            """
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"{info.field_name} is required")
            return value

        setattr(cls, f"_require_{'_'.join(fields)}", _require_non_empty)
        return cls

    return decorator
