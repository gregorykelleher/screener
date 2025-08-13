# feeds/feed_validators.py

from collections.abc import Callable
from typing import TypeVar

from pydantic import BaseModel, model_validator

T = TypeVar("T", bound=BaseModel)


def required(*fields: str) -> Callable[[T], T]:
    """
    Class decorator for Pydantic models to enforce that specified fields are
    required non-empty strings.

    Args:
        *fields (str): Names of fields to validate as required non-empty strings.

    Returns:
        Callable[[T], T]: Decorator that adds a model-level validator to ensure
        each specified field is present and not an empty string.

    Example:
        @required("name", "symbol")
        class YFinanceFeedData(BaseModel):
            ...
    """

    def decorator(cls: T) -> T:
        """
        Decorates a Pydantic model class to enforce that specified fields are
        required non-empty strings.

        Args:
            cls (T): The Pydantic model class to decorate.

        Returns:
            T: The decorated Pydantic model class with the required field
            validator applied.
        """

        @model_validator(mode="after")
        def _require_non_empty(self: T) -> T:
            """
            Ensures each specified field is a non-empty string after model
            initialisation.

            Args:
                self (T): The Pydantic model instance to validate.

            Returns:
                T: The validated Pydantic model instance.

            Raises:
                ValueError: If any required field is None or an empty string.
            """
            for field in fields:
                value = getattr(self, field)
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ValueError(f"{field} is required")
            return self

        return type(
            cls.__name__,
            (cls,),
            {f"_require_{'_'.join(fields)}": _require_non_empty},
        )

    return decorator
