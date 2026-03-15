"""
Data filtering utilities for Streamlit UI.
Converts UI filter selections to database query parameters.
"""

from typing import Optional


class DataFilters:
    """Helper class for converting UI filter values to database query parameters."""

    # ============================================================================
    # CORE FILTER CONVERTERS
    # ============================================================================

    @staticmethod
    def get_within_limit_filter(filter_value: str) -> Optional[bool]:
        """
        Convert within_limit filter to boolean or None.

        Args:
            filter_value: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All"
        """
        if filter_value == "Yes":
            return True
        elif filter_value == "No":
            return False
        return None

    @staticmethod
    def get_usage_filter(filter_value: str) -> Optional[bool]:
        """
        Convert used filter to boolean or None.

        Args:
            filter_value: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All"
        """
        if filter_value == "Yes":
            return True
        elif filter_value == "No":
            return False
        return None

    @staticmethod
    def get_filtered_filter(filter_value: str) -> Optional[bool]:
        """
        Convert filtered filter to boolean or None.

        Args:
            filter_value: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All"
        """
        if filter_value == "Yes":
            return True
        elif filter_value == "No":
            return False
        return None

    @staticmethod
    def get_executed_filter(filter_value: str) -> Optional[bool]:
        """
        Convert executed filter to boolean or None.

        Args:
            filter_value: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All"
        """
        if filter_value == "Yes":
            return True
        elif filter_value == "No":
            return False
        return None

    # ============================================================================
    # GENERIC FILTER CONVERTER
    # ============================================================================

    @staticmethod
    def get_boolean_filter(filter_option: str) -> Optional[bool]:
        """
        Convert Yes/No/All filter to boolean (generic version).

        This is a generic converter that can be used for any boolean filter.
        The specific methods above are kept for clarity and backward compatibility.

        Args:
            filter_option: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All" or any other value
        """
        if filter_option == "Yes":
            return True
        elif filter_option == "No":
            return False
        return None

    # ============================================================================
    # DEPRECATED METHODS (Keep for backward compatibility)
    # ============================================================================

    @staticmethod
    def get_processed_filter(filter_value: str) -> Optional[bool]:
        """
        DEPRECATED: Use get_filtered_filter instead.
        Convert processed_by_workflow filter to boolean or None.

        Args:
            filter_value: "Yes", "No", or "All"

        Returns:
            True for "Yes", False for "No", None for "All"
        """
        if filter_value == "Yes":
            return True
        elif filter_value == "No":
            return False
        return None

    # ============================================================================
    # UTILITY METHODS
    # ============================================================================

    @staticmethod
    def validate_filter_value(filter_value: str, allowed_values: list = None) -> bool:
        """
        Validate that a filter value is one of the allowed values.

        Args:
            filter_value: The filter value to validate
            allowed_values: List of allowed values (default: ["Yes", "No", "All"])

        Returns:
            True if valid, False otherwise
        """
        if allowed_values is None:
            allowed_values = ["Yes", "No", "All"]
        return filter_value in allowed_values

    @staticmethod
    def convert_bool_to_filter_string(bool_value: Optional[bool]) -> str:
        """
        Convert a boolean value back to filter string.
        Useful for setting initial filter states.

        Args:
            bool_value: True, False, or None

        Returns:
            "Yes" for True, "No" for False, "All" for None
        """
        if bool_value is True:
            return "Yes"
        elif bool_value is False:
            return "No"
        return "All"

    @staticmethod
    def get_active_filters(filter_dict: dict) -> dict:
        """
        Filter out None values from a filter dictionary.
        Returns only the active filters.

        Args:
            filter_dict: Dictionary of filter names to values

        Returns:
            Dictionary with only non-None values
        """
        return {k: v for k, v in filter_dict.items() if v is not None}

    @staticmethod
    def count_active_filters(filter_dict: dict) -> int:
        """
        Count the number of active filters (non-None values).

        Args:
            filter_dict: Dictionary of filter names to values

        Returns:
            Number of active filters
        """
        return len(DataFilters.get_active_filters(filter_dict))

    @staticmethod
    def format_filter_summary(filter_dict: dict) -> str:
        """
        Format filter dictionary into a human-readable summary.

        Args:
            filter_dict: Dictionary of filter names to values

        Returns:
            Formatted string like "within_limit=Yes, used=No"
        """
        active = DataFilters.get_active_filters(filter_dict)
        if not active:
            return "No filters active"

        parts = []
        for key, value in active.items():
            display_value = DataFilters.convert_bool_to_filter_string(value)
            parts.append(f"{key}={display_value}")

        return ", ".join(parts)


# ============================================================================
# MODULE-LEVEL CONVENIENCE FUNCTIONS
# ============================================================================

def convert_filter(filter_value: str) -> Optional[bool]:
    """
    Convenience function for filter conversion.
    Alias for DataFilters.get_boolean_filter().

    Args:
        filter_value: "Yes", "No", or "All"

    Returns:
        True for "Yes", False for "No", None for "All"
    """
    return DataFilters.get_boolean_filter(filter_value)


def validate_filter(filter_value: str) -> bool:
    """
    Convenience function for filter validation.
    Alias for DataFilters.validate_filter_value().

    Args:
        filter_value: The filter value to validate

    Returns:
        True if valid, False otherwise
    """
    return DataFilters.validate_filter_value(filter_value)
