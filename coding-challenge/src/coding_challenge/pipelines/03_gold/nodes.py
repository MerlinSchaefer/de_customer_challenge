from typing import Dict, List

def check_sales_not_empty(
    raw_sales: dict,
    parameters: dict,
) -> str:
    """
    Checks each new sales file for emptiness and logs the filename if empty.

    Args:
        raw_sales: Dict mapping filenames to DataFrames.
        parameters: Dict of parameters.

    Returns:
        A string listing empty filenames, or a message if skipped.
    """
    if not parameters.get("check_empty_sales_log", True):
        return ""
    empty_files = []
    for filename, df in raw_sales.items():
        if hasattr(df, "empty") and df.empty:
            empty_files.append(filename)

    if empty_files:
        # Return filenames separated by newlines for appending to the log file
        return "\n".join(empty_files) + "\n"
    else:
        return ""