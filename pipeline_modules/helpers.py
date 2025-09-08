"""
Helper utility functions for normalizing dates and currency values.
"""

from datetime import datetime

def normalize_date(date_str):
    """Convert various date formats to dd/mm/yyyy"""
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %B %Y", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return date_str  # fallback

def extract_currency_symbol(value):
    """Extract the first recognized currency symbol from a string."""
    if not isinstance(value, str):
        return None
    symbols = ['$', '€', '£', '¥', '₹','LKR','AUD']
    for symbol in symbols:
        if symbol in value:
            return symbol
    return None

def normalize_currency(value):
    """Normalize currency strings to float by removing symbols and formatting."""
    if not isinstance(value, str):
        return value

    # Remove currency symbols
    currency_symbols = ['$', '€', '£', '¥', '₹', 'LKR', 'AUD']
    cleaned = value
    for symbol in currency_symbols:
        cleaned = cleaned.replace(symbol, '')

    # Remove commas (thousand separators)
    cleaned = cleaned.replace(',', '').strip()

    try:
        return float(cleaned)
    except ValueError:
        return value  # return original if cannot parse
