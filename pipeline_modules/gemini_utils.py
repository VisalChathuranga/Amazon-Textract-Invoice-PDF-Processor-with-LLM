"""
Gemini (LLM) utility functions.
Handles Markdown → JSON structured invoice extraction.
"""

import json
import logging
from google import genai
from google.genai.types import GenerateContentConfig
from pipeline_modules.config import GEMINI_API_KEY
from pipeline_modules.helpers import normalize_date, normalize_currency, extract_currency_symbol

# Gemini client
gemini = genai.Client(api_key=GEMINI_API_KEY)


# ========== GEMINI JSON WITH ENHANCED PROMPT ==========
def md_to_json(md_text, pdf_filename):
    prompt = f"""
You are an expert invoice processing system. Extract the following fields from this invoice:

REQUIRED FIELDS:
1. Invoice Number (look for labels like "Invoice No", "Invoice Number", "INV-")
2. Invoice Date (look for "Invoice Date", "Date", in various formats)
3. Line Items - extract each item with:
   - description (what was sold/provided)
   - quantity (how many)
   - unit_price (price per unit) with currency symbol if present
   - amount (total for this line) with currency symbol if present
4. Invoice Total (this is the FINAL payable amount).
   - Look for labels like "Invoice Total", "Grand Total", "Total Amount", "Total Due", "Amount Due", "Balance Due".
   - IGNORE values labeled as "Subtotal", "Discount", "Fee", "Tax", "Adjusted Total", or similar.
   - If multiple totals exist, prefer the one at the BOTTOM or the one labeled as payable.
   - If still ambiguous, choose the HIGHEST value, but only if it is clearly marked as the final amount.
   - Always include the currency symbol if present.

5. Payment Terms
   - Look for keywords such as "Payment Terms", "Terms", "Net 30", "Net 15", "Due in 30 days", "Due upon receipt", "Payment due".
   - If payment terms are explicitly mentioned, extract the full text explaining the terms.
   - If no payment terms are explicitly mentioned, return null.

IMPORTANT INSTRUCTIONS:
- If a field is not found, use null or empty string
- For line items, extract ALL items you can find in table format
- For amounts, always include the currency symbol if present
- For dates, standardize to YYYY-MM-DD format if possible
- The invoice might be in different languages (English, German, etc.)

Respond with ONLY valid JSON in this exact format:
{{
  "invoice_number": "extracted value or null",
  "invoice_date": "extracted value or null",
  "invoice_total": "total amount (with currency)",
  "currency": "currency symbol",
  "line_items": [
    {{
      "description": "item description",
      "quantity": "quantity",
      "unit_price": "price per unit (with currency)",
      "amount": "line total (with currency)"
    }}
  ],  
  "payment_terms": "payment terms or null",
  "extraction_confidence": "high/medium/low",
  "source_file": "{pdf_filename}"
}}

INVOICE CONTENT:
{md_text}
"""
    response = gemini.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt,
        config=GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
        ),
    )

    try:
        data = json.loads(response.text)

        # Normalize invoice_date
        if "invoice_date" in data:
            data["invoice_date"] = normalize_date(data["invoice_date"])

        # Extract currency symbol
        currency_symbol = None
        if "invoice_total" in data and isinstance(data["invoice_total"], str):
            currency_symbol = extract_currency_symbol(data["invoice_total"])
        if not currency_symbol and "line_items" in data:
            for item in data["line_items"]:
                if isinstance(item.get("unit_price"), str):
                    currency_symbol = extract_currency_symbol(item["unit_price"])
                    if currency_symbol:
                        break
        data["currency"] = currency_symbol if currency_symbol else None

        # Convert invoice_total to float
        if "invoice_total" in data:
            data["invoice_total"] = normalize_currency(data["invoice_total"])

        # Convert line_items fields to float
        if "line_items" in data and isinstance(data["line_items"], list):
            for item in data["line_items"]:
                if "quantity" in item:
                    try:
                        item["quantity"] = float(item["quantity"])
                    except (ValueError, TypeError):
                        item["quantity"] = item["quantity"]
                if "unit_price" in item:
                    item["unit_price"] = normalize_currency(item["unit_price"])
                if "amount" in item:
                    item["amount"] = normalize_currency(item["amount"])

        return data
    except Exception as e:
        logging.error(f"Failed to parse JSON: {e}")
        logging.error("Raw response: %s", response.text[:500])
        return {}

