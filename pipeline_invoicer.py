import os
import time
import boto3
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from google.genai.types import GenerateContentConfig

# ========== LOAD ENV ==========
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY_2")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found. Please set it in .env file.")

# ========== CONFIG ==========
AWS_REGION = "us-east-1"
BUCKET_NAME = "visal-invoice-processing-bucket-2"
INVOICE_DIR = "invoices"
RESULTS_DIR = "pipeline_results"
MARKDOWN_DIR = "markdowns"
POLL_INTERVAL = 5
TEXTRACT_FEATURES = ["TABLES", "FORMS"]

# Ensure folders exist
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(MARKDOWN_DIR, exist_ok=True)

# AWS clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)

# Gemini client
gemini = genai.Client(api_key=GEMINI_API_KEY)

# ========== LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

# ========== HELPERS ==========
def normalize_date(date_str):
    """Convert any date format to dd/mm/yyyy"""
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
    symbols = ['$', '€', '£', '¥', '₹']
    for symbol in symbols:
        if symbol in value:
            return symbol
    return None

def normalize_currency(value):
    """Normalize currency strings to float values by removing symbols and formatting."""
    if not isinstance(value, str):
        return value

    # Remove currency symbols
    currency_symbols = ['$', '€', '£', '¥', '₹']
    cleaned = value
    for symbol in currency_symbols:
        cleaned = cleaned.replace(symbol, '')

    # Remove commas (thousand separators)
    cleaned = cleaned.replace(',', '').strip()

    try:
        return float(cleaned)
    except ValueError:
        # If can't parse, return original
        return value

# ========== IMPROVED TEXTRACT → MARKDOWN ==========
def textract_to_markdown(textract_output, filename):
    """Convert Textract output to well-structured markdown"""
    blocks_map = {block["Id"]: block for block in textract_output["Blocks"]}
    md = []
    
    # Header
    md.append(f"# Document Analysis Report: {filename}")
    md.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
    
    # Extract forms data
    forms = []
    key_map = {}
    value_map = {}
    
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "KEY_VALUE_SET":
            if "KEY" in block.get("EntityTypes", []):
                key_map[block["Id"]] = block
            else:
                value_map[block["Id"]] = block
    
    for key_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key_text = get_text_from_block(key_block, blocks_map)
        value_text = get_text_from_block(value_block, blocks_map) if value_block else ""
        
        if key_text:
            forms.append({
                'key': key_text,
                'value': value_text,
                'confidence': key_block.get('Confidence', 0)
            })
    
    # Add forms section
    if forms:
        md.append("## Form Fields\n")
        md.append("\n| Field | Value | Confidence |")
        md.append("|-------|-------|------------|")
        for form in forms[:20]:  # Limit to 20 fields for readability
            key = form.get('key', '').replace('|', '\\|')
            value = form.get('value', '').replace('|', '\\|')
            confidence = form.get('confidence', 0)
            md.append(f"| {key} | {value} | {confidence:.1f}% |")
        
        if len(forms) > 20:
            md.append(f"\n*... and {len(forms) - 20} more fields*")
    
    # Extract tables
    tables = []
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "TABLE":
            table_data = extract_table_data(block, blocks_map)
            if table_data:
                tables.append(table_data)
    
    # Add tables section
    if tables:
        md.append("\n## Tables\n")
        for i, table in enumerate(tables, 1):
            md.append(f"\n### Table {i}")
            md.append(f"*Dimensions: {table['row_count']} rows × {table['column_count']} columns*")
            md.append(f"*Confidence: {table['confidence']:.1f}%*\n")
            
            if table.get('rows'):
                rows = table['rows'][:10]
                if rows:
                    header = rows[0] if rows else []
                    md.append("| " + " | ".join(str(cell).replace('|', '\\|') for cell in header) + " |")
                    md.append("|" + "---|" * len(header))

                    for row in rows[1:]:
                        md.append("| " + " | ".join(str(cell).replace('|', '\\|') for cell in row) + " |")

                    if len(table['rows']) > 10:
                        md.append(f"\n*... and {len(table['rows']) - 10} more rows*")
    
    # Extract lines
    lines = []
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "LINE":
            is_form_or_table = False
            if "Relationships" in block:
                for relationship in block["Relationships"]:
                    if relationship["Type"] == "CHILD":
                        for child_id in relationship["Ids"]:
                            child_block = blocks_map.get(child_id, {})
                            if child_block.get("BlockType") in ["KEY_VALUE_SET", "TABLE"]:
                                is_form_or_table = True
                                break
            if not is_form_or_table:
                lines.append(block["Text"])
    
    if lines:
        md.append("\n## Document Content\n")
        for line in lines[:50]:
            md.append(line)
        
        if len(lines) > 50:
            md.append(f"\n*... and {len(lines) - 50} more lines*")
    
    return "\n".join(md)

def find_value_block(key_block, value_map):
    if "Relationships" in key_block:
        for relationship in key_block["Relationships"]:
            if relationship["Type"] == "VALUE":
                for value_id in relationship["Ids"]:
                    if value_id in value_map:
                        return value_map[value_id]
    return None

def get_text_from_block(block, blocks_map):
    if not block:
        return ""
    if "Text" in block:
        return block["Text"]
    text = ""
    if "Relationships" in block:
        for relationship in block["Relationships"]:
            if relationship["Type"] == "CHILD":
                for child_id in relationship["Ids"]:
                    child_block = blocks_map.get(child_id)
                    if child_block and child_block.get("BlockType") == "WORD":
                        text += child_block.get("Text", "") + " "
    return text.strip()

def extract_table_data(table_block, blocks_map):
    table = {
        'confidence': table_block.get('Confidence', 0),
        'rows': [],
        'row_count': 0,
        'column_count': 0
    }
    cells = []
    if 'Relationships' in table_block:
        for relationship in table_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    cell_block = blocks_map.get(child_id)
                    if cell_block and cell_block.get('BlockType') == 'CELL':
                        cells.append(cell_block)
    rows = {}
    max_col = 0
    for cell in cells:
        row_index = cell.get('RowIndex', 1)
        col_index = cell.get('ColumnIndex', 1)
        if row_index not in rows:
            rows[row_index] = {}
        cell_text = get_text_from_cell(cell, blocks_map)
        rows[row_index][col_index] = cell_text
        max_col = max(max_col, col_index)
    for row_index in sorted(rows.keys()):
        row_data = []
        for col_index in range(1, max_col + 1):
            row_data.append(rows[row_index].get(col_index, ''))
        table['rows'].append(row_data)
    table['row_count'] = len(table['rows'])
    table['column_count'] = max_col
    return table

def get_text_from_cell(cell, blocks_map):
    text = ''
    if 'Relationships' in cell:
        for relationship in cell['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = blocks_map.get(child_id)
                    if child_block and child_block.get('BlockType') == 'WORD':
                        text += child_block.get('Text', '') + ' '
    return text.strip()

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

        # Normalize dates
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

        # Normalize total
        if "invoice_total" in data:
            data["invoice_total"] = normalize_currency(data["invoice_total"])

        # Normalize line items
        if "line_items" in data and isinstance(data["line_items"], list):
            for item in data["line_items"]:
                if "quantity" in item:
                    if isinstance(item["quantity"], str):
                        try:
                            item["quantity"] = float(item["quantity"])
                        except ValueError:
                            pass
                if "unit_price" in item:
                    item["unit_price"] = normalize_currency(item["unit_price"])
                if "amount" in item:
                    item["amount"] = normalize_currency(item["amount"])
        return data
    except Exception as e:
        logging.error(f"Failed to parse JSON: {e}")
        logging.error("Raw response: %s", response.text[:500])
        return {}

# ========== PROCESS INVOICE ==========
def process_invoice(file_path):
    textract_output = run_textract(file_path)
    md_text = textract_to_markdown(textract_output, os.path.basename(file_path))
    md_file = os.path.join(MARKDOWN_DIR, os.path.basename(file_path).replace(".pdf", ".md"))
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md_text)

    extracted_json = md_to_json(md_text, os.path.basename(file_path))
    out_file = os.path.join(RESULTS_DIR, os.path.basename(file_path).replace(".pdf", ".json"))
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(extracted_json, f, indent=4, ensure_ascii=False)

    logging.info(f"Processed {file_path}")
    logging.info(f"   -> Markdown: {md_file}")
    logging.info(f"   -> JSON: {out_file}")

# ========== RUN TEXTRACT ==========
def run_textract(file_path):
    file_name = os.path.basename(file_path)
    s3_client.upload_file(file_path, BUCKET_NAME, file_name)
    response = textract_client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": BUCKET_NAME, "Name": file_name}},
        FeatureTypes=TEXTRACT_FEATURES,
    )
    job_id = response["JobId"]
    while True:
        result = textract_client.get_document_analysis(JobId=job_id)
        if result["JobStatus"] in ["SUCCEEDED", "FAILED"]:
            break
        time.sleep(POLL_INTERVAL)
    if result["JobStatus"] == "SUCCEEDED":
        return result
    else:
        raise Exception("Textract job failed.")

# ========== MAIN ==========
if __name__ == "__main__":
    for file in os.listdir(INVOICE_DIR):
        if file.lower().endswith(".pdf"):
            process_invoice(os.path.join(INVOICE_DIR, file))
