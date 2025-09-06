"""
Textract utility functions for parsing results.
Includes Markdown conversion, extracting forms, tables, and text content.
"""

import os
from datetime import datetime
from pipeline_modules.aws_clients import s3_client, textract_client
from pipeline_modules.config import BUCKET_NAME, TEXTRACT_FEATURES, POLL_INTERVAL

import time


def run_textract(file_path):
    """
    Uploads file to S3, runs Textract analysis, and returns output.
    """
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


# ---------- Parsing Helpers ----------

def find_value_block(key_block, value_map):
    """Find the value block associated with a key block."""
    if "Relationships" in key_block:
        for relationship in key_block["Relationships"]:
            if relationship["Type"] == "VALUE":
                for value_id in relationship["Ids"]:
                    if value_id in value_map:
                        return value_map[value_id]
    return None


def get_text_from_block(block, blocks_map):
    """Extract text from Textract block."""
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


def get_text_from_cell(cell, blocks_map):
    """Extract text from a table cell."""
    text = ''
    if 'Relationships' in cell:
        for relationship in cell['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = blocks_map.get(child_id)
                    if child_block and child_block.get('BlockType') == 'WORD':
                        text += child_block.get('Text', '') + ' '
    return text.strip()


def extract_table_data(table_block, blocks_map):
    """Extract structured table data from Textract table block."""
    table = {
        'confidence': table_block.get('Confidence', 0),
        'rows': [],
        'row_count': 0,
        'column_count': 0
    }

    # Collect all cells
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
        row_data = [rows[row_index].get(col, '') for col in range(1, max_col + 1)]
        table['rows'].append(row_data)

    table['row_count'] = len(table['rows'])
    table['column_count'] = max_col
    return table


def textract_to_markdown(textract_output, filename):
    """
    Convert Textract output to a well-structured Markdown document.
    Includes forms, tables, and raw document text.
    """
    blocks_map = {block["Id"]: block for block in textract_output["Blocks"]}
    md = []

    # Header
    md.append(f"# Document Analysis Report: {filename}")
    md.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")

    # ----- Extract Forms -----
    forms = []
    key_map, value_map = {}, {}
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

    if forms:
        md.append("## Form Fields\n")
        md.append("\n| Field | Value | Confidence |")
        md.append("|-------|-------|------------|")
        for form in forms[:20]:
            md.append(f"| {form['key']} | {form['value']} | {form['confidence']:.1f}% |")
        if len(forms) > 20:
            md.append(f"\n*... and {len(forms) - 20} more fields*")

    # ----- Extract Tables -----
    tables = []
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "TABLE":
            table_data = extract_table_data(block, blocks_map)
            if table_data:
                tables.append(table_data)

    if tables:
        md.append("\n## Tables\n")
        for i, table in enumerate(tables, 1):
            md.append(f"\n### Table {i}")
            md.append(f"*Dimensions: {table['row_count']} rows × {table['column_count']} columns*")
            md.append(f"*Confidence: {table['confidence']:.1f}%*\n")
            rows = table['rows'][:10]
            if rows:
                header = rows[0]
                md.append("| " + " | ".join(str(c) for c in header) + " |")
                md.append("|" + "---|" * len(header))
                for row in rows[1:]:
                    md.append("| " + " | ".join(str(c) for c in row) + " |")
                if len(table['rows']) > 10:
                    md.append(f"\n*... and {len(table['rows']) - 10} more rows*")

    # ----- Extract Raw Lines -----
    lines = []
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "LINE":
            lines.append(block["Text"])
    if lines:
        md.append("\n## Document Content\n")
        for line in lines[:50]:
            md.append(line)
        if len(lines) > 50:
            md.append(f"\n*... and {len(lines) - 50} more lines*")

    return "\n".join(md)
