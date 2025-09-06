"""
Main invoice processing pipeline module.
Coordinates Textract → Markdown → Gemini JSON → File saving.
"""

import os
import json
from pipeline_modules.textract_utils import run_textract, textract_to_markdown
from pipeline_modules.gemini_utils import md_to_json
from pipeline_modules.config import RESULTS_DIR, MARKDOWN_DIR
from pipeline_modules.logging_config import logger


def process_invoice(file_path):
    """
    Full pipeline for processing a single invoice:
    1. Run Textract on PDF
    2. Convert Textract output → Markdown
    3. Save Markdown file
    4. Send Markdown to Gemini → JSON structured data
    5. Save JSON file
    """
    textract_output = run_textract(file_path)

    # Convert to Markdown
    md_text = textract_to_markdown(textract_output, os.path.basename(file_path))
    md_file = os.path.join(MARKDOWN_DIR, os.path.basename(file_path).replace(".pdf", ".md"))
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md_text)

    # Extract JSON using Gemini
    extracted_json = md_to_json(md_text, os.path.basename(file_path))
    out_file = os.path.join(RESULTS_DIR, os.path.basename(file_path).replace(".pdf", ".json"))
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(extracted_json, f, indent=4, ensure_ascii=False)


    # Logging
    logger.info(f"Processed {file_path}")
    logger.info(f"   -> Markdown: {md_file}")
    logger.info(f"   -> JSON: {out_file}")
