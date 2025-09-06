"""
Main entry point for the Textract + Gemini Invoice Processing Pipeline.
Iterates through all PDF invoices in INVOICE_DIR and processes them.
"""

import os
from pipeline_modules.invoice_processor import process_invoice
from pipeline_modules.config import INVOICE_DIR


if __name__ == "__main__":
    for file in os.listdir(INVOICE_DIR):
        if file.lower().endswith(".pdf"):
            process_invoice(os.path.join(INVOICE_DIR, file))
