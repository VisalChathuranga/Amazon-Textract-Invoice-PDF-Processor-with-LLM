"""
Configuration and environment setup module.
Handles API keys, AWS region, constants, and folder creation.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Gemini API key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY_2")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found. Please set it in .env file.")

# AWS Configuration
AWS_REGION = "us-east-1"
BUCKET_NAME = "visal-invoice-processing-bucket-2"

# Directory Paths
INVOICE_DIR = "invoices"
RESULTS_DIR = "pipeline_results"
MARKDOWN_DIR = "markdowns"

# Textract Configuration
POLL_INTERVAL = 5
TEXTRACT_FEATURES = ["TABLES", "FORMS"]

# Ensure local result folders exist
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(MARKDOWN_DIR, exist_ok=True)
