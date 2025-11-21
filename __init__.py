# Expose key modules at package level:
from extract import extract_customers, extract_payments
from transform import transform_pipeline
from load import load_to_sqlite

__all__ = [
    "extract_customers",
    "extract_payments",
    "transform_pipeline",
    "load_to_sqlite",
]


"""In Python, an __init__.py file inside a folder tells the interpreter “this directory is a package.” Here’s what it does for you:

Turns a folder into a package
Without __init__.py, you can’t do import scraper.job_scraper. The file (even if empty) signals “scraper” is a Python package.

Controls what’s exported
You can define an __all__ list in __init__.py to specify which modules get imported when someone does from scraper import *.

Package‑level setup
If you need package‑wide initialization (e.g. setting up logging, loading shared config), you can put that code here."""
