from dagster import load_assets_from_package_module

from . import extract

EXTRACT = 'extract'

extract_assets = load_assets_from_package_module(package_module=extract, group_name=EXTRACT)
