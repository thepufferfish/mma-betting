from dagster import load_assets_from_package_module, with_resources
from mma_betting.resources.json_io_manager import JSONIOManager
from . import extract

EXTRACT = 'extract'

extract_assets = with_resources(
    load_assets_from_package_module(package_module=extract, group_name=EXTRACT),
    resource_defs={"io_manager": JSONIOManager()}
)
