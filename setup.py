from setuptools import find_packages, setup

setup(
    name="mma_betting",
    packages=find_packages(exclude=["mma_betting_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "scrapy",
        "scrapy-user-agents",
        "scrapy-rotating_proxies"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
