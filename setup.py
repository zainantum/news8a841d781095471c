from setuptools import find_packages, setup

setup(
    name="news8a841d781095471c",
    version="0.1.1",
    packages=find_packages(),
    install_requires=[
        'aiohttp>=3.8.1',  # For asynchronous HTTP requests
        'python-dateutil>=2.8.2',  # For parsing dates
        'requests>=2.26.0',  # For fetching the JSON feed
        # 'exorde_data',  # If this is a custom module, ensure it is installable or included in the package
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
