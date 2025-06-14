from setuptools import setup, find_packages

setup(
    name="cafe-etl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.0",
        "pandas==2.1.4",
        "pytest==7.4.3",
        "black==23.11.0",
        "flake8==6.1.0",
    ],
    python_requires=">=3.8",
)
