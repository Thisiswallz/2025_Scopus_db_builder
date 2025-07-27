"""Setup configuration for Scopus Database package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="scopus-db",
    version="0.2.0",
    author="Claude Code",
    description="A tool for creating optimized SQLite databases from Scopus CSV exports",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Thisiswallz/2025_Scopus_db_builder",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
    install_requires=[
        # No external dependencies - uses only standard library
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.9",
        ],
        "viz": [
            "pandas>=1.3.0",
            "matplotlib>=3.5.0",
            "seaborn>=0.11.0",
            "wordcloud>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "scopus-db=scopus_db.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)