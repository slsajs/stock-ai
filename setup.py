from setuptools import setup, find_packages

setup(
    name="stock-ai",
    version="1.0.0",
    description="한국투자증권 KIS API 기반 주식 자동매매 시스템",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "aiohttp>=3.8.0",
        "websockets>=10.0",
        "python-dotenv>=0.19.0",
        "pycryptodome>=3.15.0",
    ],
    extras_require={
        "analysis": ["pandas>=1.3.0"],
        "dev": ["pytest>=6.0.0", "black", "flake8"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)