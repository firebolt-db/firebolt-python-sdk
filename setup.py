from setuptools import find_packages, setup

setup(
    name="firebolt",
    description="Python SDK for Firebolt",
    author="Eric Gustavson",
    author_email="eg@firebolt.io",
    url="https://github.com/firebolt-analytics/firebolt-sdk",
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    entry_points={
        'console_scripts': [
            'firebolt=firebolt.cli:run',
            'fb=firebolt.cli:run',
        ],
    },
    install_requires=[
        "httpx[http2]==0.18",
        "python-dotenv",
        "pydantic",
        "typer",
        "toolz",
    ],
    extras_require={
        "dev": [
            "pre-commit",
            "pytest",
            "pytest-mock",
        ]
    },
)
