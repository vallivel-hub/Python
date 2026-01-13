from setuptools import setup, find_packages

setup(
    name="db_utils",
    version="1.0",
    packages=find_packages(),
        install_requires=[
        "pyyaml",
        "oracledb",
        "pyodbc"
    ],
    include_package_data=True,
    description="Reusable database utilities including connection management and environment handling.",
    author="Bruce Bierce",
    author_email="bj@maine.edu",
    url="https://github.com/yourusername/db_utils",  # Replace with GitHub repo 
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)