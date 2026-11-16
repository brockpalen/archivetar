import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="archivetar",
    version="1.1.0",
    author="Brock Palen",
    author_email="brockp@umich.edu",
    description="Prep folder for archive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brockpalen/archivetar/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    scripts=[
        "bin/archivetar",
        "bin/unarchivetar",
        "bin/archivepurge",
        "bin/archivescan",
        "bin/archivetar-flow",
    ],
    # The Globus Flow wrapper (archivetar_flow) is optional. Its endpoint
    # functions need only the standard library (Python 3.11+); this extra
    # is for registering and deploying the flow from an admin machine.
    extras_require={
        "globus-flow": ["globus-sdk", "globus-compute-sdk", "jinja2"],
    },
    package_data={
        "archivetar_flow": ["templates/*.j2", "*.example.toml"],
    },
)
