import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="archivetar",
    version="0.13.1",
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
    python_requires=">=3.6",
    scripts=[
        "bin/archivetar",
        "bin/unarchivetar",
        "bin/archivepurge",
        "bin/archivescan",
    ],
)
