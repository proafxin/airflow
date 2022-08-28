from setuptools import find_packages, setup

setup(
    name="etl",
    version="0.1.0",
    author="Masum Billal",
    author_email="billalmasum93@gmail.com",
    packages=find_packages(),
    # scripts=["bin/script1", "bin/script2"],
    url="http://pypi.python.org/pypi/PackageName/",
    license="LICENSE.txt",
    description="An awesome package that does something",
    long_description=open("README.md", encoding="utf-8").read(),
    install_requires=[],
)
