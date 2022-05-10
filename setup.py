from setuptools import setup, find_packages

VERSION = '0.6.0'
DESCRIPTION = 'Connectors, model classes, utilities to be used within SmartForge-related Python projects.'
LONG_DESCRIPTION = 'Connectors, model classes, utilities to be used within SmartForge-related Python projects.'

setup(
    name="smartforge",
    version=VERSION,
    author="Tommaso Azzalin",
    author_email="azzalintommaso@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=["asyncua == 0.9.92",
                      "cassandra-driver == 3.25.0",
                      "redis == 4.2.2",
                      "requests == 2.27.1"],
    keywords=['smartforge', 'karlstad', 'university',
              'kau', 'bharat', 'forge', 'bf'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Manufacturing",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Typing :: Typed"
    ]
)
