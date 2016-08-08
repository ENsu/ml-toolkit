from distutils.core import setup
# use >> python setup.py sdist upload     to upload new version

setup(
    name='ml-toolkit',    # This is the name of your PyPI-package.
    packages=['ml_toolkit'],
    version='0.06',                          # Update the version number for new releases
    description='Tools for doing machine learning',
    author='ENsu',
    install_requires=[
        "google-api-python-client",
        "pandas",
        "retrying",
        "requests",
        "ml_metrics",
        "tqdm"
    ]
)
