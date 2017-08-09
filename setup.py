from distutils.core import setup

# upload to pypi:
# python setup.py sdist; twine upload dist/isparkcache-0.1.12.tar.gz

__version__ = "0.1.12"

setup(
    name='isparkcache',
    version=__version__,
    packages=['isparkcache', 'isparkcache.fs'],
    install_requires=["snakebite"],
    url='https://github.com/bataeves/isparkcache',
    download_url='https://github.com/bataeves/isparkcache/archive/%s.tar.gz' % __version__,
    license='',
    author='Bataev Evgeny',
    author_email='bataev.evgeny@gmail.com',
    description='Cache Spark Dataframes for Jupyter',
    long_description=open("README.rst").read(),
    keywords=['spark', 'jupyter', 'ipython']
)
