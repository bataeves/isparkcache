from distutils.core import setup

setup(
    name='isparkcache',
    version='0.1.11',
    packages=['isparkcache', 'isparkcache.fs'],
    install_requires=["snakebite"],
    url='https://github.com/bataeves/isparkcache',
    download_url='https://github.com/bataeves/isparkcache/archive/0.1.11.tar.gz',
    license='',
    author='Bataev Evgeny',
    author_email='bataev.evgeny@gmail.com',
    description='Cache Spark Dataframes for Jupyter',
    long_description=open("README.rst").read(),
    keywords=['spark', 'jupyter', 'ipython']
)
