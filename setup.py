from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='pyspark-anonymizer',
    packages=['pyspark_anonymizer'],
    version='0.2',
    license='apache-2.0',
    description='Python library which makes it possible to dynamically mask/anonymize data using JSON string or python dict rules in a PySpark environment.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='wesleywilian',
    url='https://github.com/wesleywilian/pyspark-anonymizer',
    download_url='https://github.com/wesleywilian/pyspark-anonymizer/archive/v0.2.tar.gz',
    keywords=['data anonymizer', 'anon', 'spark', 'data mask', 'mask', 'data masking', 'masking'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
