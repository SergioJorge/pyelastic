# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

version = '0.0.1'

setup(name='PyElastic',
      version=version,
      description="Python ElasticSearch Wrapper",
      classifiers=[],
      keywords='Elastic search elasticsearch',
      author='Sergio Jorge',
      author_email='sergiojcamposjr@gmail.com',
      url='http://github.com/SergioJorge/pyelastic',
      license='MIT',
      packages=find_packages(exclude=['testes']),
      include_package_data=True,
      zip_safe=True,
      install_requires=['requests==2.20.0'],
      )
