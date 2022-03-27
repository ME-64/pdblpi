from setuptools import setup
import pathlib


HERE = pathlib.Path(__file__).parent
# README = (HERE / "README.md").read_text()


setup(
  name = 'pdblpi',
  packages = ['pdblpi'],
  version = '0.0.1',
  license='MIT',
  description = 'enhanced bloomberg api wrapper',
  # long_description = README,
  #long_description_content_type = 'text/markdown',
  author = 'Milo Elliott',
  author_email = 'milo_elliott@icloud.com',
  # url = 'https://github.com/ME-64/pyfixerio',
  keywords = ['API wrapper', 'bbg', 'bloomberg'],
  # include_package_data = True,
   install_requires=[
           'pandas', 'pytz', 'blpapi', 'xlwings', 'pyparsing'
       ],
  classifiers=[
    'Development Status :: 3 - Alpha',      
    'Intended Audience :: Developers',     
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
  ],
)
