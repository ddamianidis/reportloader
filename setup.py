 #!/usr/bin/env python
 
from setuptools import setup, find_packages
setup(
    name = "reportloader",
    version = "0.0.3",
    packages = find_packages(),
    #scripts = ['bbidder_server', 'bbidder_paserver'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = [
        'docutils>=0.3',
        'mysqlclient>=1.3.12', 
        'pymongo' ,
        'simplejson',
        'oauth2client',
        'google-api-python-client',
        'paramiko',
        'requests',
#        '-e git+http://damianos@gitlab/damianos/jsonrpcserver.git#egg=jrpc'
    ],
    entry_points = {
        'console_scripts': [
                        'loadmysqltomongo=reportloader.console:mysql_to_mongo',
                        'pullappnexus=reportloader.console:pull_appnexus_data'
                            
                           ],
    },
    
    package_data = {
        'reportloader': ['reportloader.conf', 
                         'tests/data/pull_adsense_expected.json',
                         'tests/data/pull_adx_expected.json',
                         'tests/data/pull_appnexus_expected.json',
                         'tests/data/pull_criteo_expected.json',
                         'tests/data/pull_criteohb_expected.json',
                         'tests/data/pull_facebook_expected.json',
                         'tests/data/pull_smaato_expected.json',
                         'tests/data/pull_smart_expected.json',
                         'tests/data/pull_taboola_expected.json',
                         'tests/data/pull_teads_expected.json',
                         'tests/data/pull_rubicon_expected.json',
                         'tests/data/pull_pubmatic_expected.json',
                         
                         'tests/data/pull_adsense_dict_expected.json',
                         'tests/data/pull_adx_dict_expected.json',
                         'tests/data/pull_appnexus_dict_expected.json',
                         'tests/data/pull_criteo_dict_expected.json',
                         'tests/data/pull_criteohb_dict_expected.json',
                         'tests/data/pull_facebook_dict_expected.json',
                         'tests/data/pull_smaato_dict_expected.json',
                         'tests/data/pull_smart_dict_expected.json',
                         'tests/data/pull_taboola_dict_expected.json',
                         'tests/data/pull_teads_dict_expected.json',
                         'tests/data/pull_rubicon_dict_expected.json',
                         'tests/data/pull_pubmatic_dict_expected.json',
                         
                         'tests/data/pull_adsense_dict_nd_expected.json',
                         'tests/data/pull_adx_dict_nd_expected.json',
                         'tests/data/pull_appnexus_dict_nd_expected.json',
                         'tests/data/pull_criteo_dict_nd_expected.json',
                         'tests/data/pull_criteohb_dict_nd_expected.json',
                         'tests/data/pull_facebook_dict_nd_expected.json',
                         'tests/data/pull_smaato_dict_nd_expected.json',
                         'tests/data/pull_smart_dict_nd_expected.json',
                         'tests/data/pull_taboola_dict_nd_expected.json',
                         'tests/data/pull_teads_dict_nd_expected.json',
                         'tests/data/pull_rubicon_dict_nd_expected.json',
                         'tests/data/pull_pubmatic_dict_nd_expected.json',
                         
                         'platforms/google/data/adsense_credentials.dat',
                         'platforms/google/data/adx_credentials.json',
                         'platforms/rubicon/data/gmail_credentials.json'],
    },

    # metadata for upload to PyPI
    author = "Damianos Damianidis",
    author_email = "damidamianidis@gmail.com",
    description = "Tailwind's ETL Reporting",
    license = "PSF",

    # could also include long_description, download_url, classifiers, etc.
)
