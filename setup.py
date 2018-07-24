from setuptools import setup

setup(
    name = 'fr_user_event_consumer',
    version  = '0.1',
    description = 'Command-line utilities to process and store user-facing events for WMF Fundraising',
    license = 'GPL',
    packages = [ 'fr_user_event_consumer' ],
    install_requires = [
        'pyyaml >= 3.11'
    ],
    scripts = [
       'bin/consume_central_notice_events',
       'bin/consume_landing_page_events'
   ]
)