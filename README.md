`fr_user_events_consumer`
===========================

A custom Python library and scripts for Wikimedia Foundation Fundraising. Consumes
user-facing events from log files and loads related data into a MariaDB database.

Usage
-----

Copy `config-example.yaml` to `config.yaml` and adjust settings as appropriate.

Scripts are in the `bin/` directory. Run them with --help for details about
command-line options. See also inline comments in `config-example.yaml` and
sample log files in the `test_data/` directory.

Regardless of the options selected, events will only be consumed from files that have not
been marked as already processed.

Filenames
----------

Log filenames must be globally unique across all log types consumed by the scripts.

Timestames in filenames must be in Ymd-HMS format as output by time.strftime().

For CentralNotice event log files, the sample rate component of filenames must appear
after timestamp.

Filenames ending in '.gz' are assumed to be compressed with gzip. Otherwise, there are
read as plain text.
