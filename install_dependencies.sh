set -e
set -x

sudo apt-get install python-pip python-virtualenv

if [ -z "$VIRTUAL_ENV" ]; then
    virtualenv RDBINJECTOR
    . ./RDBINJECTOR/bin/activate
fi

pip install -U pip argparse npyscreen
