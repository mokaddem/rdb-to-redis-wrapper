set -e
set -x

sudo apt-get install python3-virtualenv

if [ -z "$VIRTUAL_ENV" ]; then
    virtualenv -p python3 RDBINJECTOR
    . ./RDBINJECTOR/bin/activate
fi

pip3 install -U pip argparse npyscreen rdbtools
