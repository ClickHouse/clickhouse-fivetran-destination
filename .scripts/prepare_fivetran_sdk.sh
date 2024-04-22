#!/bin/bash

set -e

rm -rf fivetran_sdk
git clone https://github.com/fivetran/fivetran_sdk.git fivetran_sdk
cd fivetran_sdk
git init
# FIXME: this is the commit before the NAIVE_TIME update; checkout the latest when it's implemented.
git checkout 1fabb7626b6ec81a4f56d49a16a654210cb1d0be
cd ..
git init
mkdir -p proto
