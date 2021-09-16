#!/usr/bin/env bash
set -x
# run script example:
# $ sh scripts/encrypt_cheetah.sh <dir>
# This file is kept on Databricks in /dbfs/FileStore/experian/encrypt_cheetah.sh directory, along with the experian public key from below.
gpg --allow-secret-key-import --import /dbfs/FileStore/experian/experian-com.public.asc

# load input params
local_dir=$1

for file in $(ls ${local_dir})
do
 cd ${local_dir}
 sourceName=$local_dir$file
 sourceTmpName=$(echo ${sourceName}.gpg)
 #encrypt files
 gpg --yes --trust-model always --output $sourceTmpName --encrypt --recipient 'support@conversen.com' $sourceName
done