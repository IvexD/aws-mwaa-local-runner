#!/usr/bin/env bash
set -x
# run script example:
# $ sh scripts/encrypt_cheetah.sh <dir>

# load input params
local_dir=$1

cd /tmp;
#preparing uploading
#download gpg experian public key
if [ ! -f "/tmp/experian-com.public.asc" ]; then
    aws s3 cp s3://devops.aetndigital.com/vip/pgp-keys/experian-com.public.asc /tmp/experian-com.public.asc;
fi
gpg --yes --import /tmp/experian-com.public.asc;

for file in $(ls ${local_dir})
do
 cd ${local_dir}
 sourceName=$file
 sourceTmpName=$(echo ${sourceName}_tmp)
 #encrypt files
 gpg --yes --trust-model always --output $sourceTmpName --encrypt --recipient 'support@conversen.com' $sourceName

 #overwrite original files
 mv ${sourceTmpName} ${sourceName}
done