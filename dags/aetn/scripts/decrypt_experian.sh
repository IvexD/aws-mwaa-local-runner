#!/usr/bin/env bash
set -x
# run script example:
# $ sh scripts/decrypt_experian.sh <dir>

# load input params
local_dir=$1

cd /tmp;
#preparing uploading
#download gpg experian public key
if [ ! -f "/tmp/vip-aetndigital-com.private.asc" ]; then
    aws s3 cp s3://devops.aetndigital.com/vip/pgp-keys/vip-aetndigital-com.private.asc /tmp/vip-aetndigital-com.private.asc;
fi
gpg --allow-secret-key-import --import /tmp/vip-aetndigital-com.private.asc;

for file in $(ls ${local_dir})
do
 cd ${local_dir}
 sourceName=$file
 sourceTmpName=$(echo ${sourceName}_tmp)
 #decrypt files
 echo r0ckstar|gpg --batch --passphrase-fd 0 --cipher-algo AES256 --output ${sourceTmpName} --decrypt ${sourceName}

 #overwrite original files
 mv ${sourceTmpName} ${sourceName}
done
