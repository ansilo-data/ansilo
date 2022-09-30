#!/bin/bash

set -e
set -x

echo $VAULT_DEV_ROOT_TOKEN_ID | vault login -


vault policy write secrets_read - << EOF
path "kv2/data/secrets/*" {
  capabilities = ["read", "list"]
}
EOF

vault auth enable userpass
vault auth enable approle

vault write auth/userpass/users/testuser \
    password=ansilo_test \
    policies=secrets_read

vault secrets enable -version=2 -path=kv2 kv
vault kv put kv2/secrets/test key1=mysecret key2=anothersecret
vault kv put kv2/supersecrets/test key=mysupersecret

