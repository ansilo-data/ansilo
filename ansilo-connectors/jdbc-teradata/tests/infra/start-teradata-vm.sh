#!/bin/bash

set -e

echo "Retrieving azure secret from Secrets Manager..."
AZURE_LOGIN=$(aws secretsmanager get-secret-value \
    --secret-id arn:aws:secretsmanager:ap-southeast-2:635198228996:secret:test/azure/service-user-Fo94AC \
    --query SecretString \
    --output text \
    | jq -r .azure_secret)

echo "Logging into Azure..."
az login --service-principal \
    -u $(echo $AZURE_LOGIN | jq -r .clientId) \
    -p $(echo $AZURE_LOGIN | jq -r .clientSecret) \
    --tenant $(echo $AZURE_LOGIN | jq -r .tenantId) 

echo "Starting teradata vm..."
az vm start --resource-group Ansilo --name teradata-test

# echo "Waiting for port..."
# TRIES=0
# while ((TRIES < 10));
# do
#     echo "Checking if port is open..."
#     set +e
#     nc -vz ansilo-teradata-test.japaneast.cloudapp.azure.com 1026
#     set -e

#     if [[ $EXIT_CODE == 0 ]];
#     then
#         break
#     fi
    
#     sleep 5
#     let "TRIES+=1"
# done

echo "Waiting for boot..."
# sleep 30

echo "Done"