#!/bin/bash

set -e

KITE_CONF_FILE=$1

if [ ! -f $KITE_CONF_FILE ]; then
    echo "Kite configuration file $KITE_CONF_FILE not found"
    exit 1
fi

TICKET_CACHE_LOCATION=$(grep -m 1 "kite.ticket.cache.locatio" $KITE_CONF_FILE | awk -F'=' '{print $2}')

if [ -f $TICKET_CACHE_LOCATION ]; then
    echo "Ticket cache file already exists"

    # Check if the ticket cache file is valid or not
    CACHE_INFO=$(klist $TICKET_CACHE_LOCATION 2>&1 | grep -m 1 "krbtgt")
    if [ $? -eq 0 ]; then
        TICKET_EXPIRATION=$(echo "$CACHE_INFO" | awk '{print $3, $4}')
        EXPIRATION_TIMESTAMP=$(date -d "$TICKET_EXPIRATION" +%s)
        CURRENT_TIMESTAMP=$(date +%s)

        if [ $EXPIRATION_TIMESTAMP -gt $CURRENT_TIMESTAMP ]; then
            echo "Ticket cache file is still valid"
            exit 0
        else
            echo "Ticket cache file is expired"
        fi
    else
        echo "Failed to get ticket cache expiration date"
    fi
fi

KEYSTONE_URL="https://os-identity.vip.ebayc3.com/v2.0/tokens"
KITE_ENDPOINT=$(grep -m 1 "kite.server.endpoint" $KITE_CONF_FILE | awk -F'=' '{print $2}')
PRINCIPAL=$(grep -m 1 "kite.user.principal" $KITE_CONF_FILE | awk -F'=' '{print $2}')

KEYSTONE_API_KEY=$(grep -m 1 "keystone.api.key" $KITE_CONF_FILE | awk -F'=' '{print $2}')
KEYSTONE_API_SECRET=$(grep -m 1 "keystone.api.secret" $KITE_CONF_FILE | awk -F'=' '{print $2}')


KEYSTONE_RESPONSE=$(curl -s -X POST $KEYSTONE_URL -H 'cache-control: no-cache' -H 'content-type: application/json' -d '{"auth": { "passwordCredentials": {"username": "'$KEYSTONE_API_KEY'", "password": "'$KEYSTONE_API_SECRET'"}}}')
TOKEN=$(echo $KEYSTONE_RESPONSE | jq -r '.access.token.id')
if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
    echo "Failed to get token, response: $KEYSTONE_RESPONSE"
    exit 1
fi


KITE_RESPONSE=$(curl -s -X GET -H 'AUTH_TYPE: KEYSTONE' -H "KEYSTONE_TOKEN: $TOKEN" "$KITE_ENDPOINT/v1/credential?userPrincipal=$PRINCIPAL")
TICKET_CACHE_DATA=$(echo $KITE_RESPONSE | jq -r '.credentialData')
if [ -z "$TICKET_CACHE_DATA" ] || [ "$TICKET_CACHE_DATA" == "null" ]; then
    echo "Failed to get ticket cache data, response: $KITE_RESPONSE"
    exit 1
fi

echo $TICKET_CACHE_DATA | base64 -d > $TICKET_CACHE_LOCATION
