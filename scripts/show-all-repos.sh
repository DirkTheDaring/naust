#!/usr/bin/bash
#set -ex
URL=https://registry.trantor.kaupon.de:10443
# All repos single call
curl -sS "$URL/_meta/catalog?include_tags=1" | jq
exit 0
# All repos within one org, with tags:
curl -sS "$URL/_meta/orgs/org1/repos?include_tags=1" | jq
# One repo with its tags:
curl -sS "$URL/_meta/repos/org1/repoa?include_tags=1" | jq
