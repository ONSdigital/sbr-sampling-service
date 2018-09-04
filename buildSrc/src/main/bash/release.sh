#!/bin/bash

echo "Release in progress....."
echo 'Current version: ' $1
#echo -n "Are you sure you wish to release with tag [$2]? (y/n) > "
#    read response
#    if [ "$response" != "y" ]; then
#        echo 'Bumping version to: ' $2
#        gradle release -Prelease.useAutomaticVersion=true -Prelease.releaseVersion=$1 -Prelease.newVersion=$2
#        echo "Successfully bumped version to $2"
#        exit 1
#    fi
echo 'Bumping version to: ' $2
gradle release -Prelease.useAutomaticVersion=true -Prelease.releaseVersion=$1 -Prelease.newVersion=$2
echo "Successfully bumped version to $2"
