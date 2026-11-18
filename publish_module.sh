#!/bin/bash

version="v1.1.7"

git add .
git commit -m "update version: $version"
git tag $version
echo "update version: $version"
git push origin $version
git push origin main