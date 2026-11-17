#!/bin/bash

version="v1.1.4"

git add .
git commit
git tag $version
echo "update version: $version"
git push origin $version
git push origin main