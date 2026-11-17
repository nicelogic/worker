#!/bin/bash

version="v1.0.1"

git add .
git commit
git tag $version
echo "update version: $version"
git push origin $version