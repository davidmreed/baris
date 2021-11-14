#!/usr/bin/bash

sfdx force:org:create -f sfdx/config/project-scratch-def.json -a sift
source refresh-token.sh