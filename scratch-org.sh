#!/usr/bin/bash

sfdx force:org:create -f sift/config/project-scratch-def.json
. refresh-token.sh