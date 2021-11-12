export SESSION_ID=$(sfdx force:org:display --json -u sift | jq -r .result.accessToken)
export INSTANCE_URL=$(sfdx force:org:display --json -u sift | jq -r .result.instanceUrl)
