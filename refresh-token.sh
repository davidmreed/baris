export SESSION_ID=$(sfdx force:org:display --json -u test-4iu0vw81wmy2@example.com | jq -r .result.accessToken)
export INSTANCE_URL=$(sfdx force:org:display --json -u test-4iu0vw81wmy2@example.com | jq -r .result.instanceUrl)
