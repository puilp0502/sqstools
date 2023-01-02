#!/bin/bash

TOTAL_MESSAGE_COUNT=$(aws sqs get-queue-attributes --queue-url "$1" --attribute-names ApproximateNumberOfMessages | jq -r '.Attributes.ApproximateNumberOfMessages')
let VISIBILITY_TIMEOUT=$TOTAL_MESSAGE_COUNT/9+10
echo "Visibility Timeout: $VISIBILITY_TIMEOUT" 1>&2

MESSAGE_COUNT=1
TOTAL_MESSAGES=0
while [[ "$MESSAGE_COUNT" -gt 0 ]]; do
	API_CALL_RESULT=$(aws sqs receive-message --queue-url "$1" --max-number-of-messages 10 --visibility-timeout "$VISIBILITY_TIMEOUT")
	MESSAGE_COUNT=$(echo $API_CALL_RESULT | jq '.Messages | length')
	echo $API_CALL_RESULT | jq -rc '.Messages[]'
	if [[ "$MESSAGE_COUNT" -gt 0 ]]; then
		let TOTAL_MESSAGES=$TOTAL_MESSAGES+$MESSAGE_COUNT
		echo -ne "Messages Dumped: $TOTAL_MESSAGES \r" 1>&2
	fi	
done


