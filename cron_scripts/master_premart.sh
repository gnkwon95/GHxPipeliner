# 변수 선언
# $ts_nodash= $(date +%Y%m%dT%H%M%S --date="-15 minutes")
# $token = "AAAAAAAAAAAAAAAAAAAAAJTwbAEAAAAAD7XIHIwmYLky2lzV%2Bc1dJCSjJ5g%3DDRg1HkrEyRcbIVHcZSMH1PUwUf00Yv0r41aVwXMOKmKILtQLMu"
# $aws_access_key_id = "AKIAWJWWCQEZ433B6NHP"
# $aws_secret_access_key = "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"
ts_nodash=$(date +%Y%m%dT%H%M%S --date="-15 minutes")
token="AAAAAAAAAAAAAAAAAAAAAJTwbAEAAAAAD7XIHIwmYLky2lzV%2Bc1dJCSjJ5g%3DDRg1HkrEyRcbIVHcZSMH1PUwUf00Yv0r41aVwXMOKmKILtQLMu"
aws_access_key_id="AKIAWJWWCQEZ433B6NHP"
aws_secret_access_key="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"

echo $ts_nodash # cron에 남는다.
echo "shell"
# py -> job or feature, .sh 파일 한 폴더로 scripts로 구성
# 각 cron 실행 (각 쉘을 위의 변수를 받아 실행시키고 그 로그를 twitter_cron.txt로 남겨라; 동일해야하는 ts_nodash를 print하기)
# twitter_cron.sh
# premart_twitter_cron.sh

# /home/ubuntu/GHxPipeliner/cron/twitter_cron.sh --ts_nodash $ts_nodash --token $token --aws_access_key_id $aws_access_key_id --aws_secret_access_key aws_secret_access_key > /home/ubuntu/GHxPipeliner/log/twitter_cron.txt 2>&1
# /home/ubuntu/GHxPipeliner/cron/premart_twitter_cron.sh --ts_nodash $ts_nodash --token $token --aws_access_key_id $aws_access_key_id --aws_secret_access_key aws_secret_access_key > /home/ubuntu/GHxPipeliner/log/twitter_cron.txt 2>&1

#/home/ubuntu/GHxPipeliner/cron/twitter_cron.sh $ts_nodash $token $aws_access_key_id $aws_secret_access_key > /home/ubuntu/GHxPipeliner/log/twitter_cron.txt 2>&1
/home/ubuntu/GHxPipeliner/cron/premart_twitter_cron.sh $ts_nodash $token $aws_access_key_id $aws_secret_access_key > /home/ubuntu/GHxPipeliner/log/twitter_cron.txt 2>&1
