# echo $(date +%Y%m%dT%H%M%S --date="-15 minutes") 
# 변수명이 print 되도록 수정하기
ts_nodash=$1
token=$2
aws_access_key_id=$3
aws_secret_access_key=$4

echo $ts_nodash 
echo "twitter_premart_py"

/home/ubuntu/anaconda3/bin/python3 /home/ubuntu/GHxPipeliner/scripts/premart_hourly_text.py --ts_nodash $ts_nodash --token $token --aws_access_key_id $aws_access_key_id --aws_secret_access_key $aws_secret_access_key  > /home/ubuntu/GHxPipeliner/log/premart_hourly_text.txt 2>&1
