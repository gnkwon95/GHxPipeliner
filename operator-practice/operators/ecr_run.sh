# aws 계정 로그인
aws ecr get-login-password --region ap-northeast-2 | sudo docker login --username AWS --password-stdin 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com

# docker 실행
sudo docker pull 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com/feature_practice_hb:latest
sudo docker run --name feature_test 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com/feature_practice_hb python runner.py --m "$1" --t "$2" --c $3

# docker container 삭제
sudo docker rm feature_test
