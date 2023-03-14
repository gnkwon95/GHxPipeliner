### 구조
* Docker image (ECR)
    * feature/
        * `runner.py`: 가장 최근 분봉 계산해서 Blue 인지 Red인지 알려줌

* airflow/operators/
    * `ecr_operator.py`: ECR에 있는 feature로 연산하는 operator