# data_miner
데이터 전처리 / DB IO / 분석을 위한 장고 REST API 프로젝트 입니다.

데이터 처리를 API 작성을 위하여 Django RestFrameWork를 사용

## main Dependencies
- python 2.7.13
- django 1.11
- djangorestframework 3.6.3
- pandas 0.20.2
- scikit-learn 0.18.1
- using anaconda 4.3.22

```bash
conda create -n project_name python=2.7
source activate project_name
conda install --yes --file requirements.txt
```

## 설명
  * preprocessor : DATA 전처리 API
    * views
      * preprocess_views.py : controller 작업
    * models.py : 해당 API 내 django orm 사용을 위한 데이터 모델

  * actionlogwriter : 사용자 행동로그 DB IO API
    * views
      * actionlogs_views.py : controller 작업
    * models.py : 해당 API 내 django orm 사용을 위한 데이터 모델

  * cluster : 사용자 행동로그 기반 클러스터링 분석 API
    * views
      * cluster_views.py : contorller 작업
    * models.py : 해당 API 내 django orm 사용을 위한 데이터 모델

  * data_ager : 사용자 행동로그 후처리 프로세스 API
    * views
      * aging_views.py: controller 작업
    * models.py : 해당 API 내 django orm 사용을 위한 데이터 모델

  * migrator : 로그 ( file to DB) 마이그레이터 API
    * views
      * migrator_views.py : controller 작업
    * models.py : 해당 API 내 django orm 사용을 위한 데이터 모델
