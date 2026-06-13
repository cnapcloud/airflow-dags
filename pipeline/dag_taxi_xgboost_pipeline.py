"""
Airflow DAG that runs each stage in a Kubernetes pod.

1. ML 전용 노드에 Role 지정
   kubectl label node <worker-node> node-role.kubernetes.io/ml=true
2. 컨테이너 이미지 준비: cnapcloud/taxi-xgboost:latest
3. MLflow 서버: mlflow.ml.svc.cluster.local
4. Ray Cluster: raycluster-head-svc.ml.svc.cluster.local:10001
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def _pod_task(task_id: str, arguments: list[str]) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=os.getenv("MLOPS_AIRFLOW_NAMESPACE", "ml"),
        image=os.getenv("MLOPS_PIPELINE_IMAGE", "cnapcloud/taxi-xgboost:latest"),
        cmds=["python"],
        arguments=arguments,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy=os.getenv("MLOPS_PIPELINE_IMAGE_PULL_POLICY", "Always"),
        node_selector={"node-role.kubernetes.io/ml": "true"},
        logging_interval=3,
        startup_timeout_seconds=600,
    )


with DAG(
    dag_id="taxi_xgboost_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "mlops"},
    tags=["mlops", "xgboost", "ray"],
) as dag:
    analyze = _pod_task("step1_analyze", [
        "/app/step1_analyze.py",
        "--input", "data/raw/",
        "--output", "reports/analysis",
    ])

    validate = _pod_task("step2_validate", [
        "/app/step2_validate.py",
        "--input", "data/raw/",
        "--output", "reports/validation",
    ])

    train = _pod_task("step3_train", [
        "/app/step3_train.py",
        "--input", "data/raw/",
        "--mlflow-uri", "http://mlflow.ml.svc.cluster.local",
        "--ray-address", "ray://raycluster-head-svc.ml.svc.cluster.local:10001",
        "--num-workers", "2",
    ])

    evaluate = _pod_task("step4_evaluate", [
        "/app/step4_evaluate.py",
        "--test-data", "data/raw/",
        "--mlflow-uri", "http://mlflow.ml.svc.cluster.local",
        "--threshold", "0.0",
    ])

    register = _pod_task("step5_register", [
        "/app/step5_register.py",
        "--mlflow-uri", "http://mlflow.ml.svc.cluster.local",
        "--auto-promote",
    ])

    analyze >> validate >> train >> evaluate >> register