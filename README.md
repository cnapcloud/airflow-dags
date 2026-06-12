# airflow-dags

Airflow DAG으로 Kubernetes Pod에서 Llama LoRA 파이프라인을 실행하기 위해 아래 사전 준비가 필요합니다.

### 1. 노드 Role 설정

파이프라인 Pod는 `ml` role이 지정된 노드에만 스케줄됩니다.

```bash
kubectl label node <worker-node> node-role.kubernetes.io/ml=true
```

확인:
```bash
kubectl get nodes
# KIND-WORKER 의 ROLES 컬럼에 ml 이 표시되어야 합니다.
```

### 2. Hugging Face Secret 생성

```bash
kubectl -n ml create secret generic hf-secret \
  --from-literal=HF_TOKEN=your_hf_token
```

### 3. PVC 사전 생성

학습 데이터 및 모델 저장에 사용하는 PVC를 미리 생성해야 합니다.

```bash
# 예시 (StorageClass는 클러스터 환경에 맞게 변경)
kubectl -n ml apply -f manifests/lora-data-pvc.yaml
```

PVC 이름: `lora-data-pvc`  
마운트 경로: `/mnt/data`

확인:
```bash
kubectl -n ml get pvc lora-data-pvc
# STATUS 가 Bound 이어야 합니다.
```

### 4. 컨테이너 이미지 준비

```bash
docker pull cnapcloud/llama-3.2-1b-lora:latest
```

이미지에는 `wrappers.*` 모듈(seed, analysis, validation, train, eval, promote)이 포함되어 있어야 합니다.

## 파이프라인 구조

```
seed → analysis → validation → train → evaluate → check_promote → promote
```

| Task | 모듈 |
|------|------|
| seed | `wrappers.airflow_seed` |
| analysis | `wrappers.airflow_analysis` |
| validation | `wrappers.airflow_validation` |
| train | `wrappers.airflow_train` |
| evaluate | `wrappers.airflow_eval` |
| promote | `wrappers.airflow_promote` |

`check_promote`는 `evaluate` 결과의 `promoted: true` 여부를 확인해 이후 단계를 단락(short-circuit)합니다.

## 환경 변수 (Airflow 환경에서 설정)

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `MLOPS_AIRFLOW_NAMESPACE` | `ml` | Pod가 실행될 네임스페이스 |
| `MLOPS_PIPELINE_IMAGE` | `cnapcloud/llama-3.2-1b-lora:latest` | 파이프라인 컨테이너 이미지 |
| `MLOPS_PIPELINE_IMAGE_PULL_POLICY` | `Always` | 이미지 풀 정책 |
