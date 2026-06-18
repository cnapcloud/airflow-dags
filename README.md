# MLOps Airflow DAGs

Kubernetes 환경에서 ML 파이프라인을 실행하는 Airflow DAG 모음입니다.

---

## 테스트 환경

**클러스터**
- 구성: Multipass VM 기반 k3s 클러스터 (Ubuntu 24.04.4 LTS)
- k3s 버전: v1.35.5+k3s1 / containerd 2.2.3
- SSH 접근: `multipass shell <node-name>`

**노드**
- `master-1` (192.168.0.231) — control-plane, etcd, taint: `node-role.kubernetes.io/control-plane:NoSchedule`
- `worker-1` (192.168.0.232) — 범용 워크로드
- `worker-2` (192.168.0.233) — ML 워크로드 전용 (`node-role.kubernetes.io/ml=true`), taint: `ml:NoSchedule`
- `worker-3` (192.168.0.234) — AV 워크로드 전용 (`node-role.kubernetes.io/av=`)
- 워커 스펙: CPU 6코어 / 메모리 16GB / 디스크 40GB

**실행 중인 인프라** (`ml` 네임스페이스)
- Airflow (Celery executor) — api-server, dag-processor, scheduler, worker, triggerer, redis, postgresql, statsd
- MLflow — 실험 추적 및 모델 레지스트리 (postgresql 백엔드)
- KubeRay — RayCluster 오퍼레이터
- RayCluster — head 1개, worker 최대 2개 (오토스케일)
  - head: CPU 2코어 / 메모리 4Gi (request 200m / 2Gi)
  - worker: CPU 2코어 / 메모리 8Gi (request 1코어 / 8Gi)

**모니터링** (`infra` 네임스페이스)
- Prometheus + Grafana + Alertmanager
- MinIO — 오브젝트 스토리지

**주의사항**
1. 클러스터 구성
   - VM 위에 kind를 설치하면 스토리지 I/O가 `Mac → VM → Docker overlay → 컨테이너` 다중 레이어를 거쳐 훈련 중 I/O 포화로 노드 전체가 먹통이 되는 현상 발생
   - 반드시 각 노드가 독립 VM 또는 물리 머신으로 분리된 클러스터(k3s + Multipass, kubeadm 등)에 적용할 것
2. OOM 리스크
   - `train` pod(~2GB), `airflow-worker`(~1GB), `raycluster-worker`(4~8GB)가 같은 노드에 스케줄되면 16GB 노드에서 train이나 airflow-worker가 OOM eviction 발생 가능 -- 파이프라인 중단
   - 메모리 request 미설정 pod는 kubelet eviction 1순위 대상이므로 반드시 `resources.requests` 지정
   - 워커 노드 메모리 32GB 이상 권장

---

## 공통 사전 준비

모든 파이프라인 실행 전 아래 인프라가 `ml` 네임스페이스에 준비되어 있어야 합니다.

### MLflow 서버

```bash
kubectl -n ml get svc mlflow
# mlflow.ml.svc.cluster.local 으로 접근 가능해야 합니다.
```

### Ray Cluster

```bash
kubectl -n ml get svc raycluster-head-svc
# raycluster-head-svc.ml.svc.cluster.local:10001 으로 접근 가능해야 합니다.
```

---

## Taxi XGBoost 파이프라인

NYC Taxi 데이터를 XGBoost + Ray로 학습하고 MLflow에 등록하는 파이프라인입니다.

### 사전 준비

**1. 입력 데이터 준비**

각 Pod는 `data/raw/` 경로에서 원본 데이터를 읽습니다. 해당 경로에 NYC Taxi 원본 데이터가 있어야 합니다.

**2. 컨테이너 이미지 준비**

```bash
docker pull cnapcloud/taxi-xgboost:latest
```

이미지에는 `step1_analyze.py` ~ `step5_register.py` 스크립트가 포함되어 있어야 합니다.

### 파이프라인 구조

```
analyze → validate → train → evaluate → register
```

| Task | 스크립트 |
|------|---------|
| step1_analyze | `/app/step1_analyze.py` |
| step2_validate | `/app/step2_validate.py` |
| step3_train | `/app/step3_train.py` (Ray 분산 학습) |
| step4_evaluate | `/app/step4_evaluate.py` |
| step5_register | `/app/step5_register.py` (MLflow 등록 + 자동 프로모트) |

---

## Llama LoRA 파이프라인

Airflow DAG으로 Kubernetes Pod에서 Llama LoRA 파이프라인을 실행하기 위해 아래 사전 준비가 필요합니다.

### 사전 준비

**1. 노드 Role 설정**

이미지 크기가 큰 경우 특정 노드에만 pull하여 캐시하고 재사용할 수 있습니다. 파이프라인 Pod에 `ml` role node selector를 지정하면 해당 노드로만 스케줄됩니다.

```bash
kubectl label node <worker-node> node-role.kubernetes.io/ml=true
```

확인:
```bash
kubectl get nodes
# ROLES 컬럼에 ml 이 표시되어야 합니다.
```

**2. Hugging Face Secret 생성**

```bash
kubectl -n ml create secret generic hf-secret \
  --from-literal=HF_TOKEN=your_hf_token
```

**3. PVC 사전 생성**

학습 데이터 및 모델 저장에 사용하는 PVC를 미리 생성해야 합니다.

```bash
kubectl -n ml apply -f manifests/lora-data-pvc.yaml
```

PVC 이름: `lora-data-pvc`  
마운트 경로: `/mnt/data`

확인:
```bash
kubectl -n ml get pvc lora-data-pvc
# STATUS 가 Bound 이어야 합니다.
```

**4. 컨테이너 이미지 준비**

```bash
docker pull cnapcloud/llama-3.2-1b-lora:latest
```

이미지에는 `wrappers.*` 모듈(seed, analysis, validation, train, eval, promote)이 포함되어 있어야 합니다.

### 파이프라인 구조

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

### 환경 변수 (Airflow 환경에서 설정)

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `MLOPS_AIRFLOW_NAMESPACE` | `ml` | Pod가 실행될 네임스페이스 |
| `MLOPS_PIPELINE_IMAGE` | `cnapcloud/llama-3.2-1b-lora:latest` | 파이프라인 컨테이너 이미지 |
| `MLOPS_PIPELINE_IMAGE_PULL_POLICY` | `Always` | 이미지 풀 정책 |
