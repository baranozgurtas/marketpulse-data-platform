# ============================================================
# MarketPulse - local Kubernetes (k3s via k3d) workflow
# ============================================================
# Targets:
#   make cluster    - create a local k3d cluster (k3s in Docker)
#   make build      - build the Airflow and Spark images
#   make import     - import local images into the k3d cluster
#   make deploy     - apply all manifests (requires k8s/02-secret.yaml)
#   make status     - show pods, services, jobs
#   make teardown   - delete the marketpulse namespace
#   make destroy    - delete the whole k3d cluster
#
# Usage on plain k3s (not k3d): skip `make cluster`/`make import`
# and instead load images with:
#   docker save marketpulse/airflow:latest | sudo k3s ctr images import -
#   docker save marketpulse/spark:latest   | sudo k3s ctr images import -
# ============================================================

CLUSTER_NAME ?= marketpulse
NAMESPACE    ?= marketpulse
AIRFLOW_IMG  ?= marketpulse/airflow:latest
SPARK_IMG    ?= marketpulse/spark:latest

.PHONY: cluster build import deploy status teardown destroy

cluster:
	# Maps NodePort 30080 (Airflow UI) to localhost:30080
	k3d cluster create $(CLUSTER_NAME) --port "30080:30080@server:0"

build:
	docker build -f docker/Dockerfile.airflow -t $(AIRFLOW_IMG) .
	docker build -f docker/Dockerfile.spark   -t $(SPARK_IMG) .

import:
	k3d image import $(AIRFLOW_IMG) $(SPARK_IMG) -c $(CLUSTER_NAME)

deploy:
	@test -f k8s/02-secret.yaml || \
		(echo "ERROR: k8s/02-secret.yaml not found."; \
		 echo "Run: cp k8s/02-secret.template.yaml k8s/02-secret.yaml and fill in credentials."; \
		 exit 1)
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/01-configmap.yaml
	kubectl apply -f k8s/02-secret.yaml
	kubectl apply -f k8s/03-postgres.yaml
	kubectl -n $(NAMESPACE) wait --for=condition=ready pod -l app=postgres --timeout=180s
	# Jobs are immutable; drop any previous run before re-applying.
	kubectl -n $(NAMESPACE) delete job airflow-init --ignore-not-found
	kubectl apply -f k8s/04-airflow-init-job.yaml
	kubectl -n $(NAMESPACE) wait --for=condition=complete job/airflow-init --timeout=300s
	kubectl apply -f k8s/05-airflow-webserver.yaml
	kubectl apply -f k8s/06-airflow-scheduler.yaml
	kubectl apply -f k8s/07-spark.yaml
	@echo ""
	@echo "Deployed. Airflow UI: http://localhost:30080 (admin/admin)"

status:
	kubectl -n $(NAMESPACE) get pods,svc,jobs

teardown:
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

destroy:
	k3d cluster delete $(CLUSTER_NAME)
