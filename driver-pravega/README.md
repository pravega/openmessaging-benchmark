# Pravega Driver for OpenMessaging Benchmark

# Run in AWS (without Kubernetes)

For a more detailed guide, see http://openmessaging.cloud/docs/benchmarks/pulsar/.

## Build Pravega

```
cd
git clone https://github.com/pravega/pravega
cd pravega
git checkout f273314
./gradlew install distTar
```

## Build Benchmark

```
mvn install
```

## Deployment to AWS

Install Terraform 0.11.14.

Install Ansible.

```
ansible-galaxy install cloudalchemy.node-exporter
ssh-keygen -f ~/.ssh/pravega_aws
cd deploy
terraform init
terraform apply
ansible-playbook --user ec2-user --inventory `which terraform-inventory` deploy.yaml
```

## Collecting Logs and Metrics

```
cd deploy
ansible-playbook --user ec2-user --inventory `which terraform-inventory` collect_logs_and_metrics.yaml.yaml
```

## Viewing Previously Collected Metrics

This will load local instances of InfluxDB, Prometheus, and Grafana with previously
collected metrics.

```
cd deploy
open_saved_metrics/open_saved_metrics.sh ../../data/pravega_logs/pravega_logs_20191004T034401
```

Open Grafana at http://localhost:3000.
Login using user name "admin" and any password.

Configure Grafana with the following data sources:

  - Prometheus
    - Name: Prometheus
    - HTTP URL: http://prometheus:9090
  - InfluxDB
    - Name: pravega-influxdb
    - HTTP URL: http://influxdb:8086
    - InfluxDB Details Database: pravega

Load dashboards from [deploy/templates/dashboards](deploy/templates/dashboards).

## Troubleshooting

```
terraform-inventory -inventory
ssh -i ~/.ssh/pravega_aws ec2-user@`terraform output segmentstore_0_ssh_host`
journalctl -u pravega-segmentstore
```

# Run in Kubernetes

## Build Docker container

```
./docker-build.sh
```

## Run local driver on Kubernetes:
```
kubectl run -n examples --rm -it --image claudiofahey/openmessaging-benchmark:latest --serviceaccount examples-pravega openmessaging-benchmark
```

## Run in Kubernetes

```
./deploy-k8s-components.sh
```

# P3 Test Driver

P3 Test Driver can be used to run multiple tests automatically.

```
cd ../p3_test_driver
tests/testgen_pravega.py | ./p3_test_driver.py -t - -c config/pravega_k8s.config.yaml
tests/testgen_pravega_ssh.py | ./p3_test_driver.py -t - -c config/pravega_ssh.config.yaml
```

# Run Jupyter for data analysis of results from P3 Test Driver

```
cd ..
docker run -d -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v "$PWD":/home/jovyan/work --name jupyter jupyter/scipy-notebook:1386e2046833
docker logs jupyter
```

Open Notebook results-analyzer/results-analyzer-2.ipynb and run all cells.
