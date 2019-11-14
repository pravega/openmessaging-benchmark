# Pravega Driver for OpenMessaging Benchmark

# Run in AWS (without Kubernetes)

For a more detailed guide, see http://openmessaging.cloud/docs/benchmarks/pulsar/.

## Build Pravega

To use a pre-release version of Pravega, you must build it manually
using the following steps.

```
pushd ../..
git clone https://github.com/pravega/pravega
cd pravega
git checkout 23b7340
./gradlew install distTar
popd
```

This will build the file ../../pravega/build/distributions/pravega-0.6.0-2361.f273314-SNAPSHOT.tgz.

If needed, change the variable pravegaVersion in deploy.yaml to match the version built.

If needed, change pom.xml to match the version built.

## Build Benchmark

```
mvn install
```

## Deployment to AWS

Install [Terraform 0.11.14](https://terraform.io/).

Install [Ansible](http://docs.ansible.com/ansible/latest/intro_installation.html).

Install Ansible modules.
```
ansible-galaxy install cloudalchemy.node-exporter
```

Create EC2 key pair.
```
ssh-keygen -f ~/.ssh/pravega_aws
```

Create EC2 resources such as VMs.
```
cd deploy
terraform init
terraform apply
```

Install software onto VMs.
```
ansible-playbook --user ec2-user --inventory `which terraform-inventory` deploy.yaml
```

Determine the Grafana IP address by running `terraform output metrics_host`.
Open Grafana at http://grafana_ip:3000.
Login using user name "admin" and password "pravega".

Configure Grafana with the following data sources:

  - Prometheus
    - Name: Prometheus
    - HTTP URL: http://localhost:9090
  - InfluxDB
    - Name: pravega-influxdb
    - HTTP URL: http://localhost:8086
    - InfluxDB Details Database: pravega

Load dashboards from [deploy/templates/dashboards](deploy/templates/dashboards).

## Run the Benchmark

```
ssh -i ~/.ssh/pravega_aws ec2-user@`terraform output client_ssh_host`
cd /opt/benchmark
bin/benchmark -d driver-pravega/pravega.yaml workloads/1-topic-16-partitions-1kb.yaml
```

## Collecting Logs and Metrics

This will download logs and metrics from the cluster to your local machine.

```
cd deploy
ansible-playbook --user ec2-user --inventory `which terraform-inventory` collect_logs_and_metrics.yaml
```

Example output:

```
ls ../../data/pravega_logs/pravega_logs_20191004T034401
benchmark-worker-10.0.0.119.log.gz
benchmark-worker-10.0.0.13.log.gz
benchmark-worker-10.0.0.42.log.gz
benchmark-worker-10.0.0.82.log.gz
bookkeeper-10.0.0.145.log.gz
bookkeeper-10.0.0.186.log.gz
bookkeeper-10.0.0.247.log.gz
influxdb.tgz
pravega-controller-10.0.0.242.log.gz
pravega-segmentstore-10.0.0.121.log.gz
pravega-segmentstore-10.0.0.253.log.gz
pravega-segmentstore-10.0.0.80.log.gz
prometheus.tgz
zookeeper-10.0.0.185.log.gz
zookeeper-10.0.0.202.log.gz
zookeeper-10.0.0.219.log.gz
```

## Viewing Previously Collected Metrics

This will run local instances of InfluxDB, Prometheus, and Grafana loaded with previously
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

## Tearing Down

```
terraform destroy
```

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
tests/testgen_pravega_ssh.py | ./p3_test_driver.py -t - -c config/pravega_ssh.config.yaml
```

# Run Jupyter for data analysis of results from P3 Test Driver

```
cd ..
docker run -d -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v "$PWD":/home/jovyan/work --name jupyter jupyter/scipy-notebook:1386e2046833
docker logs jupyter
```

Open Notebook results-analyzer/results-analyzer-2.ipynb and run all cells.
