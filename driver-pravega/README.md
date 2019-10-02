# Pravega Driver for OpenMessaging Benchmark

# Run in AWS (without Kubernetes)

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
ssh-keygen -f ~/.ssh/pravega_aws
cd driver-pravega/deploy
terraform init
terraform apply
ansible-playbook --user ec2-user --inventory `which terraform-inventory` deploy.yaml
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

# Run Jupyter for data analysis

```
cd ..
docker run -d -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v "$PWD":/home/jovyan/work --name jupyter jupyter/scipy-notebook:1386e2046833
docker logs jupyter
```

Open Notebook results-analyzer/results-analyzer-1.ipynb and run all cells.

# P3 Test Driver

P3 Test Driver can be used to run multiple tests automatically.

```
cd ../p3_test_driver
tests/testgen_pravega.py | ./p3_test_driver.py -t - -c config/pravega_k8s.config.yaml
tests/testgen_pravega_ssh.py | ./p3_test_driver.py -t - -c config/pravega_ssh.config.yaml
```
