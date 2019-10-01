# Pravega Driver for OpenMessaging Benchmark

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

## Run Jupyter for data analysis

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
tests/testgen_pravega.py | ./p3_test_driver.py -t - -c config/pravega.config.yaml
```

# Deployment to AWS

```
mvn install
ssh-keygen -f ~/.ssh/pravega_aws
cd driver-pravega/deploy
terraform_0.11.14 init
terraform_0.11.14 apply
ansible-playbook --user ec2-user --inventory `which terraform-inventory` deploy.yaml
```
