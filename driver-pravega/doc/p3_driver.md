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
