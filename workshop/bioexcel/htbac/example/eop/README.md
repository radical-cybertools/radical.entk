# Running the Ensemble of Pipelines example

## Installation

Ideally, you can run the EnTK script with ```python runme.py``` after
[installing](https://radicalentk.readthedocs.io/en/latest/install.html) EnTK.

We have simplified the installation process by wrapping all dependencies
inside a Docker container. You can find the instructions to install Docker in
the [official documentation](https://www.docker.com/products/docker-engine).

## Execution

You can build and run this container with

```
docker build . -t entk-eop
docker run --rm -v $(pwd)/output:/output:rw entk-eop
```

## Understanding the output

You will have five files ```output_ana_1.txt``` to ```output_ana_5.txt```
generated for each of the five pipelines in your EnTK script. Each of the files
will have output from the simulation task of that pipeline. You can replace the
tasks with real simulation and analysis kernels, and add more pipelines or
stages or tasks as required by your application.

We hope this gives example gave you a quick and brief understanding of the EnTK
API and its use to create the ensemble of pipelines pattern.
