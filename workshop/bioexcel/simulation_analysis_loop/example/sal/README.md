# Running the Simulation Analysis Loop example

## Installation

Ideally, you can run the EnTK script with ```python runme.py``` after
[installing](https://radicalentk.readthedocs.io/en/latest/install.html) EnTK.

We have simplified the installation process by wrapping all dependencies
inside a Docker container. You can find the instructions to install Docker in
the [official documentation](https://www.docker.com/products/docker-engine).

## Execution

You can build and run this container with

```
docker build . -t entk-sal
docker run --rm -v $(pwd)/output:/output:rw entk-sal
```

## Understanding the output

You will have two files ```output_ana_1.txt``` and ```output_ana_2.txt```
generated in each of the two iterations of the simulation analysis loop script.
Each of the files will have output from the simulation task of that
iteration. You can replace the tasks with real simulation and analysis kernels,
and add more stages or tasks as required by your application.

We hope this gives example gave you a quick and brief understanding of the EnTK
API and its use to create the simulation analysis loop pattern.
