# CU Trial Data Validator (Dockerized)
This app validates CSV medical trial data, archives valid files, and logs errors for rejected ones.

## Quick start
```bash
docker build -t cu-validator .
docker run --rm -it cu-validator
