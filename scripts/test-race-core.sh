#!/usr/bin/env bash
set -euo pipefail

go test -race -count=1 ./storage ./benchmark ./coordserver ./coordinator/runtime ./transport/grpcx ./client ./adminhttp
