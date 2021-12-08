#!/usr/bin/env bash
# echo $1
for run in {1..$1}; do 
go run ./cmd/mrworker.go wc.so  &
echo $! >> pid.txt
done

# go