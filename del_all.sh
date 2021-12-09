#!/usr/bin/env bash

while read p; do
    kill $p
done < pid.txt