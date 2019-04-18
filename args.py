#!/usr/bin/env python
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--port', '-p', type=int, default=7070)
parser.add_argument('--gost', '-g', type=str, default='👻')

args = parser.parse_args()

print(vars(args))
