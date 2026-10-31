#!/usr/bin/env bash
helm push dist/*.tgz oci://ghcr.io/scaibu/charts
