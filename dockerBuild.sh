#!/usr/bin/env bash
docker run --rm --mount type=bind,source="$(pwd)",target=/opt  nightscape/scala-mill /bin/sh -c 'cd /opt; chmod +x test.sh; /opt/test.sh'
