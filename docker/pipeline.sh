#!/bin/sh
# `/sbin/setuser pipeline` runs the given command as the user `pipeline`.
# If you omit that part, the command will be run as root.
exec /sbin/setuser pipeline /usr/bin/pipeline -log=/data/pipeline.log -config=/data/pipeline.conf -pem=/data/pipeline_rsa
