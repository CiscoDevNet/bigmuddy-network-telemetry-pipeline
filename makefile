# name of executable.
BINARY=pipeline

include skeleton/pipeline.mk

# Setup pretest as a prerequisite of tests.
test: pretest
pretest:
	@echo Setting up zookeeper, kafka. Docker required.
	tools/test/run.sh
