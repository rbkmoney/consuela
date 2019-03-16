REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)

UTILS_PATH := build-utils
TEMPLATES_PATH := .

SERVICE_NAME := consuela
BUILD_IMAGE_TAG := ee0028263b7663828614e3a01764a836b4018193

CALL_ANYWHERE := all submodules compile xref lint dialyze clean distclean testcompile
CALL_W_CONTAINER := $(CALL_ANYWHERE) test

define DOCKER_COMPOSE_PREEXEC_HOOK
	make wc_testcompile
endef

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk

.PHONY: $(CALL_W_CONTAINER)

submodules:
	@if git submodule status | egrep -q '^[-]|^[+]'; then git submodule update --init; fi

compile: submodules
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint:
	$(REBAR) lint

dialyze: submodules
	$(REBAR) dialyzer

clean:
	$(REBAR) clean

distclean:
	rm -rf _build

testcompile:
	$(REBAR) as test compile

test: submodules
	$(REBAR) eunit
	$(REBAR) ct

TESTSUITES = $(wildcard test/*_SUITE.erl)

define testsuite

test.$(patsubst %_SUITE.erl,%,$(notdir $(1))): $(1) submodules
	$(REBAR) ct --suite=$$<

endef

$(foreach suite,$(TESTSUITES),$(eval $(call testsuite,$(suite))))
