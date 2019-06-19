REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)

UTILS_PATH := build-utils
TEMPLATES_PATH := .

SERVICE_NAME := consuela
BUILD_IMAGE_TAG := 3ff0ac71c353f2f61045ac8ebf72e20ed55b3ec8

CALL_ANYWHERE := all submodules compile xref lint dialyze clean distclean
CALL_W_CONTAINER := $(CALL_ANYWHERE) test

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

test: submodules
	$(REBAR) eunit
	$(REBAR) ct

TESTSUITES = $(wildcard test/*_SUITE.erl)

define testsuite

test.$(patsubst %_SUITE.erl,%,$(notdir $(1))): $(1) submodules
	$(REBAR) ct --suite=$$<

endef

$(foreach suite,$(TESTSUITES),$(eval $(call testsuite,$(suite))))
