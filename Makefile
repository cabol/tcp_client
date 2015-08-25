PROJECT = tcp_client

DEPS = poolboy

dep_poolboy = git https://github.com/devinus/poolboy.git 1.5.1

DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

include erlang.mk
