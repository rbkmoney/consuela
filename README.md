# consuela

Distributed consistent process registry over [consul](https://consul.io) cluster.

## Build

    $ rebar3 compile

## Use

### As process registry

As any other process registry, you can register and refer to a process with `{via, consuela, Name}`
tuple:
```erlang
{ok, Pid} = gen_server:start_link({via, consuela, some_global_resource}, ?MODULE, Args, []).
```

To set things up you can start `consuela` with registry configured like this:
```erlang
{consuela, [
    {registry, #{
        nodename => "consul",
        namespace => <<"my-service-ns">>,
    }}
}
```

Complete registry configuration looks like this:
```erlang
#{
    % [Required] Where's Consul server in the network?
    nodename => "consul0",
    % [Required] What namespace to use to isolate my set of registrations?
    namespace => <<"my-service-ns">>,
    % Consul client options
    consul => #{
        % Consul API endpoint ("http://{nodename}:8500" by default)
        url => "http://consul0:8500",
        opts => #{
            % Client event handler, e.g. for logging communication issues
            pulse => {my_service_logger, client}
        }
    },
    % Consul session options
    % See: https://www.consul.io/docs/internals/sessions.html
    session => #{
        % Name of the session ("{namespace}" by default)
        name => <<"my-service-session">>,
        % How long should session stay alive in Consul without renewing? (20s by default)
        ttl => 20,
        % How long should Consul prevent locks left after died session to be re-acquired? (10s by default)
        lock_delay => 10
    },
    % Session keeper options
    keeper => #{
        % How often session should be renewed? Number of seconds or ratio (half of TTL by default)
        interval => genlib_rational:new(1, 3),
        % Keeper event handler, e.g. for logging renewal issues
        pulse => {my_service_logger, keeper}
    },
    reaper => #{
        % For how long and How often to retry failed reaping? (at most 10min with 5s intervals between attempts by default)
        retry => genlib_retry:linear({max_total_timeout, 10 * 60 * 1000}, 5000),
        % Reaper event handler, e.g. for tracking queue metrics
        pulse => {my_service_logger, reaper}
    },
    registry => #{
        % Registry event handler, e.g. for logging failed registrations or measuring registration latencies
        pulse => {my_service_logger, registry}
    }
}
```

## Gotchas

* **[IMPORTANT]** Please run Consul with [_dead servers cleanup_ facility
  disabled](https://learn.hashicorp.com/consul/day-2-operations/autopilot#dead-server-cleanup) as it
  may under some circumstances evict a Consul server from a quorum too early, which breaks safety
  guarantees. See [hashicorp/consul#6793](https://github.com/hashicorp/consul/issues/6793).

## Roadmap

* User-friendly lock identifiers? E.g. `space/acceptor-10`.
