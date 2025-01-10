include("../utils.jl")

using Prometheus

function run()
    router = broker(wait=false)

    islistening(router, wait=10)
    rb = connect()

    Rembus.RouterCollector(router)
    metrics = []
    for collector in Prometheus.DEFAULT_REGISTRY.collectors
        if isa(collector, Rembus.RouterCollector)
            @info "exposing $collector"
            Prometheus.collect!(metrics, collector)
        end
    end
    @info "metrics: $metrics"
    @test length(metrics) == 1
    close(rb)
end

@info "[test_prometheus] start"
try
    run()
catch e
    @test false
finally
    shutdown()
end
@info "[test_prometheus] stop"
