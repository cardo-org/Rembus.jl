using Rembus

function set_indicator(kpi)
    if haskey(kpi, "name") && kpi["name"] == "T"
        return "ok"
    else
        error("ko")
    end
end

function main()
    rb = component("myserver")
    expose(rb, set_indicator)
    wait(rb)
end

main()
