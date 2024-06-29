using Rembus
using Rocket

@component "rock"

function my_topic(actor, n)
    @info "my_topic: $n"
    next!(actor, n)
end

subject = Subject(Number)

Visor.dump()
@shared subject
@subscribe my_topic before_now

keeper = keep(Number)
subscribe!(subject |> filter((n) -> n > 100), (n) -> @publish alarm("critical value: $n"))
subscribe!(subject, keeper)
subscribe!(subject, logger())

@forever
