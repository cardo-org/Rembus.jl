using Rembus
using Rocket

#=
Start the rocker subscriber:

terminal> j -i rocket.jl

Send numbers with a publisher component:

using Rembus
@publish value(1)

=#

@component "rocket"

function my_topic(actor, n)
    next!(actor, n)
end

subject = Subject(Number)
@shared subject

@subscribe my_topic before_now

keeper = keep(Number)

# send an alarm if the number published on value is greater than 100
subscribe!(
    subject |> filter((n) -> n > 100),
    (n) -> @publish alarm("critical value: $n")
)

subscribe!(subject, keeper)
subscribe!(subject, logger())

@forever
