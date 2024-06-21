using Rembus
using UUIDs

consume(topic, value) = @info "$topic = $value"

url = isempty(ARGS) ? string(uuid4()) : ARGS[1]
rb = component(url)
subscribe(rb, "a/*/c", consume, true)

forever(rb)
