using Rembus
using UUIDs

consume(topic, value) = println("$topic = $value")

url = isempty(ARGS) ? string(uuid4()) : ARGS[1]
rb = component(url)
subscribe(rb, "a/*/c", consume)
reactive(rb)
wait(rb)
