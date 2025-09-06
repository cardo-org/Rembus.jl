using Rembus
using UUIDs

consume(topic, value) = println("$topic = $value")

url = string(uuid4())
topic = isempty(ARGS) ? "a/*/c" : ARGS[1]

rb = component(url)
subscribe(rb, topic, consume)
reactive(rb)
wait(rb)
