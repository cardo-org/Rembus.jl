using Rembus

rb = broker()
add_plugin(rb, KeySpaceRouter())
wait(rb)
