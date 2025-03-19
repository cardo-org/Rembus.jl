using Rembus

function say_hello(name)
    return "hello $name"
end

@component "greater"
@expose say_hello
@wait
