using Rembus

function say_hello(name)
    return "hello loving $name"
end

@component "greater"
@expose say_hello
@forever
