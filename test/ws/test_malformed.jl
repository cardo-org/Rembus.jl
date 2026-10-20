include("../utils.jl")

using HTTP

# An empty, truncated or undecodable message closes the connection with 1008.
function malformed_close_code(payload)
    code = nothing
    try
        HTTP.WebSockets.open("ws://127.0.0.1:8339") do ws
            HTTP.WebSockets.send(ws, payload)
            HTTP.WebSockets.receive(ws)
        end
    catch e
        code = e.message.code
    end
    return code
end

function run()
    for payload in (UInt8[], UInt8[0xff, 0xff], "{bad", "[1]",
                    """{"rembus":"1.0","id":1,"method":"m","params":[]}""",
                    """{"rembus":"1.0","id":"0","method":"m","params":[]}""")
        @test malformed_close_code(payload) == Rembus.WS_CLOSE_MALFORMED
    end
end

execute(run, "malformed", ws=8339)
