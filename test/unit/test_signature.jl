using Rembus
using Test

struct FakeTwin
    challenge::Vector{UInt8}
    session::Dict
    FakeTwin(dare) = new(dare, Dict("challenge" => dare))
end

function verify(cid, wrong_challenge=nothing)
    rb = Rembus.RBConnection(cid)

    challenge = [0x1, 0x2, 0x3, 0x4]
    Response = Rembus.ResMsg(1, Rembus.STS_SUCCESS, challenge)

    att = Rembus.attestate(rb, Response)

    if wrong_challenge === nothing
        server_challenge = challenge
    else
        server_challenge = wrong_challenge
    end
    Rembus.verify_signature(FakeTwin(server_challenge), att)
end

#cid = "test_cid"
cid = "plain_test"
secret = "pippo"

mkpath(Rembus.keys_dir())

client_fn = Rembus.pkfile(cid)
server_fn = Rembus.key_file(cid)
@info "secret file: $client_fn"

# create client and server files
for fn in [client_fn, server_fn]
    open(fn, "w") do f
        write(f, secret)
    end
end

isvalid = verify(cid)
@test isvalid

## challenge = [0x1, 0x2, 0x3, 0x4]
## Response = Rembus.ResMsg(1, Rembus.STS_SUCCESS, challenge)
##
## att = Rembus.attestate(rb, Response)
## @info att
## isvalid = Rembus.verify_signature(FakeTwin(challenge), att)
## @info "isvalid: $isvalid"
## @test isvalid

try
    verify(cid, [0x01])
    @test false
catch e
    @test e.msg == "authentication failed"
end

cid = "private_test"

# create private secret
client_fn = Rembus.pkfile(cid)
pubkey = Rembus.create_private_key(cid)
mv("$(client_fn).tmp", client_fn, force=true)

# create public secret
server_fn = Rembus.key_file(cid)
open(server_fn, "w") do f
    write(f, pubkey)
end

isvalid = verify(cid)
@test isvalid
#pubf = Rembus.pubkey_file(cid)
#@info "pubfile: $pubf"
