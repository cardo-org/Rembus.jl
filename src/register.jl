#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

"""
    create_private_key(cid::AbstractString)

Create a private key for `cid` component and return its public key.
"""
function create_private_key(cid::AbstractString)
    file = "$(pkfile(cid)).tmp"
    cmd = `ssh-keygen -f $file -m PEM -b 2048 -N ''`
    Base.run(cmd)

    try
        return read(`ssh-keygen -f $file -e -m PEM`)
    finally
        rm("$file.pub", force=true)
    end
end

"""
  register(cid::AbstractString)

Register the client identified by `cid`.
"""
function register(cid::AbstractString, userid::AbstractString, pin::AbstractString)
    cmp = Component(cid)

    kfile = pkfile(cmp.id)

    if isfile(kfile)
        error("$cid component: found private key $kfile")
    end

    @debug "connecting register"
    process = NullProcess(cmp.id)
    rb = RBConnection(cmp)
    _connect(rb, process)

    try
        @debug "registering $cid"
        pubkey = create_private_key(cmp.id)

        value = parse(Int, pin, base=16)
        msgid = id() & 0xffffffffffffffffffffffff00000000 + value

        msg = Register(msgid, cmp.id, userid, pubkey)
        response = wait_response(rb, msg, request_timeout())
        if (response.status != STS_SUCCESS)
            rembuserror(code=response.status, reason=response.data)
        end
        # finally save the key
        mv("$(kfile).tmp", kfile)

    catch e
        rm("$(kfile).tmp", force=true)
        rethrow()
    finally
        close(rb)
    end
end


"""
  unregister(cid::AbstractString)

Unregister the client identified by `cid`.

The secret pin is not needed because only an already connected and authtenticated
component may execute the unregister command.
"""
function unregister(rb, cid::AbstractString)
    @debug "unregistering $cid"

    msg = Unregister(cid)
    response = wait_response(rb, msg, request_timeout())
    if (response.status != STS_SUCCESS)
        rembuserror(code=response.status)
    end

    # remove the private key
    rm(pkfile(cid), force=true)
end

function transport_send(router, ws, msg::Register)
    pkt = [TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.userid, msg.pubkey]
    transport_write(ws, pkt)
end

function transport_send(router, ws, msg::Unregister)
    pkt = [TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]
    transport_write(ws, pkt)
end