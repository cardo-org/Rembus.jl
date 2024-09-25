#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

#=
    rsa_private_key(cid::AbstractString)

Create a private key for `cid` component and return its public key.
=#
function rsa_private_key(cid::AbstractString)
    file = "$(pkfile(cid)).tmp"
    cmd = `ssh-keygen -f $file -t rsa -m PEM -b 2048 -N ''`
    Base.run(cmd)

    try
        return read(`ssh-keygen -f $file -e -m PEM`)
    finally
        rm("$file.pub", force=true)
    end
end

function ecdsa_private_key(cid::AbstractString)
    file = "$(pkfile(cid)).tmp"
    cmd = `ssh-keygen -f $file -t ecdsa -m PEM -b 256 -N ''`
    Base.run(cmd)

    try
        return read(`ssh-keygen -f $file -e -m pem`)
    finally
        rm("$file.pub", force=true)
    end
end

"""
    register(cid::AbstractString, userid::AbstractString, pin::AbstractString)

Register the component with name `cid`.

To register a component a user must be provisioned in the `owners.csv` table.

The `pin` shared secret is a 8 hex digits string (for example "deedbeef").

"""
function register(
    cid::AbstractString,
    userid::AbstractString,
    pin::AbstractString,
    type::UInt8=SIG_RSA
)
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
        if type === SIG_RSA
            pubkey = rsa_private_key(cmp.id)
        elseif type === SIG_ECDSA
            pubkey = ecdsa_private_key(cmp.id)
        end

        value = parse(Int, pin, base=16)
        msgid = id() & 0xffffffffffffffffffffffff00000000 + value

        msg = Register(msgid, cmp.id, userid, pubkey, type)
        response = wait_response(rb, msg, request_timeout())
        if isa(response, RembusTimeout)
            throw(response)
        elseif (response.status != STS_SUCCESS)
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
    unregister(rb)

Unregister the connected component.

Only a connected and authenticated component may execute the unregister command.

```
using Rembus

rb = connect("authenticated_component")
Rembus.unregister(rb)
close(rb)
```
"""
function unregister(rb::RBConnection)
    cid = rb.client.id
    @debug "unregistering $cid"

    msg = Unregister(cid)
    response = wait_response(rb, msg, request_timeout())
    if isa(response, RembusTimeout)
        throw(response)
    elseif (response.status != STS_SUCCESS)
        rembuserror(code=response.status)
    end

    # remove the private key
    rm(pkfile(cid), force=true)
end
