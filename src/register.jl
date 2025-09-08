#=
    rsa_private_key(cid::AbstractString)

Create a private key for `cid` component and return its public key.
=#
function rsa_private_key(cid::AbstractString)
    file = "$(pkfile(cid, create_dir=true)).tmp"
    cmd = `ssh-keygen -f $file -t rsa -m PEM -b 3072 -N ''`
    Base.run(cmd)

    try
        return read(`ssh-keygen -f $file -e -m PEM`)
    finally
        rm("$file.pub", force=true)
    end
end

function ecdsa_private_key(cid::AbstractString)
    file = "$(pkfile(cid, create_dir=true)).tmp"
    cmd = `ssh-keygen -f $file -t ecdsa -m PEM -b 256 -N ''`
    Base.run(cmd)

    try
        return read(`ssh-keygen -f $file -e -m pem`)
    finally
        rm("$file.pub", force=true)
    end
end

"""
    register(
        cid::AbstractString,
        pin::AbstractString;
        scheme::UInt8
    )

Register the component with name `cid`.

To register a component a single `pin` or a set of tenants must be configured
in the `tenants.json` file.

The `pin` shared secret is a 8 hex digits string (for example "deedbeef").

"""
function register(
    cid::AbstractString,
    pin::AbstractString;
    scheme::UInt8=SIG_RSA,
    enc::UInt8=CBOR
)
    cmp = RbURL(cid)
    kfile = pkfile(cmp.id)
    if isfile(kfile)
        error("$cid component: found private key $kfile")
    end

    @debug "connecting register"
    twin = connect(RbURL(protocol=cmp.protocol, host=cmp.host, port=cmp.port))
    twin.enc = enc
    try
        @debug "registering $cid"
        if scheme === SIG_RSA
            pubkey = rsa_private_key(cmp.id)
        elseif scheme === SIG_ECDSA
            pubkey = ecdsa_private_key(cmp.id)
        end

        msg = Register(twin, id(), cmp.id, pin, pubkey, scheme)
        futresponse = send_msg(twin, msg)
        response = fetch(futresponse.future)
        if isa(response, Exception)
            throw(response)
        elseif (response.status != STS_SUCCESS)
            rembuserror(code=response.status, reason=response_data(response))
        end
        # finally save the key
        mv("$(kfile).tmp", kfile)
    catch e
        rm("$(kfile).tmp", force=true)
        rethrow()
    finally
        shutdown(twin)
    end
end


"""
    unregister(twin)

Unregister the connected component.

Only a connected and authenticated component may execute the unregister command.

```
using Rembus

twin = connect("authenticated_component")
Rembus.unregister(twin)
close(twin)
```
"""
function unregister(twin::Twin)
    cid = twin.uid.id
    @debug "unregistering $cid"

    msg = Unregister(twin)
    futresponse = send_msg(twin, msg)
    response = fetch(futresponse.future)
    if isa(response, RembusTimeout)
        throw(response)
    elseif (response.status != STS_SUCCESS)
        rembuserror(code=response.status)
    end

    # remove the private key
    rm(pkfile(cid), force=true)
end

function isenabled(router, tenant_id::AbstractString)
    return haskey(router.owners, tenant_id)
end

function check_token(router, tenant, token::AbstractString)
    if haskey(router.owners, tenant) && router.owners[tenant] == token
        @debug "tenant [$tenant]: token is valid"
        return true
    else
        @info "tenant [$tenant]: invalid token"
        return false
    end
end

#=
    register(router, msg)

Register a component.
=#
function register_node(router, msg)
    tenant = domain(msg.cid)
    @debug "registering pubkey of $(msg.cid), id: $(msg.id), tenant: $tenant"

    if !isenabled(router, tenant)
        return ResMsg(
            msg.twin, msg.id, STS_GENERIC_ERROR, "tenant [$tenant] not enabled"
        )
    end

    sts = STS_SUCCESS
    reason = nothing
    token_match = check_token(router, tenant, msg.pin)
    if token_match === false
        sts = STS_GENERIC_ERROR
        reason = "wrong tenant/pin"
    elseif isregistered(router, msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        kdir = keys_dir(router)
        mkpath(kdir)
        save_pubkey(router, msg.cid, msg.pubkey, msg.type)
    end
    return ResMsg(msg.twin, msg.id, sts, reason)
end

#=
    unregister(router, twin, msg)

Unregister a component.
=#
function unregister_node(router, msg)
    nodeid = rid(msg.twin)
    @debug "[$router] unregistering $nodeid, isauth: $(msg.twin.isauth)"
    twin = msg.twin
    sts = STS_SUCCESS
    reason = nothing

    if !command_permitted(router, twin) || !twin.isauth
        sts = STS_GENERIC_ERROR
        reason = "invalid operation"
    else
        remove_pubkey(router, nodeid)
    end
    response = ResMsg(twin, msg.id, sts, reason)
    put!(twin.process.inbox, response)

    return nothing
end
