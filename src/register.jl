#=
    rsa_private_key(cid::AbstractString)

Create a private key for `cid` component and return its public key.
=#
function rsa_private_key(cid::AbstractString)
    file = "$(pkfile(cid, create_dir=true)).tmp"
    cmd = `ssh-keygen -f $file -t rsa -m PEM -b 2048 -N ''`
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
        tenant=Union{Nothing, AbstractString} = nothing,
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
    tenant::Union{Nothing,AbstractString}=nothing,
    scheme::UInt8=SIG_RSA
)
    cmp = RbURL(cid)
    kfile = pkfile(cmp.id)
    if isfile(kfile)
        error("$cid component: found private key $kfile")
    end

    @debug "connecting register"
    twin = connect(RbURL(protocol=cmp.protocol, host=cmp.host, port=cmp.port))
    try
        @debug "registering $cid"
        if scheme === SIG_RSA
            pubkey = rsa_private_key(cmp.id)
        elseif scheme === SIG_ECDSA
            pubkey = ecdsa_private_key(cmp.id)
        end

        value = parse(Int, pin, base=16)
        msgid = id() & 0xffffffffffffffffffffffff00000000 + value

        msg = Register(twin, msgid, cmp.id, tenant, pubkey, scheme)
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

    msg = Unregister(twin, cid)
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
    df = router.owners[(router.owners.tenant.==tenant_id), :]
    if isempty(df)
        @info "unknown tenant [$tenant_id]"
        return false
    else
        if nrow(df) > 1
            @info "multiple tenants found for [$tenant_id]"
            return false
        end
        return (columnindex(df, :enabled) == 0) ||
               ismissing(df[1, :enabled]) ||
               (df[1, :enabled] === true)
    end
end

function get_token(router, tenant, id::UInt128)
    vals = UInt8[(id>>24)&0xff, (id>>16)&0xff, (id>>8)&0xff, id&0xff]
    token = bytes2hex(vals)
    df = router.owners[(router.owners.pin.==token).&(router.owners.tenant.==tenant), :]
    if isempty(df)
        @info "tenant [$tenant]: invalid token"
        return nothing
    else
        @debug "tenant [$tenant]: token is valid"
        return token
    end
end

#=
    register(router, msg)

Register a component.
=#
function register_node(router, msg)
    @debug "registering pubkey of $(msg.cid), id: $(msg.id), tenant: $(msg.tenant)"

    if !isenabled(router, msg.tenant)
        return ResMsg(
            msg.twin, msg.id, STS_GENERIC_ERROR, "tenant [$(msg.tenant)] not enabled"
        )
    end

    sts = STS_SUCCESS
    reason = nothing
    if msg.tenant === nothing
        tenant = router.process.supervisor.id
    else
        tenant = msg.tenant
    end

    token = get_token(router, tenant, msg.id)
    if token === nothing
        sts = STS_GENERIC_ERROR
        reason = "wrong tenant/pin"
    elseif isregistered(router, msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        kdir = keys_dir(router)
        mkpath(kdir)
        save_pubkey(router, msg.cid, msg.pubkey, msg.type)
        if !(msg.cid in router.component_owner.component)
            push!(router.component_owner, [tenant, msg.cid])
        end
        save_tenant_component(router, router.component_owner)
    end
    return ResMsg(msg.twin, msg.id, sts, reason)
end

#=
    unregister(router, twin, msg)

Unregister a component.
=#
function unregister_node(router, msg)
    @debug "[$router] unregistering $(msg.cid), isauth: $(msg.twin.isauth)"
    twin = msg.twin
    sts = STS_SUCCESS
    reason = nothing

    if !command_permitted(router, twin) || !twin.isauth
        sts = STS_GENERIC_ERROR
        reason = "invalid operation"
    else
        remove_pubkey(router, msg.cid)
        deleteat!(router.component_owner, router.component_owner.component .== msg.cid)
        save_tenant_component(router, router.component_owner)
    end
    response = ResMsg(twin, msg.id, sts, reason)
    put!(twin.process.inbox, response)

    return nothing
end
