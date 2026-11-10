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
It is not sent on the wire: the Register proof request carries
`SHA256(CBOR([pin, cid, nonce, pubkey, type]))` where `nonce` is issued by the
broker in a preceding challenge request.

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

        pin_exchange(twin, Register, cmp.id, pin, pubkey, scheme)
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
    rekey(
        cid::AbstractString,
        pin::AbstractString;
        scheme::UInt8=SIG_RSA,
        enc::UInt8=CBOR
    )

Replace the key pair of the registered component `cid`.

The component connects and authenticates with its current private key, then a
new key pair is generated and the new public key is sent in a `Rekey` message
(packet type 0x07) carrying the same nonce-bound `pin` proof used by [`register`](@ref).
The `scheme` may differ from the one used at registration time.

The local private key is replaced only if the broker answers with success; on any
failure the current private key (and the broker's registered public key) is kept
and an error is thrown.

```julia
rekey("authenticated_component", "deedbeef"; scheme=SIG_ECDSA)
```
"""
function rekey(
    cid::AbstractString,
    pin::AbstractString;
    scheme::UInt8=SIG_RSA,
    enc::UInt8=CBOR
)
    cmp = RbURL(cid)
    kfile = pkfile(cmp.id)
    if !isfile(kfile)
        error("$cid component: private key $kfile not found")
    end

    twin = connect(cmp, enc=enc)
    try
        if scheme === SIG_RSA
            pubkey = rsa_private_key(cmp.id)
        elseif scheme === SIG_ECDSA
            pubkey = ecdsa_private_key(cmp.id)
        else
            error("invalid scheme $scheme")
        end

        pin_exchange(twin, Rekey, cmp.id, pin, pubkey, scheme)
        mv("$(kfile).tmp", kfile, force=true)
    catch e
        rm("$(kfile).tmp", force=true)
        rethrow()
    finally
        shutdown(twin)
    end
end

# Return the nonce carried by a challenge response as bytes, or nothing.
function nonce_bytes(data)
    isa(data, ZMQ.Message) && (data = message2data(data))
    isa(data, AbstractString) && return base64decode(data)
    isa(data, Vector{UInt8}) && return data
    # the ZMQ client turns decoded arrays into Vector{Any}
    isa(data, Vector) && all(x -> isa(x, UInt8), data) && return Vector{UInt8}(data)
    return nothing
end

#=
Two-step Register/Rekey: send the challenge request (empty pin), then the
proof request bound to the returned nonce.
=#
function pin_exchange(twin, ::Type{T}, cid, pin, pubkey, scheme) where {T<:Union{Register,Rekey}}
    response = fetch(send_msg(twin, T(twin, id(), cid, UInt8[], UInt8[], 0)).future)
    isa(response, Exception) && throw(response)
    if response.status != STS_CHALLENGE
        rembuserror(code=response.status, reason=response_data(response))
    end
    nonce = nonce_bytes(response_data(response))
    nonce === nothing && error("invalid $T challenge")

    proof = pin_proof(pin, cid, nonce, pubkey, scheme)
    response = fetch(send_msg(twin, T(twin, id(), cid, proof, pubkey, scheme)).future)
    isa(response, Exception) && throw(response)
    if response.status != STS_SUCCESS
        rembuserror(code=response.status, reason=response_data(response))
    end
    return nothing
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

#=
    pin_proof(pin, cid, nonce, pubkey, type)

Return the value sent in the Register/Rekey `pin` field:
SHA256(CBOR([pin, cid, nonce, pubkey, type])).
The pin itself is never sent on the wire.
=#
function pin_proof(
    pin::AbstractString,
    cid::AbstractString,
    nonce::Vector{UInt8},
    pubkey::Vector{UInt8},
    type::Integer
)
    return MbedTLS.digest(
        MD_SHA256, encode([String(pin), String(cid), nonce, pubkey, UInt(type)])
    )
end

# Lifetime in seconds of an unused Register/Rekey nonce.
const REGISTER_NONCE_TTL = Ref(60.0)
const MAX_REGISTER_NONCES = 8
const CID_GRAMMAR = r"^[A-Za-z0-9._-]{1,255}$"

valid_cid(cid::AbstractString) =
    occursin(CID_GRAMMAR, cid) && !startswith(cid, ".") && !occursin("..", cid)

function issue_nonce!(twin::Twin, op::UInt8, cid::AbstractString)
    now_ts = time()
    filter!(((_, v),) -> v[2] > now_ts, twin.regnonces)
    key = (op, String(cid))
    delete!(twin.regnonces, key)
    if length(twin.regnonces) >= MAX_REGISTER_NONCES
        return nothing
    end
    nonce = rand(RandomDevice(), UInt8, 32)
    twin.regnonces[key] = (nonce, now_ts + REGISTER_NONCE_TTL[], twin.socket)
    return nonce
end

# Single use: the nonce is discarded whatever the outcome of the proof check.
function take_nonce!(twin::Twin, op::UInt8, cid::AbstractString)
    entry = pop!(twin.regnonces, (op, String(cid)), nothing)
    entry === nothing && return nothing
    nonce, expiry, socket = entry
    (expiry > time() && socket === twin.socket) || return nothing
    return nonce
end

function check_token(router, tenant, cid::AbstractString, nonce, msg)
    if nonce !== nothing && msg.type >= 0 && haskey(router.owners, tenant)
        expected = pin_proof(router.owners[tenant], cid, nonce, msg.pubkey, msg.type)
        if length(msg.pin) == length(expected) && constant_time_equal(msg.pin, expected)
            @debug "tenant [$tenant]: token is valid"
            return true
        end
    end
    @info "tenant [$tenant]: invalid token"
    return false
end

function constant_time_equal(a::Vector{UInt8}, b::Vector{UInt8})
    acc = UInt8(0)
    for i in eachindex(a)
        acc |= a[i] ⊻ b[i]
    end
    return acc == 0
end

#=
Handle the first step of Register/Rekey: reply with a nonce, or with an error
if `cid` is not valid or the nonce limit is reached.
=#
function register_challenge(msg, op::UInt8)
    twin = msg.twin
    if !valid_cid(msg.cid)
        return ResMsg(twin, msg.id, STS_GENERIC_ERROR, "invalid cid")
    elseif op == TYPE_REKEY && (!twin.isauth || rid(twin) != msg.cid)
        return ResMsg(
            twin, msg.id, STS_GENERIC_ERROR,
            "cid does not match the authenticated component"
        )
    end
    nonce = issue_nonce!(twin, op, msg.cid)
    if nonce === nothing
        return ResMsg(twin, msg.id, STS_GENERIC_ERROR, "too many pending challenges")
    end
    # The WebSocket JSON encoding carries binary data as base64 text.
    isjson = twin.enc == JSON && isa(twin.socket, WS)
    return ResMsg(twin, msg.id, STS_CHALLENGE, isjson ? base64encode(nonce) : nonce)
end

isregistered(router, cid::AbstractString) = key_file(router, cid) !== nothing

#=
    register(router, msg)

Register a component.
=#
function register_node(router, msg)
    if length(msg.pin) != 0 && length(msg.pin) != 32
        return ResMsg(msg.twin, msg.id, STS_GENERIC_ERROR, "invalid pin length")
    elseif isempty(msg.pin)
        return register_challenge(msg, TYPE_REGISTER)
    elseif !valid_cid(msg.cid)
        return ResMsg(msg.twin, msg.id, STS_GENERIC_ERROR, "invalid cid")
    end
    nonce = take_nonce!(msg.twin, TYPE_REGISTER, msg.cid)
    tenant = domain(msg.cid)
    @debug "registering pubkey of $(msg.cid), id: $(msg.id), tenant: $tenant"

    if !isenabled(router, tenant)
        return ResMsg(
            msg.twin, msg.id, STS_GENERIC_ERROR, "tenant [$tenant] not enabled"
        )
    end

    sts = STS_SUCCESS
    reason = nothing
    token_match = check_token(router, tenant, msg.cid, nonce, msg)
    if msg.type != SIG_RSA && msg.type != SIG_ECDSA
        sts = STS_GENERIC_ERROR
        reason = "invalid key type $(msg.type)"
    elseif (perr = pubkey_error(msg.pubkey, msg.type)) !== nothing
        sts = STS_GENERIC_ERROR
        reason = "invalid pubkey: $perr"
    elseif token_match === false
        sts = STS_GENERIC_ERROR
        reason = "wrong tenant/pin"
    elseif isregistered(router, msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        save_pubkey(router, msg.cid, msg.pubkey)
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

#=
    valid_pubkey(pubkey, type)

Return true if `pubkey` (PEM or DER) parses as a public key of the declared
`type`: RSA with at least 2048 bits or ECDSA on the P-256 curve.
=#
function valid_pubkey(pubkey::Vector{UInt8}, type::Integer)
    return pubkey_error(pubkey, type) === nothing
end

# Return nothing if `pubkey` is valid, otherwise the reason it is rejected.
function pubkey_error(pubkey::Vector{UInt8}, type::Integer)
    isempty(pubkey) && return "empty key"
    try
        ctx = parse_pubkey(pubkey)
        name = MbedTLS.get_name(ctx)
        if type == SIG_RSA
            name == "RSA" || return "key is $name, not RSA"
            MbedTLS.bitlength(ctx) >= 2048 || return "RSA key shorter than 2048 bits"
        elseif type == SIG_ECDSA
            name == "EC" || return "key is $name, not ECDSA"
            is_p256(ctx) || return "ECDSA key is not on the P-256 curve"
        else
            return "unknown key type $type"
        end
        return nothing
    catch e
        line = split(String(copy(pubkey)), '\n', limit=2)[1]
        return "cannot parse key (header \"$(line[1:min(end, 40)])\"): " *
               first(sprint(showerror, e), 120)
    end
end

# PEM input must be NUL terminated and counts the terminator; DER input must
# be passed with its exact length (MbedTLS.parse_public_key! always adds one).
function parse_pubkey(pubkey::Vector{UInt8})
    ispem = occursin("-----BEGIN", String(copy(pubkey)))
    buf = ispem ? [pubkey; 0x00] : copy(pubkey)
    ctx = MbedTLS.PKContext()
    GC.@preserve buf begin
        rc = ccall(
            (:mbedtls_pk_parse_public_key, MbedTLS.libmbedcrypto), Cint,
            (Ptr{Cvoid}, Ptr{UInt8}, Csize_t), ctx.data, pointer(buf), length(buf)
        )
    end
    rc == 0 || error("invalid public key: mbedtls error $rc")
    return ctx
end

# DER content of the prime256v1 (P-256) object identifier.
const P256_OID = UInt8[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07]

# The curve is read from the parsed key, re-exported as a DER
# SubjectPublicKeyInfo, so it does not depend on how the key was encoded.
function is_p256(ctx::MbedTLS.PKContext)
    buf = zeros(UInt8, 256)
    GC.@preserve buf begin
        n = ccall(
            (:mbedtls_pk_write_pubkey_der, MbedTLS.libmbedcrypto), Cint,
            (Ptr{Cvoid}, Ptr{UInt8}, Csize_t), ctx.data, pointer(buf), length(buf)
        )
    end
    n > 0 || return false
    der = buf[(end-n+1):end]
    return findfirst(P256_OID, der) !== nothing
end

#=
    rekey_node(router, msg)

Replace the public key of the authenticated component `msg.cid`.

The old key is kept unless every check succeeds. Any failure is reported with
STS_GENERIC_ERROR.
=#
function rekey_node(router, msg)
    twin = msg.twin
    if length(msg.pin) != 0 && length(msg.pin) != 32
        return ResMsg(twin, msg.id, STS_GENERIC_ERROR, "invalid pin length")
    elseif isempty(msg.pin)
        return register_challenge(msg, TYPE_REKEY)
    end
    nonce = take_nonce!(twin, TYPE_REKEY, msg.cid)
    tenant = domain(msg.cid)
    reason = nothing

    if !twin.isauth || rid(twin) != msg.cid
        reason = "cid does not match the authenticated component"
    elseif !isenabled(router, tenant)
        reason = "tenant [$tenant] not enabled"
    elseif !check_token(router, tenant, msg.cid, nonce, msg)
        reason = "wrong tenant/pin"
    elseif (perr = pubkey_error(msg.pubkey, msg.type)) !== nothing
        reason = "invalid pubkey: $perr"
    elseif !isregistered(router, msg.cid)
        reason = "$(msg.cid) not registered"
    else
        try
            save_pubkey(router, msg.cid, msg.pubkey)
        catch e
            @warn "[$twin] rekey failed: $e"
            reason = "rekey failed"
        end
    end

    sts = reason === nothing ? STS_SUCCESS : STS_GENERIC_ERROR
    return ResMsg(twin, msg.id, sts, reason)
end
