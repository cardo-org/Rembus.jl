function keystore_dir()
    return get(ENV, "REMBUS_KEYSTORE", joinpath(rembus_dir(), "keystore"))
end

keys_dir(r::Router) = joinpath(r.settings.rembus_dir, r.process.supervisor.id, "keys")
keys_dir(name::AbstractString) = joinpath(rembus_dir(), name, "keys")

function fullname(basename::AbstractString)
    for format in ["pem", "der"]
        for type in ["rsa", "ecdsa"]
            fn = "$basename.$type.$format"
            if isfile(fn)
                return fn
            end
        end
    end
    return isfile(basename) ? basename : nothing
end

function key_base(router::Router, cid::AbstractString)
    res = joinpath(router.settings.rembus_dir, router.process.supervisor.id, "keys", cid)
    return res
end

function key_base(broker_name::AbstractString, cid::AbstractString)
    return joinpath(rembus_dir(), broker_name, "keys", cid)
end

function key_file(router::AbstractRouter, cid::AbstractString)
    basename = key_base(router, cid)
    return fullname(basename)
end

function key_file(broker_name::AbstractString, cid::AbstractString)
    basename = key_base(broker_name, cid)
    return fullname(basename)
end

function save_pubkey(router, cid::AbstractString, pubkey, type)
    name = key_base(router, cid)
    format = "der"
    # check if pubkey start with -----BEGIN chars
    if pubkey[1:10] == UInt8[0x2d, 0x2d, 0x2d, 0x2d, 0x2d, 0x42, 0x45, 0x47, 0x49, 0x4e]
        format = "pem"
    end
    if type == SIG_RSA
        typestr = "rsa"
    else
        typestr = "ecdsa"
    end
    fn = "$name.$typestr.$format"
    open(fn, "w") do io
        write(io, pubkey)
    end
end

function remove_pubkey(router, cid::AbstractString)
    fn = key_file(router, cid)
    if fn !== nothing
        rm(fn)
    end
end

function pubkey_file(router, cid::AbstractString)
    fn = key_file(router, cid)

    if fn !== nothing
        return fn
    else
        error("auth failed: unknown $cid")
    end
end
