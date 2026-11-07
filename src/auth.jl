function keystore_dir()
    return get(ENV, "REMBUS_KEYSTORE", joinpath(rembus_dir(), "keystore"))
end

keys_dir(r::Router) = joinpath(r.settings.rembus_dir, r.process.supervisor.id, "keys")
keys_dir(name::AbstractString) = joinpath(rembus_dir(), name, "keys")

# Legacy key file names encode the key type and format: <cid>.<type>.<format>.
legacy_key_files(basename::AbstractString) = [
    "$basename.$type.$format" for format in ["pem", "der"] for type in ["rsa", "ecdsa"]
]

# The canonical key file is named <cid> (no extension); legacy names are only
# read as a fallback.
function fullname(basename::AbstractString)
    isfile(basename) && return basename
    for fn in legacy_key_files(basename)
        isfile(fn) && return fn
    end
    return nothing
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

# Store the public key of `cid` in the canonical key file with a same-directory
# temporary file and an atomic rename. Legacy key files are removed only after
# the rename succeeded.
function save_pubkey(router, cid::AbstractString, pubkey)
    fn = key_base(router, cid)
    mkpath(dirname(fn))
    tmp, io = mktemp(dirname(fn))
    try
        write(io, pubkey)
        close(io)
        mv(tmp, fn, force=true)
    catch
        close(io)
        rm(tmp, force=true)
        rethrow()
    end
    for legacy in legacy_key_files(fn)
        rm(legacy, force=true)
    end
end

function remove_pubkey(router, cid::AbstractString)
    fn = key_base(router, cid)
    rm(fn, force=true)
    for legacy in legacy_key_files(fn)
        rm(legacy, force=true)
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
