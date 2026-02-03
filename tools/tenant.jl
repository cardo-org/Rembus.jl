#!/usr/bin/env julia

using ArgParse
using DuckDB
using Rembus


function load_file(broker_name)
    dir = broker_dir(broker_name)
    mkpath(dir)

    fn = joinpath(dir, Rembus.TENANTS_FILE)

    tenants = Dict{String,String}()

    if isfile(fn)
        tenants = JSON3.read(read(fn, String), Dict{String,String})
    end

    return tenants
end

function save_file(broker_name, tenants)
    fn = joinpath(broker_dir(broker_name), Rembus.TENANTS_FILE)
    open(fn, "w") do f
        JSON3.write(f, tenants; indent=2)
    end
end

"""
Add or update a tenant entry in the broker database.
"""
function add_tenant(tenant::String, secret::String, broker_name::String="broker")
    println("Adding tenant [$tenant]")
    if haskey(ENV, "REMBUS_FILESTORE")
        tenants = load_file(broker_name)
        tenants[tenant] = secret
        save_file(broker_name, tenants)
    else
        db = Rembus.dbconnect(broker=broker_name)

        try
            DuckDB.execute(
                db,
                """
                CREATE TABLE IF NOT EXISTS tenant (
                    name   TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    secret TEXT NOT NULL
                )
                """)

            DuckDB.execute(
                db,
                """
                DELETE FROM tenant
                WHERE name = ? AND tenant = ?
                """,
                (broker_name, tenant),
            )

            DuckDB.execute(
                db,
                """
                INSERT INTO tenant (name, tenant, secret)
                VALUES (?, ?, ?)
                """,
                (broker_name, tenant, secret),
            )
        catch e
            println("Add tenant [$tenant] failed: $e")
        finally
            close(db)
        end
    end
end

"""
Remove a tenant entry from the broker database.
"""
function remove_tenant(tenant::String, broker_name::String="broker")
    println("Removing tenant [$tenant]")
    if haskey(ENV, "REMBUS_FILESTORE")
        tenants = load_file(broker_name)
        pop!(tenants, tenant, nothing)
        save_file(broker_name, tenants)
    else
        db = Rembus.dbconnect(broker=broker_name)

        try
            DuckDB.execute(
                db,
                """
                DELETE FROM tenant
                WHERE name = ? AND tenant = ?
                """,
                (broker_name, tenant),
            )
        catch e
            println("Remove tenant [$tenant] failed: $e")
        finally
            close(db)
        end
    end
end

function main()
    s = ArgParseSettings(description="Manage Rembus tenants")

    @add_arg_table s begin
        "tenant"
        help = "Tenant name"

        "--delete", "-d"
        help = "Remove tenant"
        action = :store_true

        "--secret", "-s"
        help = "Tenant secret"

        "--broker"
        help = "Broker name (default: broker)"
        default = "broker"
    end

    args = parse_args(s)

    tenant = args["tenant"]
    broker = args["broker"]

    if args["delete"]
        remove_tenant(tenant, broker)
    else
        secret = get(args, "secret", nothing)
        if secret === nothing
            error("Missing --secret when adding a tenant")
        end
        add_tenant(tenant, secret, broker)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
