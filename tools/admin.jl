#!/usr/bin/env julia

using ArgParse
using DuckDB
using Rembus


function load_file(broker_name)
    dir = broker_dir(broker_name)
    mkpath(dir)
    fn = joinpath(dir, Rembus.ADMINS_FILE)
    admins = Set{String}()
    if isfile(fn)
        admins = JSON3.read(read(fn, String), Set{String})
    end

    return admins
end

function save_file(broker_name, admins)
    fn = joinpath(broker_dir(broker_name), Rembus.ADMINS_FILE)
    open(fn, "w") do f
        JSON3.write(f, admins)
    end
end

"""
Add an admin entry to the broker database.
"""
function add_admin(name::String, broker_name::String="broker")
    println("Adding [$name] as admin")
    if haskey(ENV, "REMBUS_FILESTORE")
        admins = load_file(broker_name)
        push!(admins, name)
        save_file(broker_name, admins)
    else
        db = Rembus.dbconnect(broker=broker_name)

        try
            DuckDB.execute(
                db,
                """
                CREATE TABLE IF NOT EXISTS admin (
                    name TEXT NOT NULL,
                    twin TEXT
                )
                """
            )

            DuckDB.execute(
                db,
                """
                DELETE FROM admin
                WHERE name = ? AND twin = ?
                """,
                (broker_name, name),
            )

            DuckDB.execute(
                db,
                """
                INSERT INTO admin (name, twin)
                VALUES (?, ?)
                """,
                (broker_name, name),
            )
        catch e
            println("Add admin [$name] failed: $e")
        finally
            close(db)
        end
    end
end

"""
Remove an admin entry from the broker database.
"""
function remove_admin(name::String, broker_name::String="broker")
    println("Removing [$name] as admin")
    if haskey(ENV, "REMBUS_FILESTORE")
        admins = load_file(broker_name)
        pop!(admins, name, nothing)
        save_file(broker_name, admins)
    else
        db = Rembus.dbconnect(broker=broker_name)

        try
            DuckDB.execute(
                db,
                """
                DELETE FROM admin
                WHERE name = ? AND twin = ?
                """,
                (broker_name, name),
            )
        catch e
            println("Remove admin [$name] failed: $e")
        finally
            close(db)
        end
    end
end

function main()
    s = ArgParseSettings(description="Manage Rembus admins")

    @add_arg_table s begin
        "admin_name"
        help = "Admin name"

        "--delete", "-d"
        help = "Remove admin"
        action = :store_true

        "--broker"
        help = "Broker name (default: broker)"
        default = "broker"
    end

    args = parse_args(s)

    name = args["admin_name"]
    broker = args["broker"]

    if args["delete"]
        remove_admin(name, broker)
    else
        add_admin(name, broker)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
