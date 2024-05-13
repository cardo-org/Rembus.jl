using Rembus

function df_service(session, df)
    @info "df_service isauthorized=$(isauthorized(session)): $df"
    isauthorized(session) || error("unauthorized")
    return df
end

function rpc_service(data)
end

function start_server()
    rb = embedded()
    provide(rb, df_service)
    provide(rb, rpc_service)
    serve(rb)
end

start_server()
