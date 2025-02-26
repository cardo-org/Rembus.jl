using DataFrames
using Rembus

# A custom structure where announcements are stored
# just to demonstrate the strong symbiosis between Rembus and
# the dataframe concept it is used a Julia DataFrame.
mutable struct Announcements
    df::DataFrame
    Announcements() = new(DataFrame(:user => [], :message => []))
end

# A sort of global context to share state between exposed and subscribed methods.
const ANN_DB = Announcements()

# This get called for each message received on topic "announcement"
function announcement(ann_db, rb, user, message)
    @info "news from [$user]: $message"
    push!(ann_db.df, [user, message])
end

# This get called for each RPC request.
# Return a Dataframe object.
all_announcements(ann_db) = return ann_db.df

# Naming a component is optional, you can comment the following line.
# A component without a name became a sort of "anonymous" entity that
# assumes an ephemeral identity each time it connects to the broker.
@component "organizer"

# Subscribe to the topic "announcement". the option before_now declare interest
# in messages published in time intervals when the component was offline.
@subscribe announcement before_now

# If a component is "anonymous", the above @component line commented out, then the
# messages published when the component is offline get lost even if @subscribe
# use the option before_now.

# Shared object between all subscribers and exposers methods.
# When declared it get used as the first argument of the exposed/subscribed methods.
@inject ANN_DB

# Expose the method that returns the announcements dataframe
@expose all_announcements

# await forever for client requests or Ctrl-C termination command.
@wait
