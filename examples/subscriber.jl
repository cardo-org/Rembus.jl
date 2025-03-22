using DataFrames
using Rembus

# A custom structure where announcements are stored
# just to demonstrate the symbiosis between Rembus and
# Dataframes.
mutable struct Announcements
    df::DataFrame
    Announcements() = new(DataFrame(:user => [], :message => []))
end

# A sort of global context to share state between exposed and subscribed methods.
const ANN_DB = Announcements()

# This get called for each message received on topic "announcement"
function announcement(ann_db, rb, user, message)
    println("news from [$user]: $message")
    push!(ann_db.df, [user, message])
end

# This get called for each RPC request.
# Return a Dataframe object.
all_announcements(ann_db, rb) = return ann_db.df

# Naming a component is optional, you can comment the following line.
# A component without a name became a sort of "anonymous" entity that
# assumes an ephemeral identity each time it connects to the broker.
@component "organizer"

# Subscribe to the topic "announcement". the option form declare interest
# in messages published in the past and not received by the component
# becuase it was offline.
#
# subscribe is a configuration command, to start receive messages a reactive
# command MUST be issued.
#
@subscribe announcement from = Rembus.LastReceived

#
# From now on the component may receive pub/sub message from the broker.
#
@reactive

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
