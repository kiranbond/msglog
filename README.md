# msglog
MsgLog implements a persistent/durable log for message streams. Streams are produced
and consumed in FIFO order. It is Kafka-esque in terms of usage model but only
on a single node when replication is not needed.

msglog is implemented using a two level abstraction. msglog itself is an interface
with the backend persistence implemented using a store (message store). The
library contains an implementation with a filesystem backend MessageStoreFS.
It is possible to use other backends like Cassandra or etcd for lower volume data
to implement a replicated message log.

Messages are streamed to persistent backend as they are written after assigning
a Message Sequence Number (MSN). The MSN is returned to the user and response
is sent on the supplied channel when the message is stored on disk.

Consumers can read the message log by supplying the MSN and the topic.
Any read with an invalid MSN returns the next available MSN in that topic so
consumers can discover the MSNs available for reading. When the consumer(s)
no longer needs a message corresponding to an MSN, it can Commit with an MSN
that will let the MsgLog clean up the log in the background.



MessageStoreFS implements the streaming message filestore. It is meant for
storing mostly fifo ordered writes and reads.

The input message has a MSN(message sequence number) and data bytes. The
data is written to file as records in an async manner and completions are
sent back to caller in a channel. An index for the records is maintained
in a MsgStoreIndex(btree). The data is stored in a file in sequential order.

When the file reaches a maximum size, data is written to a new file. When we
switch to a new file we also serialize the btree for that file so that it
can be read when reads are happening to that file.

The first index (masterIndex) tracks the list of starting MSNs for all the
files in the message store by topic.

The second level is an index for each file which tracks the offsets of each
entry within that file. Note that it is possible at any time to reconstruct
the index by fully parsing all the directory and all the files so losing
this index is a performance issue and not a correctness issue.
