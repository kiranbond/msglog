# msglog
MsgLog implements a persistent/durable log for message streams. Streams are produced
and consumed in FIFO order. It is Kafka-esque in terms of usage model but only
on a single node when replication is not needed.

Messages are streamed to persistent backend as they are written after assigning
a Message Sequence Number (MSN). The MSN is returned to the user and response
is sent on the supplied channel when the message is stored on disk.

Consumers can read the message log by supplying the MSN and the topic.
Any read with an invalid MSN returns the next available MSN in that topic so
consumers can discover the MSNs available for reading. When the consumer(s)
no longer needs a message corresponding to an MSN, it can Commit with an MSN
that will let the MsgLog clean up the log in the background.
