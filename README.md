# read-write-kinesis-stream

A small example of reading and writing  an AWS kinesis stream with python lambdas

For this we need 3 things:

1) A kinesis stream
2) A lambda to write data to the stream
3) A lambda to read data from the stream

You can create a kinesis stream from the aws command line but for the purposes of this demo I elected to create my
stream from the aws console. As this is just a very small demo I chose to set it up using just one shard. In kinesis streams 
you can think of a shard as a unit of processing potential. The more shards a stream has the more data it can read and write
and the faster it can process it. 

After the stream was created it was just a matter of creating two python lambdas to read and write to it. Both lambdas are quite short
and I have put plenty of comments in so won't discuss them further here. The only slightly unusual thing is that neither lambda is triggered 
by an event. They are stand-alone and can be run manually as and when required. In the real world the stream reading lambda in particular would 
probably be triggered by a kinesis stream event. The only other thing to worry about is that the lambdas obviously need permission to read
and write to kinesis. I took the easy option and extended the default lambda-execution-role to allow all access to kinesis but again in
a production system you would want to nail this down to very specific permissions.

At some point soon I will extend this example to read the stream using kinesis fire-hose and use it write the records to S3

