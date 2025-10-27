# contun.pl

contun is a set of Perl scripts to help establish a network tunnel from one host to another in situations when the host with the target service cannot bind any ports externaly and must connect out. There are typically two to three hosts involved the final target that the isolated host can access, the isolated or bastion host, and the jump box host. The end result should be that there's a port exposed on the jump box host that when connected to is proxied to the target host and port via the isolated bastion host.

jump_client_app -> jump:4444 <- jump_hub.pl -> jump:5555 <- bastion_pool.pl -> target:6666

The isolated bastion host can only operate in a connect-connect tunnel configuration (i.e. connect to the target host and port, and connect to a jump box server and port). The connections on the isolated bastion host are done with pool.pl, and the connections on the jump box host are done using hub.pl.

`hub.pl` operates in listen-listen waiting for a conection on the jump box external side, and forwarding data to the local listening side and vice versa. `pool.pl` operates in connect-connect by connecting to the jumpbox server, and once a connection is established with the hub.pl's local listen side, forwarding that to the target host and port and vice versa.

Of course, building a simple multi-hop tunnel like this could be done with socat quite easily:

jumpbox: socat tcp-listen:4444,bind=127.0.0.1 tcp-listen:5555
bastion: socat tcp:jumpbox:5555 tcp:target:6666

But, the problem you'll have is with concurrency. Even if you add the `fork` keyword to the jumpbox's localhost listener, the use of the second socat limits the ability to associate a connection across the two socat pairs.

To solve this contun's pool sets up a pool of workers on the isolated bastion host that each initiate a connection to contun's hub, which in turn can then handle multiple concurrent connections to the local listener.
