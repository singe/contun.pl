#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Getopt::Long qw(GetOptions);
use Errno qw(EWOULDBLOCK EAGAIN EINTR);
use IO::Handle;

use constant MAX_BUFFER => 1024 * 1024; # 1 MiB per-direction safety limit

$SIG{PIPE} = 'IGNORE';

Getopt::Long::Configure('no_ignore_case');

my %opts = (
    'client-bind' => '127.0.0.1',
    'client-port' => undef,
    'pool-bind'   => '0.0.0.0',
    'pool-port'   => undef,
);

my $help;
GetOptions(
    'client-bind|C=s' => \$opts{'client-bind'},
    'client-port|c=i' => \$opts{'client-port'},
    'pool-bind|P=s'   => \$opts{'pool-bind'},
    'pool-port|p=i'   => \$opts{'pool-port'},
    'help|h'          => \$help,
) or die usage();

if ($help) {
    print usage();
    exit 0;
}

defined $opts{'client-port'} && defined $opts{'pool-port'}
    or die usage("Both --client-port and --pool-port are required.\n");

my $client_listener = create_listener($opts{'client-bind'}, $opts{'client-port'});
my $pool_listener   = create_listener($opts{'pool-bind'},   $opts{'pool-port'});

my $read_set  = IO::Select->new($client_listener, $pool_listener);
my $write_set = IO::Select->new();

my %ctx;
my @available_workers;
my @pending_clients;

info("Listening for clients on $opts{'client-bind'}:$opts{'client-port'}");
info("Listening for pool workers on $opts{'pool-bind'}:$opts{'pool-port'}");

while (1) {
    my ($read_ready, $write_ready) = IO::Select::select($read_set, $write_set, undef);
    next unless $read_ready || $write_ready;

    for my $fh (@{$read_ready || []}) {
        if ($fh == $client_listener) {
            accept_client();
            next;
        }
        if ($fh == $pool_listener) {
            accept_worker();
            next;
        }
        handle_readable($fh);
    }

    for my $fh (@{$write_ready || []}) {
        flush_output($fh);
    }
}

sub usage {
    my ($msg) = @_;
    my $usage = <<"END";
Usage: $0 [options]

Required:
  -c, --client-port <port>   Local jump-box port exposed to downstream clients.
  -p, --pool-port <port>     Listener port that accepts pool workers from the bastion.

Optional:
  -C, --client-bind <addr>   Address to bind for the downstream client listener (default 127.0.0.1).
  -P, --pool-bind <addr>     Address to bind for incoming pool workers (default 0.0.0.0).
  -h, --help                 Show this help and exit.

hub.pl exposes two sockets: one facing clients on the jump box, and one facing
pool workers from the bastion. Once a client connects on --client-port the hub
will pair it with the next available worker connection arriving on --pool-port
and forward bytes in both directions until either side closes.

Example:
  $0 -c 4444 -p 5555 -C 127.0.0.1 -P 0.0.0.0
END
    $usage = $msg . "\n$usage" if defined $msg && length $msg;
    return $usage;
}

sub info {
    my ($msg) = @_;
    print STDERR "[hub] $msg\n";
}

sub create_listener {
    my ($addr, $port) = @_;
    my $sock = IO::Socket::INET->new(
        LocalAddr => $addr,
        LocalPort => $port,
        Proto     => 'tcp',
        Listen    => SOMAXCONN,
        Reuse     => 1,
    ) or die "Failed to bind $addr:$port - $!";
    $sock->blocking(0);
    return $sock;
}

sub accept_client {
    while (1) {
        my $client = $client_listener->accept();
        last unless $client;
        setup_socket($client);
        my $id = fileno($client);
        $ctx{$client} = {
            type         => 'client',
            state        => 'await_worker',
            peer         => undef,
            buffer       => '',
            outbuf       => '',
            pending_data => '',
        };
        $read_set->add($client);
        info("Client connected (fd=$id)");
        enqueue_client($client);
    }
}

sub accept_worker {
    while (1) {
        my $worker = $pool_listener->accept();
        last unless $worker;
        setup_socket($worker);
        my $id = fileno($worker);
        $ctx{$worker} = {
            type   => 'worker',
            state  => 'await_hello',
            peer   => undef,
            buffer => '',
            outbuf => '',
            client => undef,
        };
        $read_set->add($worker);
        info("Worker connected (fd=$id)");
    }
}

sub setup_socket {
    my ($sock) = @_;
    $sock->autoflush(1);
    $sock->blocking(0);
    binmode($sock);
}

sub handle_readable {
    my ($sock) = @_;
    unless (exists $ctx{$sock}) {
        close_socket($sock, 'unknown socket');
        return;
    }

    my $buffer = '';
    my $bytes = sysread($sock, $buffer, 16 * 1024);
    if (!defined $bytes) {
        return if $!{EWOULDBLOCK} || $!{EAGAIN} || $!{EINTR};
        close_socket($sock, "read error: $!");
        return;
    }
    if ($bytes == 0) {
        close_socket($sock, 'peer closed');
        return;
    }

    my $entry = $ctx{$sock};
    if ($entry->{type} eq 'worker') {
        handle_worker_data($sock, $buffer);
    } elsif ($entry->{type} eq 'client') {
        handle_client_data($sock, $buffer);
    } else {
        close_socket($sock, 'invalid type');
    }
}

sub handle_worker_data {
    my ($sock, $data) = @_;
    my $entry = $ctx{$sock} or return;
    if ($entry->{state} eq 'stream') {
        forward_stream($sock, $data);
        return;
    }

    $entry->{buffer} .= $data;
    while ($entry->{buffer} =~ s/^(.*?\n)//) {
        my $line = $1;
        $line =~ s/\r?\n$//;
        my $state = $entry->{state};
        if ($state eq 'await_hello') {
            if ($line =~ /^POOL\s+1$/) {
                $entry->{state} = 'idle';
                add_available_worker($sock);
            } else {
                close_socket($sock, "unexpected hello line '$line'");
                return;
            }
        } elsif ($state eq 'await_ok') {
            if ($line eq 'OK') {
                start_stream($sock);
            } elsif ($line =~ /^ERR\b/) {
                worker_failed($sock, $line);
                return;
            } else {
                close_socket($sock, "unexpected response '$line'");
                return;
            }
        } elsif ($state eq 'idle') {
            # Ignore keepalives or noise.
        } else {
            close_socket($sock, "unexpected line in state $state");
            return;
        }
        if (!exists $ctx{$sock}) {
            return;
        }
        if ($entry->{state} eq 'stream') {
            last;
        }
    }

    if ($entry->{state} eq 'stream' && length $entry->{buffer}) {
        my $leftover = delete $entry->{buffer};
        forward_stream($sock, $leftover);
    }
}

sub handle_client_data {
    my ($sock, $data) = @_;
    my $entry = $ctx{$sock} or return;
    my $state = $entry->{state};

    if ($state eq 'await_worker' || $state eq 'await_ok') {
        append_pending($sock, $data);
        return;
    }
    if ($state eq 'stream') {
        forward_stream($sock, $data);
        return;
    }
    close_socket($sock, "client in unexpected state $state");
}

sub append_pending {
    my ($sock, $data) = @_;
    my $entry = $ctx{$sock} or return;
    my $pending = \$entry->{pending_data};
    if (length($$pending) + length($data) > MAX_BUFFER) {
        close_socket($sock, 'client pending buffer limit exceeded');
        return;
    }
    $$pending .= $data;
}

sub forward_stream {
    my ($sock, $data) = @_;
    my $entry = $ctx{$sock} or return;
    my $peer = $entry->{peer};
    unless ($peer && exists $ctx{$peer}) {
        close_socket($sock, 'missing peer');
        return;
    }
    my $target_entry = $ctx{$peer};
    my $out = \$target_entry->{outbuf};
    if (length($$out) + length($data) > MAX_BUFFER) {
        close_socket($peer, 'peer output buffer limit exceeded');
        close_socket($sock, 'output buffer limit exceeded');
        return;
    }
    $$out .= $data;
    $write_set->add($peer);
}

sub flush_output {
    my ($sock) = @_;
    my $entry = $ctx{$sock} or do {
        $write_set->remove($sock);
        return;
    };
    my $out = \$entry->{outbuf};
    return unless length $$out;

    my $written = syswrite($sock, $$out);
    if (!defined $written) {
        if ($!{EWOULDBLOCK} || $!{EAGAIN} || $!{EINTR}) {
            return;
        }
        close_socket($sock, "write error: $!");
        return;
    }
    substr($$out, 0, $written, '');
    if (length $$out == 0) {
        $write_set->remove($sock);
    }
}

sub close_socket {
    my ($sock, $reason) = @_;
    return unless $sock;
    unless (exists $ctx{$sock}) {
        eval { $read_set->remove($sock) };
        eval { $write_set->remove($sock) };
        eval { $sock->close };
        return;
    }

    my $entry = delete $ctx{$sock};
    my $fd = fileno($sock);
    eval { $read_set->remove($sock) };
    eval { $write_set->remove($sock) };
    eval { $sock->close };

    info(sprintf 'Closed %s (fd=%d): %s',
        $entry->{type} || 'socket',
        defined($fd) ? $fd : -1,
        $reason // 'unknown'
    );

    if ($entry->{type} && $entry->{type} eq 'worker') {
        remove_available_worker($sock);
        if (my $client = $entry->{client}) {
            if (exists $ctx{$client}) {
                close_socket($client, 'worker disconnected');
            }
        }
    } elsif ($entry->{type} && $entry->{type} eq 'client') {
        remove_pending_client($sock);
        if (my $worker = $entry->{peer}) {
            if (exists $ctx{$worker}) {
                close_socket($worker, 'client disconnected');
            }
        }
    }
}

sub add_available_worker {
    my ($worker) = @_;
    push @available_workers, $worker;
    dispatch_pairs();
}

sub enqueue_client {
    my ($client) = @_;
    push @pending_clients, $client;
    dispatch_pairs();
}

sub pop_available_worker {
    while (@available_workers) {
        my $worker = shift @available_workers;
        next unless $worker && exists $ctx{$worker};
        my $entry = $ctx{$worker};
        next unless $entry->{state} && $entry->{state} eq 'idle';
        return $worker;
    }
    return;
}

sub pop_pending_client {
    while (@pending_clients) {
        my $client = shift @pending_clients;
        next unless $client && exists $ctx{$client};
        my $entry = $ctx{$client};
        next unless $entry->{state} && $entry->{state} eq 'await_worker';
        return $client;
    }
    return;
}

sub remove_available_worker {
    my ($worker) = @_;
    @available_workers = grep { defined $_ && $_ != $worker } @available_workers;
}

sub remove_pending_client {
    my ($client) = @_;
    @pending_clients = grep { defined $_ && $_ != $client } @pending_clients;
}

sub dispatch_pairs {
    while (1) {
        my $worker = pop_available_worker() or last;
        my $client = pop_pending_client() or do {
            unshift @available_workers, $worker;
            last;
        };
        assign_worker($worker, $client);
    }
}

sub assign_worker {
    my ($worker, $client) = @_;
    my $worker_entry = $ctx{$worker};
    my $client_entry = $ctx{$client};
    unless ($worker_entry && $client_entry) {
        return;
    }
    $worker_entry->{state}  = 'await_ok';
    $worker_entry->{client} = $client;
    $client_entry->{state}  = 'await_ok';
    $client_entry->{worker} = $worker;
    send_control($worker, "START\n");
    info(sprintf 'Paired client fd=%d with worker fd=%d',
        fileno($client), fileno($worker));
}

sub send_control {
    my ($sock, $payload) = @_;
    my $entry = $ctx{$sock} or return;
    my $out = \$entry->{outbuf};
    $$out .= $payload;
    $write_set->add($sock);
}

sub start_stream {
    my ($worker) = @_;
    my $worker_entry = $ctx{$worker} or return;
    my $client = $worker_entry->{client};
    unless ($client && exists $ctx{$client}) {
        close_socket($worker, 'missing client for stream start');
        return;
    }
    my $client_entry = $ctx{$client};
    $worker_entry->{state} = 'stream';
    $client_entry->{state} = 'stream';
    $worker_entry->{peer}  = $client;
    $client_entry->{peer}  = $worker;

    if (length $client_entry->{pending_data}) {
        my $pending = delete $client_entry->{pending_data};
        forward_stream($client, $pending) if length $pending;
    }

    info(sprintf 'Stream active client fd=%d <-> worker fd=%d',
        fileno($client), fileno($worker));
}

sub worker_failed {
    my ($worker, $line) = @_;
    my $worker_entry = $ctx{$worker} or return;
    my $client = $worker_entry->{client};
    if ($client && exists $ctx{$client}) {
        close_socket($client, "worker error: $line");
    }
    if (exists $ctx{$worker}) {
        $ctx{$worker}->{client} = undef;
    }
    close_socket($worker, "worker error: $line");
}
