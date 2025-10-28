#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Getopt::Long qw(GetOptions);
use Errno qw(EWOULDBLOCK EAGAIN EINTR);
use IO::Handle;
use Socket qw(AF_INET AF_INET6 inet_ntop inet_pton);
use MIME::Base64 qw(encode_base64 decode_base64);

use constant MAX_BUFFER => 1024 * 1024; # 1 MiB per-direction safety limit

$SIG{PIPE} = 'IGNORE';

Getopt::Long::Configure('no_ignore_case');

my %opts = (
    'client-bind' => '127.0.0.1',
    'client-port' => undef,
    'pool-bind'   => '0.0.0.0',
    'pool-port'   => undef,
    'mode'        => 'auto',
);

my $help;
GetOptions(
    'client-bind|C=s' => \$opts{'client-bind'},
    'client-port|c=i' => \$opts{'client-port'},
    'pool-bind|P=s'   => \$opts{'pool-bind'},
    'pool-port|p=i'   => \$opts{'pool-port'},
    'mode|m=s'        => \$opts{'mode'},
    'help|h'          => \$help,
) or die usage();

if ($help) {
    print usage();
    exit 0;
}

defined $opts{'client-port'} && defined $opts{'pool-port'}
    or die usage("Both --client-port and --pool-port are required.\n");

my $configured_mode = lc $opts{'mode'};
$configured_mode =~ /^(auto|direct|socks)$/
    or die usage("--mode must be one of auto, direct, socks\n");
my $active_mode = $configured_mode eq 'auto' ? undef : $configured_mode;

my $client_listener = create_listener($opts{'client-bind'}, $opts{'client-port'});
my $pool_listener   = create_listener($opts{'pool-bind'},   $opts{'pool-port'});

my $read_set  = IO::Select->new($client_listener, $pool_listener);
my $write_set = IO::Select->new();

my %ctx;
my @available_workers;
my @pending_clients;
my @await_mode_clients;

info("Listening for clients on $opts{'client-bind'}:$opts{'client-port'}");
info("Listening for pool workers on $opts{'pool-bind'}:$opts{'pool-port'}");
info("Configured mode: $configured_mode");
info("Active mode pinned to $active_mode") if defined $active_mode;

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
  -m, --mode <mode>          Operation mode: auto, direct, or socks (default auto).
  -h, --help                 Show this help and exit.

hub.pl exposes two sockets: one facing clients on the jump box, and one facing
pool workers from the bastion. Once a client connects on --client-port the hub
will pair it with the next available worker connection arriving on --pool-port
and forward bytes in both directions until either side closes. In direct mode
the downstream client speaks plain TCP; in socks mode the hub terminates the
SOCKS5 handshake before bridging the stream.

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

sub commit_active_mode {
    my ($mode) = @_;
    return if defined $active_mode;
    $active_mode = $mode;
    info("Active mode set to $active_mode");

    my @clients = @await_mode_clients;
    @await_mode_clients = ();
    my @to_enqueue;
    for my $client (@clients) {
        next unless $client && exists $ctx{$client};
        my $entry = $ctx{$client};
        next unless $entry->{type} && $entry->{type} eq 'client';
        next unless $entry->{state} && $entry->{state} eq 'await_mode';
        if ($active_mode eq 'direct') {
            $entry->{state} = 'await_worker';
            push @to_enqueue, $client;
        } elsif ($active_mode eq 'socks') {
            $entry->{state} = 'await_greeting';
            $entry->{socks_stage} = 'greeting';
            if (length $entry->{pending_data}) {
                $entry->{buffer} .= delete $entry->{pending_data};
            }
            process_socks_state($client);
        } else {
            close_socket($client, "unsupported active mode $active_mode");
        }
    }
    enqueue_client($_) for @to_enqueue;
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
            state        => $active_mode ? ($active_mode eq 'socks' ? 'await_greeting' : 'await_worker') : 'await_mode',
            peer         => undef,
            buffer       => '',
            outbuf       => '',
            pending_data => '',
            requested_dest => undef,
            socks_stage    => undef,
        };
        $read_set->add($client);
        info("Client connected (fd=$id)");
        if ($ctx{$client}->{state} eq 'await_worker') {
            enqueue_client($client);
        } elsif ($ctx{$client}->{state} eq 'await_greeting') {
            $ctx{$client}->{socks_stage} = 'greeting';
        } else {
            push @await_mode_clients, $client;
        }
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
            mode   => undef,
            dest   => undef,
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
    if ($entry->{state} && $entry->{state} eq 'stream') {
        forward_stream($sock, $data);
        return;
    }

    $entry->{buffer} .= $data;
    while ($entry->{buffer} =~ s/^(.*?\n)//) {
        my $line = $1;
        $line =~ s/\r?\n$//;
        my $state = $entry->{state};
        if ($state eq 'await_hello') {
            process_worker_hello($sock, $line);
        } elsif ($state eq 'await_reply') {
            process_worker_reply($sock, $line);
        } elsif ($state eq 'idle') {
            # Ignore keepalives or noise.
        } else {
            close_socket($sock, "unexpected line in state $state");
            return;
        }
        if (!exists $ctx{$sock}) {
            return;
        }
        $entry = $ctx{$sock} or return;
        if ($entry->{state} && $entry->{state} eq 'stream') {
            last;
        }
    }

    if ($entry->{state} && $entry->{state} eq 'stream' && length $entry->{buffer}) {
        my $leftover = delete $entry->{buffer};
        forward_stream($sock, $leftover);
    }
}

sub process_worker_hello {
    my ($sock, $line) = @_;
    my $entry = $ctx{$sock} or return;
    my @parts = split /\s+/, $line;
    if (@parts < 3 || $parts[0] ne 'HELLO' || $parts[1] ne '1') {
        close_socket($sock, "unexpected hello line '$line'");
        return;
    }
    my $mode = lc $parts[2];
    unless ($mode eq 'direct' || $mode eq 'socks') {
        close_socket($sock, "unsupported worker mode '$mode'");
        return;
    }

    my $dest;
    if ($mode eq 'direct') {
        unless (@parts == 7 && $parts[3] eq 'DEST') {
            close_socket($sock, "direct mode requires DEST parameters");
            return;
        }
        my ($atype, $addr_blob, $port_str) = @parts[4..6];
        my $port = $port_str =~ /^\d+$/ ? $port_str + 0 : -1;
        if ($port < 1 || $port > 65535) {
            close_socket($sock, "invalid direct destination port '$port_str'");
            return;
        }
        my ($host, undef, $err) = decode_address_blob($atype, $addr_blob);
        if (!defined $host) {
            close_socket($sock, "invalid direct destination address: $err");
            return;
        }
        $dest = {
            atype => $atype,
            host  => $host,
            port  => $port,
            blob  => $addr_blob,
        };
    } elsif (@parts > 3) {
        close_socket($sock, "unexpected tokens in socks mode hello");
        return;
    }

    if (defined $active_mode) {
        if ($active_mode ne $mode) {
            info(sprintf 'Rejecting worker fd=%d with mode %s (hub mode %s)',
                fileno($sock), $mode, $active_mode);
            close_socket($sock, 'mode mismatch');
            return;
        }
    } else {
        if ($configured_mode eq 'auto') {
            commit_active_mode($mode);
        } else {
            $active_mode = $mode;
        }
    }

    $entry->{state} = 'idle';
    $entry->{mode}  = $mode;
    $entry->{dest}  = $dest if $dest;
    $entry->{buffer} = '';
    send_control($sock, "OK\n");
    if ($mode eq 'direct') {
        info(sprintf 'Worker fd=%d registered direct target %s',
            fileno($sock), format_dest($dest));
    } else {
        info(sprintf 'Worker fd=%d registered in socks mode', fileno($sock));
    }
    add_available_worker($sock);
}

sub process_worker_reply {
    my ($sock, $line) = @_;
    my $entry = $ctx{$sock} or return;
    my @parts = split /\s+/, $line;
    unless (@parts >= 2 && $parts[0] eq 'REPLY') {
        if (@parts >= 1 && $parts[0] eq 'ERR') {
            my $reason = join ' ', @parts[1..$#parts];
            handle_worker_failure($sock, 1, 'ipv4', undef, 0);
            info(sprintf 'Worker fd=%d reported ERR %s', fileno($sock), $reason);
            return;
        }
        close_socket($sock, "unexpected worker response '$line'");
        return;
    }
    my $status_token = $parts[1];
    my ($atype, $addr_blob, $port_str) = @parts[2,3,4];
    $atype     ||= 'ipv4';
    $addr_blob ||= encode_base64(pack('N', 0), '');
    $port_str  ||= '0';
    my $port    = $port_str =~ /^\d+$/ ? $port_str + 0 : 0;

    my ($bind_host, $bind_raw, $err) = decode_address_blob($atype, $addr_blob);
    if (!defined $bind_host) {
        $bind_host = '0.0.0.0';
        $bind_raw  = "\0\0\0\0";
        $atype     = 'ipv4';
        $port      = 0;
    }

    my $client = $entry->{client};
    unless ($client && exists $ctx{$client}) {
        close_socket($sock, 'missing client for reply');
        return;
    }

    my $status_num = $status_token =~ /^\d+$/ ? $status_token + 0 : 0;

    if ($status_num == 0) {
        my $reply = {
            status   => 0,
            atype    => $atype,
            raw_addr => $bind_raw,
            port     => $port,
        };
        $entry->{buffer} = '';
        start_stream($sock, $reply);
    } else {
        handle_worker_failure($sock, $status_num, $atype, $bind_raw, $port);
    }
}

sub handle_client_data {
    my ($sock, $data) = @_;
    my $entry = $ctx{$sock} or return;
    my $state = $entry->{state};

    if ($state && $state eq 'await_mode') {
        append_pending($sock, $data);
        return;
    }
    if ($active_mode && $active_mode eq 'socks' &&
        ($state && ($state eq 'await_greeting' || $state eq 'await_request'))) {
        $entry->{buffer} .= $data;
        process_socks_state($sock);
        return;
    }
    if ($state && ($state eq 'await_worker' || $state eq 'await_reply')) {
        append_pending($sock, $data);
        return;
    }
    if ($state && $state eq 'stream') {
        forward_stream($sock, $data);
        return;
    }
    close_socket($sock, "client in unexpected state $state");
}

sub process_socks_state {
    my ($sock) = @_;
    my $entry = $ctx{$sock} or return;
    while (1) {
        my $state = $entry->{state} // '';
        if ($state eq 'await_greeting') {
            my $buflen = length $entry->{buffer};
            return if $buflen < 2;
            my ($ver, $nmethods) = unpack 'C2', substr($entry->{buffer}, 0, 2);
            if ($ver != 5) {
                send_raw_socks_failure($sock, 1, 'unsupported socks version');
                return;
            }
            my $needed = 2 + $nmethods;
            return if $buflen < $needed;
            my $methods = substr($entry->{buffer}, 2, $nmethods);
            substr($entry->{buffer}, 0, $needed, '');
            if (index($methods, "\x00") < 0) {
                send_raw_socks_failure($sock, 0xFF, 'no supported auth methods');
                return;
            }
            send_control($sock, "\x05\x00");
            $entry->{state} = 'await_request';
            next;
        } elsif ($state eq 'await_request') {
            my $buflen = length $entry->{buffer};
            return if $buflen < 4;
            my ($ver, $cmd, $rsv, $atype_byte) = unpack 'C4', substr($entry->{buffer}, 0, 4);
            if ($ver != 5) {
                send_raw_socks_failure($sock, 1, 'unsupported socks version');
                return;
            }
            if ($cmd != 1) {
                send_raw_socks_failure($sock, 7, 'command not supported');
                return;
            }
            my $offset = 4;
            my $atype;
            my $addr;
            if ($atype_byte == 1) {
                $atype = 'ipv4';
                return if $buflen < $offset + 4 + 2;
                my $raw = substr($entry->{buffer}, $offset, 4);
                $addr = inet_ntop(AF_INET, $raw);
                unless (defined $addr) {
                    send_raw_socks_failure($sock, 1, 'invalid IPv4 address');
                    return;
                }
                $offset += 4;
            } elsif ($atype_byte == 3) {
                return if $buflen < $offset + 1;
                my $len = ord substr($entry->{buffer}, $offset, 1);
                $offset += 1;
                return if $buflen < $offset + $len + 2;
                $atype = 'domain';
                $addr = substr($entry->{buffer}, $offset, $len);
                $offset += $len;
            } elsif ($atype_byte == 4) {
                $atype = 'ipv6';
                return if $buflen < $offset + 16 + 2;
                my $raw = substr($entry->{buffer}, $offset, 16);
                $addr = inet_ntop(AF_INET6, $raw);
                unless (defined $addr) {
                    send_raw_socks_failure($sock, 1, 'invalid IPv6 address');
                    return;
                }
                $offset += 16;
            } else {
                send_raw_socks_failure($sock, 8, 'address type not supported');
                return;
            }
            my $port_bytes = substr($entry->{buffer}, $offset, 2);
            my $port = unpack('n', $port_bytes);
            $offset += 2;
            substr($entry->{buffer}, 0, $offset, '');

            my ($blob, undef, $err) = encode_address_blob($atype, $addr);
            if (!defined $blob) {
                send_raw_socks_failure($sock, 1, "invalid address: $err");
                return;
            }

            $entry->{requested_dest} = {
                atype => $atype,
                host  => $addr,
                port  => $port,
                blob  => $blob,
            };
            $entry->{state} = 'await_worker';
            $entry->{socks_stage} = 'await_reply';
            if (length $entry->{buffer}) {
                my $leftover = delete $entry->{buffer};
                append_pending($sock, $leftover);
            }
            enqueue_client($sock);
            return;
        } else {
            return;
        }
    }
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
        remove_await_mode_client($sock);
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

sub remove_await_mode_client {
    my ($client) = @_;
    @await_mode_clients = grep { defined $_ && $_ != $client } @await_mode_clients;
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
    my $request = build_request_command($worker_entry, $client_entry);
    unless (defined $request) {
        close_socket($client, 'unable to build request for worker');
        return unless exists $ctx{$worker};
        $worker_entry->{state}  = 'idle';
        $worker_entry->{client} = undef;
        add_available_worker($worker);
        return;
    }
    $worker_entry->{state}  = 'await_reply';
    $worker_entry->{client} = $client;
    $client_entry->{state}  = 'await_reply';
    $client_entry->{worker} = $worker;
    send_control($worker, "$request\n");
    my $dest = $client_entry->{requested_dest};
    my $dest_str = $dest ? format_dest($dest) : 'unknown';
    info(sprintf 'Paired client fd=%d with worker fd=%d',
        fileno($client), fileno($worker));
    info("Requesting CONNECT to $dest_str");
}

sub send_control {
    my ($sock, $payload) = @_;
    my $entry = $ctx{$sock} or return;
    my $out = \$entry->{outbuf};
    $$out .= $payload;
    $write_set->add($sock);
}

sub build_request_command {
    my ($worker_entry, $client_entry) = @_;
    my $mode = $worker_entry->{mode} || $active_mode || 'direct';
    my $dest;
    if ($mode eq 'direct') {
        $dest = $worker_entry->{dest};
        return unless $dest && $dest->{blob} && $dest->{port};
        $client_entry->{requested_dest} = {
            atype => $dest->{atype},
            host  => $dest->{host},
            port  => $dest->{port},
            blob  => $dest->{blob},
        };
    } else {
        $dest = $client_entry->{requested_dest};
        return unless $dest && $dest->{blob} && $dest->{port};
    }
    my $atype = $dest->{atype};
    my $blob  = $dest->{blob};
    my $port  = $dest->{port};
    return sprintf 'REQUEST CONNECT %s %s %d', $atype, $blob, $port;
}

sub format_dest {
    my ($dest) = @_;
    return 'unknown' unless $dest && $dest->{host} && $dest->{port};
    return "$dest->{host}:$dest->{port}";
}

sub start_stream {
    my ($worker, $reply) = @_;
    my $worker_entry = $ctx{$worker} or return;
    my $client = $worker_entry->{client};
    unless ($client && exists $ctx{$client}) {
        close_socket($worker, 'missing client for stream start');
        return;
    }
    my $client_entry = $ctx{$client};

    if ($active_mode && $active_mode eq 'socks') {
        send_socks_reply($client, 0, $reply);
    }

    $worker_entry->{state} = 'stream';
    $client_entry->{state} = 'stream';
    $worker_entry->{peer}  = $client;
    $client_entry->{peer}  = $worker;

    if (length $client_entry->{pending_data}) {
        my $pending = delete $client_entry->{pending_data};
        forward_stream($client, $pending) if length $pending;
    }

    my $dest = $client_entry->{requested_dest};
    info(sprintf 'Stream active client fd=%d <-> worker fd=%d (%s)',
        fileno($client), fileno($worker), format_dest($dest));
}

sub handle_worker_failure {
    my ($worker, $status, $atype, $addr_raw, $port) = @_;
    my $worker_entry = $ctx{$worker} or return;
    my $client = $worker_entry->{client};
    my $status_num = defined $status ? int($status) : 1;
    info(sprintf 'Worker fd=%d reported failure status=%d',
        fileno($worker), $status_num);

    if ($client && exists $ctx{$client}) {
        if ($active_mode && $active_mode eq 'socks') {
            send_raw_socks_failure($client, $status_num, "worker failure status=$status_num");
        } else {
            close_socket($client, "worker failure status=$status_num");
        }
    }

    return unless exists $ctx{$worker};
    close_socket($worker, "worker failure status=$status_num");
}

sub send_socks_reply {
    my ($client, $status, $details) = @_;
    my $scode = normalize_socks_status($status);
    my $atype = $details && $details->{atype} ? $details->{atype} : 'ipv4';
    my $raw   = $details && $details->{raw_addr} ? $details->{raw_addr} : '';
    my $port  = $details && defined $details->{port} ? $details->{port} : 0;
    my $atype_byte = socks_byte_from_atype($atype);
    my $addr_bytes;
    if ($atype eq 'ipv4') {
        $addr_bytes = (defined $raw && length $raw == 4) ? $raw : "\0\0\0\0";
    } elsif ($atype eq 'ipv6') {
        $addr_bytes = (defined $raw && length $raw == 16) ? $raw : ("\0" x 16);
    } elsif ($atype eq 'domain') {
        my $domain = defined $raw ? $raw : '';
        $domain = '' if length($domain) > 255;
        $addr_bytes = chr(length($domain)) . $domain;
    } else {
        $atype_byte = 1;
        $addr_bytes = "\0\0\0\0";
    }
    my $payload = pack('C C C C', 5, $scode, 0, $atype_byte) . $addr_bytes . pack('n', $port);
    send_control($client, $payload);
}

sub send_raw_socks_failure {
    my ($sock, $status, $reason) = @_;
    my $scode = normalize_socks_status($status);
    my $payload = pack('C C C C', 5, $scode, 0, 1) . "\0\0\0\0" . pack('n', 0);
    eval { syswrite($sock, $payload); };
    close_socket($sock, $reason || 'SOCKS failure');
}

sub normalize_socks_status {
    my ($status) = @_;
    return 0 unless defined $status;
    $status = int($status);
    if ($status < 0) {
        return 1;
    }
    return $status <= 0xFF ? $status : 1;
}

sub socks_byte_from_atype {
    my ($atype) = @_;
    return 1 if $atype && $atype eq 'ipv4';
    return 4 if $atype && $atype eq 'ipv6';
    return 3 if $atype && $atype eq 'domain';
    return 1;
}

sub encode_address_blob {
    my ($atype, $addr) = @_;
    if ($atype eq 'ipv4') {
        my $raw = inet_pton(AF_INET, $addr);
        return (undef, undef, "invalid IPv4 address '$addr'") unless $raw && length $raw == 4;
        return (encode_base64($raw, ''), $raw, undef);
    }
    if ($atype eq 'ipv6') {
        my $raw = inet_pton(AF_INET6, $addr);
        return (undef, undef, "invalid IPv6 address '$addr'") unless $raw && length $raw == 16;
        return (encode_base64($raw, ''), $raw, undef);
    }
    if ($atype eq 'domain') {
        return (undef, undef, 'domain name too long') if length($addr) > 255;
        return (encode_base64($addr, ''), $addr, undef);
    }
    return (undef, undef, "unknown address type '$atype'");
}

sub decode_address_blob {
    my ($atype, $blob) = @_;
    my $raw;
    eval { $raw = decode_base64($blob); 1 } or return (undef, undef, 'invalid base64');
    if ($atype eq 'ipv4') {
        return (undef, undef, 'invalid IPv4 length') unless defined $raw && length $raw == 4;
        my $addr = inet_ntop(AF_INET, $raw);
        return ($addr, $raw, undef);
    }
    if ($atype eq 'ipv6') {
        return (undef, undef, 'invalid IPv6 length') unless defined $raw && length $raw == 16;
        my $addr = inet_ntop(AF_INET6, $raw);
        return ($addr, $raw, undef);
    }
    if ($atype eq 'domain') {
        return (undef, undef, 'domain too long') if length($raw) > 255;
        return ($raw, $raw, undef);
    }
    return (undef, undef, "unknown address type '$atype'");
}
