#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Getopt::Long qw(GetOptions);
use POSIX qw(:sys_wait_h);
use Errno qw(EWOULDBLOCK EAGAIN EINTR);
use IO::Handle;
use Socket qw(AF_INET AF_INET6 inet_ntop inet_pton);

use constant MAX_BUFFER      => 1024 * 1024;

$SIG{PIPE} = 'IGNORE';

Getopt::Long::Configure('no_ignore_case');

my %opts = (
    'hub-host'     => '127.0.0.1',
    'hub-port'     => undef,
    'target-host'  => undef,
    'target-port'  => undef,
    'workers'      => 4,
    'retry-delay'  => 1,
    'mode'         => 'direct',
);

my $help;
GetOptions(
    'hub-host|j=s'    => \$opts{'hub-host'},
    'hub-port|p=i'    => \$opts{'hub-port'},
    'target-host|t=s' => \$opts{'target-host'},
    'target-port|T=i' => \$opts{'target-port'},
    'workers|w=i'     => \$opts{'workers'},
    'retry-delay|r=f' => \$opts{'retry-delay'},
    'mode|m=s'        => \$opts{'mode'},
    'help|h'          => \$help,
) or die usage();

if ($help) {
    print usage();
    exit 0;
}

defined $opts{'hub-port'}    or die usage("Missing required --hub-port\n");
$opts{'workers'} > 0         or die usage("--workers must be positive\n");

$opts{'mode'} = lc $opts{'mode'};
$opts{'mode'} =~ /^(direct|socks)$/
    or die usage("--mode must be either direct or socks\n");

if ($opts{'mode'} eq 'direct') {
    defined $opts{'target-host'} or die usage("Missing required --target-host for direct mode\n");
    defined $opts{'target-port'} or die usage("Missing required --target-port for direct mode\n");
} else {
    if (defined $opts{'target-host'} || defined $opts{'target-port'}) {
        die usage("--target-host/--target-port are not used in socks mode\n");
    }
}

my $direct_dest;
if ($opts{'mode'} eq 'direct') {
    $direct_dest = prepare_direct_destination($opts{'target-host'}, $opts{'target-port'});
    info("Starting pool with $opts{workers} worker(s) in direct mode targeting $opts{'target-host'}:$opts{'target-port'} via $opts{'hub-host'}:$opts{'hub-port'}");
} else {
    info("Starting pool with $opts{workers} worker(s) in socks mode via $opts{'hub-host'}:$opts{'hub-port'}");
}

my %children;
my $terminate = 0;

$SIG{INT} = $SIG{TERM} = sub {
    $terminate = 1;
};

while (!$terminate) {
    while (!$terminate && scalar keys %children < $opts{workers}) {
        spawn_worker();
    }

    last if $terminate;

    my $pid = waitpid(-1, WNOHANG);
    if ($pid > 0) {
        delete $children{$pid};
        next;
    }
    sleep 1;
}

shutdown_workers();
exit 0;

sub usage {
    my ($msg) = @_;
    my $usage = <<"END";
Usage: $0 [options]

Required:
  -j, --hub-host <host>      Hub listener hostname or IP address (default 127.0.0.1).
  -p, --hub-port <port>      Hub listener port accepting pool workers.
  -m, --mode <mode>          Operation mode: direct or socks (default direct).

Direct mode:
  -t, --target-host <host>   Target hostname or IP the bastion can reach.
  -T, --target-port <port>   Target port to proxy traffic to.

Optional:
  -w, --workers <n>          Number of concurrent worker processes to keep alive (default 4).
  -r, --retry-delay <sec>    Seconds to wait before re-dialling the hub after a failure (default 1).
  -h, --help                 Show this help message and exit.

pool.pl maintains a pool of outbound connections from the bastion to the hub.
In direct mode each worker declares a fixed target and repeatedly proxies
streams to that host:port. In socks mode workers wait for the hub to supply a
destination on every session, allowing downstream SOCKS5 clients to choose their
own targets.

Example:
  $0 -j jumpbox -p 5555 -m direct -t target.internal -T 6666 -w 8
  $0 -j jumpbox -p 5555 -m socks -w 4
END
    $usage = $msg . "\n$usage" if defined $msg && length $msg;
    return $usage;
}

sub info {
    my ($msg) = @_;
    print STDERR "[pool] $msg\n";
}

sub spawn_worker {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        worker_process();
        exit 0;
    }
    $children{$pid} = 1;
    info("Spawned worker pid=$pid");
}

sub shutdown_workers {
    return unless %children;
    info("Shutting down workers");
    kill 'TERM', keys %children;
    while (%children) {
        my $pid = wait();
        last if $pid < 0;
        delete $children{$pid};
    }
}

sub worker_process {
    my $child_exit = 0;
    local $SIG{INT} = sub { $child_exit = 1; };
    local $SIG{TERM} = sub { $child_exit = 1; };

    while (!$child_exit) {
        my $hub = connect_to_hub();
        unless ($hub) {
            sleep $opts{'retry-delay'};
            next;
        }
        eval {
            handle_hub_session($hub, \$child_exit);
        };
        if ($@) {
            info("Worker $$ session error: $@");
        }
        eval { $hub->close if $hub };
        last if $child_exit;
        sleep $opts{'retry-delay'} if !$child_exit;
    }
    info("Worker $$ exiting");
}

sub connect_to_hub {
    my $sock = IO::Socket::INET->new(
        PeerAddr => $opts{'hub-host'},
        PeerPort => $opts{'hub-port'},
        Proto    => 'tcp',
        Timeout  => 5,
    );
    unless ($sock) {
        info("Worker $$ failed to connect to hub: $!");
        return;
    }
    $sock->autoflush(1);
    binmode($sock);
    info("Worker $$ connected to hub");
    return $sock;
}

sub handle_hub_session {
    my ($hub, $exit_flag_ref) = @_;
    perform_handshake($hub);
    while (!$${exit_flag_ref}) {
        my $line = <$hub>;
        unless (defined $line) {
            info("Worker $$ hub closed connection");
            return;
        }
        $line =~ s/\r?\n$//;
        next if $line eq '';
        if ($line =~ /^REQUEST\s+CONNECT\s+(\S+)\s+(\S+)\s+(\d+)$/) {
            my ($atype, $addr_text, $port_str) = ($1, $2, $3);
            my $port = $port_str + 0;
            if ($port < 1 || $port > 65535) {
                info("Worker $$ received invalid port '$port_str'");
                send_reply($hub, 1, 'ipv4', '0.0.0.0', 0);
                next;
            }
            my ($host, $err) = validate_address_text($atype, $addr_text);
            unless (defined $host) {
                info("Worker $$ failed to decode destination: $err");
                send_reply($hub, 1, 'ipv4', '0.0.0.0', 0);
                next;
            }
            if ($opts{'mode'} eq 'direct') {
                if (!$direct_dest || $host ne $direct_dest->{host} || $port != $direct_dest->{port}) {
                    info("Worker $$ rejecting mismatched request $host:$port (expected $direct_dest->{host}:$direct_dest->{port})");
                    send_reply($hub, 1, 'ipv4', '0.0.0.0', 0);
                    next;
                }
            }
            my ($target, $connect_err) = connect_to_target($host, $port);
            unless ($target) {
                my $status = map_error_to_status($connect_err);
                info("Worker $$ failed to connect to $host:$port: $connect_err");
                send_reply($hub, $status, 'ipv4', '0.0.0.0', 0);
                next;
            }
            info("Worker $$ bridged to $host:$port");
            send_reply($hub, 0, 'ipv4', '0.0.0.0', 0);
            bridge_streams($hub, $target, $exit_flag_ref);
            eval { $target->close };
            $hub->blocking(1);
            last if $${exit_flag_ref};
        } else {
            info("Worker $$ received unexpected control '$line'");
        }
    }
}

sub connect_to_target {
    my ($host, $port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    );
    unless ($sock) {
        my $err = "$!";
        return wantarray ? (undef, $err) : undef;
    }
    $sock->autoflush(1);
    binmode($sock);
    info("Worker $$ connected to target $host:$port");
    return wantarray ? ($sock, undef) : $sock;
}

sub bridge_streams {
    my ($hub, $target, $exit_flag_ref) = @_;
    set_nonblocking($hub);
    set_nonblocking($target);

    my $read_set  = IO::Select->new($hub, $target);
    my $write_set = IO::Select->new();
    my %outbuf = (
        $hub    => '',
        $target => '',
    );

    while (!$${exit_flag_ref}) {
        my ($rready, $wready) = IO::Select::select($read_set, $write_set, undef, 0.5);
        $rready ||= [];
        $wready ||= [];
        if (@$rready) {
            for my $fh (@$rready) {
                my $peer = $fh == $hub ? $target : $hub;
                my $buf  = '';
                my $bytes = sysread($fh, $buf, 16 * 1024);
                if (!defined $bytes) {
                    next if $!{EWOULDBLOCK} || $!{EAGAIN} || $!{EINTR};
                    info("Worker $$ read error: $!");
                    return;
                }
                if ($bytes == 0) {
                    info("Worker $$ stream closed");
                    return;
                }
                if (length($outbuf{$peer}) + length($buf) > MAX_BUFFER) {
                    info("Worker $$ buffer limit hit, tearing down tunnel");
                    return;
                }
                $outbuf{$peer} .= $buf;
                $write_set->add($peer);
            }
        }
        if (@$wready) {
            for my $fh (@$wready) {
                next unless length $outbuf{$fh};
                my $written = syswrite($fh, $outbuf{$fh});
                if (!defined $written) {
                    next if $!{EWOULDBLOCK} || $!{EAGAIN} || $!{EINTR};
                    info("Worker $$ write error: $!");
                    return;
                }
                substr($outbuf{$fh}, 0, $written, '');
                $write_set->remove($fh) unless length $outbuf{$fh};
            }
        }
    }
}

sub set_nonblocking {
    my ($sock) = @_;
    $sock->blocking(0);
}

sub perform_handshake {
    my ($hub) = @_;
    my $mode = $opts{'mode'};
    my $hello = "HELLO 1 $mode";
    if ($mode eq 'direct') {
        my $dest = $direct_dest or die "direct destination not prepared";
        $hello .= sprintf ' DEST %s %s %d', $dest->{atype}, $dest->{host}, $dest->{port};
    }
    print $hub "$hello\n" or die "Failed to send handshake to hub: $!";
    my $resp = <$hub>;
    die "Hub closed during handshake" unless defined $resp;
    $resp =~ s/\r?\n$//;
    if ($resp ne 'OK') {
        die "Hub rejected handshake: $resp";
    }
}

sub send_reply {
    my ($hub, $status, $atype, $addr_text, $port) = @_;
    printf {$hub} "REPLY %d %s %s %d\n", $status, $atype, $addr_text, $port
        or die "Failed to send reply to hub: $!";
}

sub prepare_direct_destination {
    my ($host, $port) = @_;
    my $atype = classify_host_type($host);
    return {
        host  => $host,
        port  => $port + 0,
        atype => $atype,
    };
}

sub classify_host_type {
    my ($host) = @_;
    return 'ipv4' if inet_pton(AF_INET, $host);
    return 'ipv6' if inet_pton(AF_INET6, $host);
    return 'domain';
}

sub map_error_to_status {
    my ($err) = @_;
    return 1 unless defined $err;
    return 5 if $err =~ /refused/i;
    return 3 if $err =~ /network.*unreachable/i;
    return 4 if $err =~ /host.*unreachable/i;
    return 4 if $err =~ /no route/i;
    return 4 if $err =~ /timed? out/i;
    return 1;
}

sub validate_address_text {
    my ($atype, $text) = @_;
    if ($atype eq 'ipv4') {
        my $raw = inet_pton(AF_INET, $text);
        return ($text, undef) if $raw && length $raw == 4;
        return (undef, "invalid IPv4 address '$text'");
    }
    if ($atype eq 'ipv6') {
        my $raw = inet_pton(AF_INET6, $text);
        return ($text, undef) if $raw && length $raw == 16;
        return (undef, "invalid IPv6 address '$text'");
    }
    if ($atype eq 'domain') {
        return (undef, 'domain empty') unless length $text;
        return (undef, 'domain too long') if length($text) > 255;
        return ($text, undef);
    }
    return (undef, "unknown address type '$atype'");
}
