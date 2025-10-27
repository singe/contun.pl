#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Getopt::Long qw(GetOptions);
use POSIX qw(:sys_wait_h);
use Errno qw(EWOULDBLOCK EAGAIN EINTR);
use Time::HiRes qw(sleep);
use IO::Handle;

use constant MAX_BUFFER => 1024 * 1024;

$SIG{PIPE} = 'IGNORE';

my %opts = (
    'hub-host'     => '127.0.0.1',
    'hub-port'     => undef,
    'target-host'  => undef,
    'target-port'  => undef,
    'workers'      => 4,
    'retry-delay'  => 1,
);

GetOptions(
    'hub-host=s'    => \$opts{'hub-host'},
    'hub-port=i'    => \$opts{'hub-port'},
    'target-host=s' => \$opts{'target-host'},
    'target-port=i' => \$opts{'target-port'},
    'workers=i'     => \$opts{'workers'},
    'retry-delay=f' => \$opts{'retry-delay'},
) or die usage();

defined $opts{'hub-port'}    or die usage("Missing required --hub-port\n");
defined $opts{'target-host'} or die usage("Missing required --target-host\n");
defined $opts{'target-port'} or die usage("Missing required --target-port\n");
$opts{'workers'} > 0         or die usage("--workers must be positive\n");

info("Starting pool with $opts{workers} worker(s) targeting $opts{'target-host'}:$opts{'target-port'} via $opts{'hub-host'}:$opts{'hub-port'}");

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
    sleep 0.5;
}

shutdown_workers();
exit 0;

sub usage {
    my ($msg) = @_;
    my $usage = <<"END";
Usage: $0 --hub-host <host> --hub-port <port> --target-host <host> --target-port <port> [--workers <n>] [--retry-delay <seconds>]
END
    $usage = $msg . $usage if defined $msg;
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
    print $hub "POOL 1\n" or die "Failed to send handshake to hub: $!";
    while (!$${exit_flag_ref}) {
        my $line = <$hub>;
        unless (defined $line) {
            info("Worker $$ hub closed connection");
            return;
        }
        $line =~ s/\r?\n$//;
        next if $line eq '';
        if ($line eq 'START') {
            my ($target, $err) = connect_to_target();
            unless ($target) {
                $err ||= 'target connect failed';
                $err =~ s/\s+/ /g;
                print $hub "ERR $err\n";
                return;
            }
            print $hub "OK\n" or die "Failed to confirm target ready: $!";
            bridge_streams($hub, $target, $exit_flag_ref);
            eval { $target->close };
            return;
        } else {
            info("Worker $$ received unexpected control '$line'");
        }
    }
}

sub connect_to_target {
    my $sock = IO::Socket::INET->new(
        PeerAddr => $opts{'target-host'},
        PeerPort => $opts{'target-port'},
        Proto    => 'tcp',
        Timeout  => 5,
    );
    unless ($sock) {
        my $err = "$!";
        info("Worker $$ failed to connect to target: $err");
        return wantarray ? (undef, $err) : undef;
    }
    $sock->autoflush(1);
    binmode($sock);
    info("Worker $$ connected to target $opts{'target-host'}:$opts{'target-port'}");
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
