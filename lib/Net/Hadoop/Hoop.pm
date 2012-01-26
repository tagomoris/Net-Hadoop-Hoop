package Net::Hadoop::Hoop;

use strict;
use warnings;
use Carp;

use URI::Escape qw//;
use JSON::XS qw//;

use Furl;


sub new {
    my ($this, %opts) = @_;
    croak "Haoop pseudo username missing" unless defined $opts{username};

    my $self = +{
        server => $opts{server} || 'localhost',
        port => $opts{port} || 14000,
        username => $opts{username},
        cookie => undef,
        useragent => $opts{useragent} || 'Furl Net::Hadoop::Hoop (perl)',
        timeout => $opts{timeout} || 10,
    };
    $self->{furl} = Furl::HTTP->new(agent => $self->{useragent}, timeout => $self->{timeout});
    return bless $self, $this;
}

sub list {
    # opts: filter=<STRING>
    my ($self, $path, %opts) = @_;
# HTTP/1.1 200 OK
# Content-Type: application/json
# Transfer-Encoding: chunked
#
# [
#   {
#     "path" : "http:\/\/<HOOP_HOST>:14000\/<PATH>\/foo.txt"
#     "isDir" : false,
#     "len" : 966,
#     "owner" : "babu",
#     "group" : "supergroup",
#     "permission" : "-rw-r--r--",
#     "accessTime" : 1310671662423,
#     "modificationTime" : 1310671662423,
#     "blockSize" : 67108864,
#     "replication" : 3
#   }
# ]
    return $self->request('GET', $path, 'list');
}

sub status {
    my ($self, $path, %opts) = @_;
# HTTP/1.1 200 OK
# Content-Type: application/json
# Transfer-Encoding: chunked
#
# [
#   {
#     "path" : "http:\/\/<HOOP_HOST>:14000\/<PATH>\/foo.txt"
#     "isDir" : false,
#     "len" : 966,
#     "owner" : "babu",
#     "group" : "supergroup",
#     "permission" : "-rw-r--r--",
#     "accessTime" : 1310671662423,
#     "modificationTime" : 1310671662423,
#     "blockSize" : 67108864,
#     "replication" : 3
#   }
# ]
    return $self->request('GET', $path, 'status');
}

sub mkdir {
    my ($self, $path, %opts) = @_;
    return $self->request('POST', $path, 'mkdirs');
}

sub rename {
    my ($self, $path, $to) = @_;
    return $self->request('PUT', $path, 'rename', undef, to => $to);
}

sub read {
    # opts: offset=<NUM:-1(beggining of file)>, len=<NUM:-1(whole file)>
    my ($self, $path, %opts) = @_;
    return $self->request('GET', $path, undef, undef, %opts);
}

sub write {
    # opts: overwrite=<*true*/false>, replication=<NUM:-1>, blocksize=<NUM:-1>
    my ($self, $path, $data, %opts) = @_;
    # TODO filehandle ?
    return $self->request('POST', $path, 'create', $data, %opts);
}

sub append {
    # opts: NOT IMPLEMENTED NOW force=<true/*false*> (do write when path not exists or not)
    my ($self, $path, $data, %opts) = @_;
    # TODO filehandle ?
    # TODO force options
    return $self->request('PUT', $path, 'append', $data)
}

sub delete {
    # opts: recursive=<*true*/false>
    my ($self, $path, %opts) = @_;
    return $self->request('DELETE', $path, 'delete', undef, %opts);
}

#TODO
# sub homedir {}

#TODO
# sub setowner {}

#TODO
# sub setpermission {}

#TODO
# sub settimes {}

#TODO
# sub setreplication {}

sub build_path {
    my ($self, $path, $op, %params) = @_;
    $path = '/' . $path unless $path =~ m!^/!;
    my $genpath =$path . (defined($op) ? '?op=' . $op : '?');
    my @params = ();
    unless ($self->{cookie}) {
        push @params, 'user.name=' . URI::Escape::uri_escape($self->{username});
    }
    foreach my $key (keys %params) {
        push @params, URI::Escape::uri_escape($key) . '=' . URI::Escape::uri_escape($params{$key});
    }
    return $genpath unless @params;
    join('&', $genpath, @params);
}

sub request {
    my ($self, $method, $path, $op, $content, %params) = @_;
    my @request_params = (
        method => $method,
        host => $self->{server},
        port => $self->{port},
        path_query => $self->build_path($path, $op, %params),
    );
    my @headers = ();
    if ($self->{cookies}) {
        push @headers, (map {(Cookie => $_)} @{$self->{cookie}});
    }
    if ($content and ($method eq 'PUT' or $method eq 'POST')) {
        push @headers, ('content-type' => 'application/octet-stream');
        push @request_params, (content => $content);
    }
    if (scalar(@headers) > 0) {
        push @request_params, (headers => \@headers);
    }
    my ($ver, $code, $msg, $headers, $body) = $self->{furl}->request(@request_params);

    # cookie expired
    if ($self->{cookies} and $code == 401) {
        $self->{cookies} = undef;
        # retry with user.name
        return $self->request($method, $path, $op, $content, %params);
    }

    my @cookies = ();
    my $content_type = undef;
    for (my $i = 0; $i < scalar(@$headers); $i += 2) {
        if ($headers->[$i] m!\Aset-cookie\Z!i) {
            push @cookies, $headers->[$i+1];
        }
        elsif ($headers->[$i] =~ m!\Acontent-type\Z!i) {
            $content_type = $headers->[$i+1];
        }
    }
    if (scalar(@cookies) > 0 and $code != 401) {
        $self->{cookie} = \@cookies;
    } else {
        $self->{cookie} = undef;
    }

    if ($code == 200 || $code == 201) {
        if ($method eq 'GET' and defined $content_type) {
            if ($content_type eq 'application/octet-stream') {
                return $body;
            }
            elsif ($content_type eq 'application/json' and length($body) > 0) {
                return JSON::XS::decode_json($body);
            }
            return 1;
        }
        # successed GET(content-type unknown) or PUT/POST/DELETE
        return 1;
    }
    # error
    if ($content_type eq 'application/json') {
        my $error = JSON::XS::decode_json($body);
        # TODO fixme
        warn $error->{message};
    }
    return undef;
}

1;

__END__

=head1 NAME

Net::Hadoop::Hoop - Hoop(Hadoop httpfs) client library

=head1 SYNOPSIS

  use Net::Hadoop::Hoop;
  my $client = Net::Hadoop::Hoop->new(server => 'your.server', port => 14000, username => 'hoopuser');
  $client->read('/path/of/your/file');

=head1 AUTHOR

TAGOMORI Satoshi E<lt>tagomoris {at} gmail.comE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
