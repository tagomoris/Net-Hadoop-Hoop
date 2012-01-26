use strict;
use warnings;

use Test::More;

use Net::Hadoop::Hoop;

subtest 'genpath' => sub {
    my $client = Net::Hadoop::Hoop->new(username => 'whoami');
    is ($client->build_path('/path/to/somewhere', 'list'), '/path/to/somewhere?op=list&user.name=whoami');
    is ($client->build_path('path/to/somewhere', 'list'), '/path/to/somewhere?op=list&user.name=whoami');
    is ($client->build_path('/path/to/somewhere', 'get'), '/path/to/somewhere?op=get&user.name=whoami');

    $client->{cookie} = ['foobar'];
    is ($client->build_path('/path/to/somewhere', 'get'), '/path/to/somewhere?op=get');
};

done_testing;
