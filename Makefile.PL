use inc::Module::Install;
name 'Net-Hadoop-Hoop';
all_from 'lib/Net/Hadoop/Hoop.pm';

requires 'Try::Tiny';
requires 'JSON';
requires 'JSON::XS';

requires 'URI::Escape';
requires 'Furl';

requires 'Test::More';
requires 'Test::Deep';
requires 'Test::MockTime';

tests 't/*.t';
test_requires 'Test::More';

auto_set_repository;

WriteAll;
