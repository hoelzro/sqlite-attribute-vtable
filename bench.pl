#!/usr/bin/env perl

use strict;
use warnings;
use lib 't/lib';
use charnames ':full';
use feature 'say';

use Benchmark ':hireswallclock';
use DBI;
use SQLite::TestUtils;

my @attrs = qw{
    foo
    bar
    baz
    quux
    qrat
    rob
    dad
    mom
    billy
    sue
    spot
    sqlite
    perl
};

my $RS = "\N{INFORMATION SEPARATOR ONE}";

sub generate_rand_attrs {
    return map {
        my $attr  = $attrs[ int(rand(scalar(@attrs))) ];
        my $value = int(rand(100));

        form_attr_string(
            $attr => $value
        );
    } ( 1 .. 100_000 );
}

unlink 'test.db';
my $dbh = create_dbh(filename => 'test.db');

$dbh->do('CREATE VIRTUAL TABLE attrs USING attributes');

my @rand_attrs = generate_rand_attrs();

$dbh->begin_work;
my $sth = $dbh->prepare('INSERT INTO attrs (attributes) VALUES (?)');
timethis(scalar(@rand_attrs), sub {
    $sth->execute(shift @rand_attrs);
}, 'Attribute Insertion');

$dbh->commit;

$sth = $dbh->prepare(q{SELECT id FROM attrs WHERE attributes MATCH 'foo'});
timethis(1_000, sub {
    $sth->execute;
    1 while $sth->fetch;
}, 'Attribute Lookup');

my @sths = map {
    $dbh->prepare(qq{SELECT id FROM attrs WHERE attributes MATCH 'foo${RS}$_'});
} ( 0 .. 99 );

my $i = 0;

timethis(1_000, sub {
    my $sth = $sths[ $i++ ];
    $i %= @sths;

    $sth->execute;
    1 while $sth->fetch;
}, 'Key + Value Lookup');

$sth = $dbh->prepare(q{SELECT COUNT(1) FROM attrs WHERE attributes MATCH 'foo'});
timethis(1_000, sub {
    $sth->execute;
    1 while $sth->fetch;
}, 'Attribute Count');

@sths = map {
    $dbh->prepare(qq{SELECT COUNT(1) FROM attrs WHERE attributes MATCH 'foo${RS}$_'});
} ( 0 .. 99 );

$i = 0;

timethis(1_000, sub {
    my $sth = $sths[ $i++ ];
    $i %= @sths;

    $sth->execute;
    1 while $sth->fetch;
}, 'Key + Value Count');

# XXX hmmm....multiple rows per attribute?
$sth = $dbh->prepare(q{SELECT s.seq_id FROM attrs_Sequence AS s INNER JOIN attrs_Attributes AS a ON a.seq_id = s.seq_id WHERE a.attr_name = 'foo'});
timethis(1_000, sub {
    $sth->execute;
    1 while $sth->fetch;
}, 'Raw attribute table query');

$sth = $dbh->prepare(q{SELECT seq_id FROM attrs_Attributes WHERE attr_name = 'foo'});

timethis(1_000, sub {
    $sth->execute;
    1 while $sth->fetch;
}, 'Raw attribute table query (no join)');

$dbh->disconnect;
