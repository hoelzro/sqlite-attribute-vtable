use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

plan tests => 1;

require DBI;

my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', undef, undef, {
    PrintError => 0,
});

$dbh->sqlite_enable_load_extension(1);

my $ok = $dbh->do(q{SELECT load_extension('./attributes.so', 'sql_attr_init')});

ok($ok, 'attribute extension should successfully load') or diag($dbh->errstr);

$dbh->disconnect;
