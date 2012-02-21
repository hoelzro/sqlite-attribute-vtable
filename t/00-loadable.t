use strict;
use warnings;
use lib 't/lib';

use Test::More tests => 1;
use SQLite::TestUtils;

check_deps;

my $dbh = create_dbh(no_load => 1);

my $ok = do {
    local $dbh->{'RaiseError'} = 0;

    $dbh->do(q{SELECT load_extension('./attributes.so', 'sql_attr_init')});
};

ok($ok, 'attribute extension should successfully load') or diag($dbh->errstr);

$dbh->disconnect;
