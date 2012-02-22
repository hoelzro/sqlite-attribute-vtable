use strict;
use warnings;
use lib 't/lib';

use Test::More tests => 3;
use SQLite::TestUtils;

check_deps;

my $ok;
my $dbh = create_dbh;

$dbh->do('PRAGMA foreign_keys = ON');

$ok = do {
    local $dbh->{'RaiseError'} = 0;

    $ok = $dbh->do(<<'END_SQL');
CREATE VIRTUAL TABLE attrs1 USING attributes;
END_SQL

    ok($ok, 'creating an attribute table with no additional columns should succeed')
        or diag($dbh->errstr);

    check_schema($dbh, attrs1 => {
        id         => 1,
        attributes => 1,
    });

    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
DROP TABLE attrs1
END_SQL
    };

    ok($ok, 'dropping an attribute table should succeed')
        or diag($dbh->errstr);

};

$dbh->disconnect;
