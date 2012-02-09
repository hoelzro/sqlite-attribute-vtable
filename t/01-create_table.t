use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

plan tests => 9;

my $ok;
my $dbh = create_dbh;

NO_EXTRA_COLS: {
    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
CREATE VIRTUAL TABLE attrs1 USING attributes;
END_SQL
    };

    ok($ok, 'creating an attribute table with no additional columns should succeed')
        or diag($dbh->errstr);

    check_schema($dbh, attrs1 => {
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
}

ONE_EXTRA_COL: {
    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
CREATE VIRTUAL TABLE attrs2 USING attributes(field1 TEXT);
END_SQL
    };

    ok($ok, 'creating an attribute table with an extra column should succeed')
        or diag($dbh->errstr);

    check_schema($dbh, attrs2 => {
        field1     => 1,
        attributes => 1,
    });

    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
DROP TABLE attrs2
END_SQL
    };

    ok($ok, 'dropping an attribute table should succeed')
        or diag($dbh->errstr);
}

RENAMED_ATTR_COL: {
    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
CREATE VIRTUAL TABLE attrs3 USING attributes(a ATTRIBUTES);
END_SQL
    };

    ok($ok, 'creating an attribute table with a renamed attributes column should succeed')
        or diag($dbh->errstr);

    check_schema($dbh, attrs3 => {
        a => 1,
    });

    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        $dbh->do(<<'END_SQL');
DROP TABLE attrs3
END_SQL
    };

    ok($ok, 'dropping an attribute table should succeed')
        or diag($dbh->errstr);
}

$dbh->disconnect;
