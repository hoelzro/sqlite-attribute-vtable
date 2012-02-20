use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

my $ok;
my $dbh = create_dbh;

create_attribute_table(
    dbh  => $dbh,
    name => 'attributes',
);

NULL_ATTRIBUTES: {
    $ok = insert_rows $dbh, 'attributes', ({
        attributes => undef,
    });

    ok !$ok, q{inserting NULL as a row's attributes should fail};
}

INTEGER_ATTRIBUTES: {
    $ok = insert_rows $dbh, 'attributes', ({
        attributes => 5,
    });

    ok !$ok, q{inserting a number as a row's attributes should fail};
}

NON_ATTRIBUTES_ATTRIBUTES: {
    $ok = insert_rows $dbh, 'attributes', ({
        attributes => 'forty-one',
    });

    ok !$ok, q{inserting a string without a record separator as a row's attributes should fail};
}

TEXT_ROWID: {
    $ok = insert_rows $dbh, 'attributes', ({
        ROWID      => 'foo',
        attributes => {
            foo => 'bar',
        },
    });

    ok !$ok, q{inserting a TEXT ROWID should fail};
}

TEXT_ID: {
    $ok = insert_rows $dbh, 'attributes', ({
        id         => 'foo',
        attributes => {
            foo => 'bar',
        },
    });

    ok !$ok, q{inserting a TEXT id should fail};
}

done_testing;
