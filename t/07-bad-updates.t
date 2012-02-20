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

insert_rows $dbh, 'attributes', ({
    attributes => {
        foo => 17,
    },
});

$dbh->{'RaiseError'} = 0;

NULL_ATTRIBUTES: {
    $ok = $dbh->do('UPDATE attributes SET attributes = NULL');
    
    ok !$ok, q{update a row's attributes to NULL should fail};
}

INTEGER_ATTRIBUTES: {
    $ok = $dbh->do('UPDATE attributes SET attributes = 5');

    ok !$ok, q{updating a row's attributes to a number should fail};
}

NON_ATTRIBUTES_ATTRIBUTES: {
    $ok = $dbh->do(q{UPDATE attributes SET attributes = 'forty-one'});
    ok !$ok, q{updating row's attributes to a non-attribute string should fail};
}

TEXT_ROWID: {
    $dbh->do(q{UPDATE attributes SET ROWID = 'foo'});

    ok !$ok, q{updating a row's ROWID to a string should fail};
}

TEXT_ID: {
    $dbh->do(q{UPDATE attributes SET id = 'foo'});

    ok !$ok, q{updating a row's id to a string should fail};
}

done_testing;
