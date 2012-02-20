use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

my $dbh = create_dbh;

create_attribute_table(
    dbh  => $dbh,
    name => 'attributes',
);

my $ok = insert_rows $dbh, 'attributes', ({
    attributes => [
        foo => 17,
        bar => 18,
        foo => 19,
    ],
});

ok !$ok, 'inserting duplicate attributes should fail';
like $dbh->errstr, qr/duplicate attributes are forbidden/;

done_testing;
