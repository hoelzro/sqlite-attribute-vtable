use strict;
use warnings;
use lib 't/lib';

use Test::More tests => 10;
use SQLite::TestUtils;

check_deps;

my $RS = get_record_separator();

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

check_sql(
    dbh  => $dbh,
    sql  => q{SELECT attributes MATCH 'foo' FROM attributes},
    rows => [
        [ 1 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => q{SELECT COUNT(1) FROM attributes WHERE attributes MATCH 'foo'},
    rows => [
        [ 1 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => q{SELECT attributes MATCH 'bar' FROM attributes},
    rows => [
        [ 0 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => q{SELECT COUNT(1) FROM attributes WHERE attributes MATCH 'bar'},
    rows => [
        [ 0 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT attributes MATCH 'foo${RS}17' FROM attributes},
    rows => [
        [ 1 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT COUNT(1) FROM attributes WHERE attributes MATCH 'foo${RS}17'},
    rows => [
        [ 1 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT attributes MATCH 'foo${RS}18' FROM attributes},
    rows => [
        [ 0 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT COUNT(1) FROM attributes WHERE attributes MATCH 'foo${RS}18'},
    rows => [
        [ 0 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT attributes MATCH 'bar${RS}17' FROM attributes},
    rows => [
        [ 0 ],
    ],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT COUNT(1) FROM attributes WHERE attributes MATCH 'bar${RS}18'},
    rows => [
        [ 0 ],
    ],
);
