use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

my $RS = get_record_separator();

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

ok $ok, 'inserting duplicate attributes should be successful';

check_sql(
    dbh  => $dbh,
    sql  => 'SELECT * FROM attributes',
    rows => [{
        attributes => form_attr_string(
            bar => 18,
            foo => 19,
        ),
    }],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT * FROM attributes WHERE attributes MATCH 'foo${RS}19'},
    rows => [{
        attributes => form_attr_string(
            bar => 18,
            foo => 19,
        ),
    }],
);

check_sql(
    dbh  => $dbh,
    sql  => qq{SELECT * FROM attributes WHERE attributes MATCH 'foo${RS}17'},
    rows => [],
);

done_testing;
