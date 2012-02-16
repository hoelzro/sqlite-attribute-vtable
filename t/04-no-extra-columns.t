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

check_schema $dbh, 'attributes', {
    id         => 1,
    attributes => 1,
};

insert_rows $dbh, 'attributes', ({
    attributes => {
        foo => 17,
        bar => 18,
    },
}, {
    attributes => {
        bar => 19,
        baz => 20,
    },
}, {
    attributes => {
        bar => 18,
    },
}, {
    attributes => {
        foo => 16,
    },
});

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes',
    ordered => 0,
    rows    => [{
        id         => 1,
        attributes => {
            foo => 17,
            bar => 18,
        },
    }, {
        id         => 2,
        attributes => {
            bar => 19,
            baz => 20,
        },
    }, {
        id         => 3,
        attributes => {
            bar => 18,
        },
    }, {
        id         => 4,
        attributes => {
            foo => 16,
        },
    }],
);

check_sql(
    dbh     => $dbh,
    sql     => q{SELECT * FROM attributes WHERE attributes MATCH 'foo'},
    ordered => 0,
    rows    => [{
        id         => 1,
        attributes => {
            foo => 17,
            bar => 18,
        },
    }, {
        id         => 4,
        attributes => {
            foo => 16,
        },
    }],
);

check_sql(
    dbh     => $dbh,
    sql     => q{SELECT * FROM attributes WHERE attributes MATCH 'foo' ORDER BY get_attr(attributes, 'foo')},
    ordered => 1,
    rows    => [{
        id         => 4,
        attributes => {
            foo => 16,
        },
    }, {
        id         => 1,
        attributes => {
            foo => 17,
            bar => 18,
        },
    }],
);

done_testing;