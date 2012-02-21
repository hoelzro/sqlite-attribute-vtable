use strict;
use warnings;
use lib 't/lib';

use Test::More tests => 7;
use SQLite::TestUtils;

check_deps;

my $dbh = create_dbh;

create_attribute_table(
    dbh  => $dbh,
    name => 'attributes',
);

insert_rows $dbh, 'attributes', ({
    attributes => {
        foo => 17,
    },
}, {
    attributes => {
        bar => 18,
    },
}, {
    attributes => {
        foo => 18,
    },
}, {
    attributes => {
        foo => 17,
        bar => 18,
    }
}, {
    attributes => {
        '5' => 6,
    },
});

CHECK_MATCHING_STRING: {
    check_sql(
        dbh     => $dbh,
        sql     => q{SELECT * FROM attributes WHERE get_attr(attributes, 'bar') = '18'},
        ordered => 0,
        rows    => [{
            id => 2,
            attributes => {
                bar => 18,
            },
        }, {
            id => 4,
            attributes => {
                foo => 17,
                bar => 18,
            }
        }],
    );
}

# take note of the CAST
CHECK_MATCHING_INTEGER: {
    check_sql(
        dbh     => $dbh,
        sql     => q{SELECT * FROM attributes WHERE CAST(get_attr(attributes, 'bar') AS INTEGER) = 18},
        ordered => 0,
        rows    => [{
            id => 2,
            attributes => {
                bar => 18,
            },
        }, {
            id => 4,
            attributes => {
                foo => 17,
                bar => 18,
            }
        }],
    );
}

CHECK_MATCHING_NULL: {
    check_sql(
        dbh     => $dbh,
        sql     => q{SELECT * FROM attributes WHERE get_attr(attributes, 'bar') IS NULL},
        ordered => 0,
        rows    => [{
            id => 1,
            attributes => {
                foo => 17,
            },
        }, {
            id => 3,
            attributes => {
                foo => 18,
            }
        }, {
            id => 5,
            attributes => {
                '5' => 6,
            },
        }],
    );
}

CHECK_BAD_ATTRS: {
    check_sql(
        dbh   => $dbh,
        sql   => q{SELECT * FROM attributes WHERE get_attr(5, 'bar') IS NULL},
        error => qr/attribute operand must be a string/,
    );

    check_sql(
        dbh   => $dbh,
        sql   => q{SELECT * FROM attributes WHERE get_attr(NULL, 'bar') IS NULL},
        error => qr/attribute operand must be a string/,
    );
}

CHECK_INTEGER_QUERY: {
    check_sql(
        dbh   => $dbh,
        sql   => q{SELECT * FROM attributes WHERE get_attr(attributes, 5) = '6'},
        rows  => [{
            id => 5,
            attributes => {
                '5' => 6,
            },
        }],
    );
}

CHECK_NULL_QUERY: {
    check_sql(
        dbh   => $dbh,
        sql   => q{SELECT * FROM attributes WHERE get_attr(attributes, NULL) IS NULL},
        error => qr/query operand must not be NULL/,
    );
}
