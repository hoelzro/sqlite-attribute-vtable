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

insert_rows $dbh, 'attributes', ({
    id         => 10,
    attributes => {
        foo => 17,
    },
});

is $dbh->last_insert_id(undef, undef, undef, undef), 10;

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes WHERE id = 10',
    ordered => 0,
    rows    => [{
        id         => 10,
        attributes => {
            foo => 17,
        },
    }],
);

insert_rows $dbh, 'attributes', ({
    ROWID => 15,
    attributes => {
        bar => 18,
    },
});

is $dbh->last_insert_id(undef, undef, undef, undef), 15;

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes WHERE id = 15',
    ordered => 0,
    rows    => [{
        id         => 15,
        attributes => {
            bar => 18,
        },
    }],
);

my $ok;

$ok = insert_rows $dbh, 'attributes', ({
    id         => 15,
    attributes => {
        a => 1,
    },
});

ok !$ok, 'inserting a row with a duplicate primary key should fail';
like $dbh->errstr, qr/PRIMARY KEY must be unique/;

$ok = insert_rows $dbh, 'attributes', ({
    ROWID      => 10,
    attributes => {
        b => 2,
    },
});

ok !$ok, 'inserting a row with a duplicate primary key should fail';
like $dbh->errstr, qr/PRIMARY KEY must be unique/;

$ok = do {
    local $dbh->{'RaiseError'} = 0;

    $dbh->do('DELETE FROM attributes WHERE id = 15');
};

ok $ok, 'deleting rows should work' or diag($dbh->errstr);

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes WHERE id = 15',
    ordered => 0,
    rows    => [],
);

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes ORDER BY id',
    ordered => 1,
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
    }, {
        id         => 10,
        attributes => {
            foo => 17,
        },
    }],
);

$ok = do {
    local $dbh->{'RaiseError'} = 0;

    my $rs = get_record_separator();

    $dbh->do("DELETE FROM attributes WHERE attributes MATCH 'bar${rs}18'");
};

ok $ok, 'deleting rows matching attributes should work' or diag($dbh->errstr);

check_sql(
    dbh     => $dbh,
    sql     => 'SELECT * FROM attributes ORDER BY id',
    ordered => 1,
    rows    => [{
        id         => 2,
        attributes => {
            bar => 19,
            baz => 20,
        },
    }, {
        id         => 4,
        attributes => {
            foo => 16,
        },
    }, {
        id         => 10,
        attributes => {
            foo => 17,
        },
    }],
);

done_testing;
