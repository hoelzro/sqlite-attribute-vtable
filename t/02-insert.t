use strict;
use warnings;
use lib 't/lib';

use Test::More tests => 1;
use SQLite::TestUtils;

check_deps;

my $ok;
my $dbh = create_dbh;

$dbh->do(<<'END_SQL');
CREATE VIRTUAL TABLE attrs1 USING attributes;
END_SQL

INSERT_DATA: {
    $ok = do {
        local $dbh->{'RaiseError'} = 0;

        my $attributes = form_attr_string(
            foo => 17,
            bar => 18,
        );

        $dbh->do('INSERT INTO attrs1 (attributes) VALUES (?)', undef, $attributes);
    };

    ok($ok, 'inserting data into an attribute table should succeed')
        or diag($dbh->errstr);
}
