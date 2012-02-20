use strict;
use warnings;
use lib 't/lib';

use Test::More;
use SQLite::TestUtils;

check_deps;

sub check_tables {
    my ( $dbh ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my @tables = $dbh->tables(undef, undef, '%', 'TABLE');

    is_deeply \@tables, [];
}

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
        id         => 1,
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

    check_tables $dbh;
}

$dbh->disconnect;

done_testing;
