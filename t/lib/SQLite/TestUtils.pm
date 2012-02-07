package SQLite::TestUtils;

use strict;
use warnings;
use parent 'Exporter';

use Test::More;

our @EXPORT = qw(check_deps create_dbh);

sub check_deps {
    my $ok = eval {
        require DBD::SQLite;
        1;
    };

    unless($ok) {
        plan skip_all => q{The DBD::SQLite module is required to run this test};
        exit 0;
    }
}

sub create_dbh {
    my ( %options ) = @_;

    require DBI;

    my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', undef, undef, {
        RaiseError => 1,
        PrintError => 0,
    });

    $dbh->sqlite_enable_load_extension(1);

    unless($options{'no_load'}) {
        $dbh->do(q{SELECT load_extension('./attributes.so', 'sql_attr_init')});
    }

    return $dbh;
}

1;
