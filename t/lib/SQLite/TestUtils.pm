package SQLite::TestUtils;

use strict;
use warnings;
use charnames ':full';
use parent 'Exporter';

use Test::More;

my $RECORD_SEPARATOR = "\N{INFORMATION SEPARATOR ONE}";

our @EXPORT = qw(check_deps check_schema create_dbh form_attr_string);

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

sub check_schema {
    my ( $dbh, $table_name, $schema ) = @_;

    $schema = { %$schema }; # shallow copy

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my $sth = $dbh->prepare(sprintf('PRAGMA table_info(%s)',
        $dbh->quote($table_name)));

    $sth->execute;

    while(my $row = $sth->fetchrow_hashref) {
        my $name = $row->{'name'};

        unless(delete $schema->{$name}) {
            fail;
            diag "Unknown column '$name' found in database schema";
            return;
        }
    }

    if(%$schema) {
        my ( $name ) = sort keys %$schema; # sort to make sure the first key
                                           # is consistent between runs

        fail;
        diag "Column '$name' missing from schema";
        return;
    }
    pass;
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

sub form_attr_string {
    my @attr_pairs = @_;

    my @pieces;

    for(my $i = 0; $i < @attr_pairs; $i += 2) {
        my ( $key, $value ) = @attr_pairs[$i, $i + 1];

        push @pieces, $key, $value; # record separator for k-v pairs
                                    # is the same
    }

    return join($RECORD_SEPARATOR, @pieces);
}

1;
