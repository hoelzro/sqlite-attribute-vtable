package SQLite::TestUtils;

use strict;
use warnings;
use charnames ':full';
use parent 'Exporter';

use Test::More;

my $RECORD_SEPARATOR = "\N{INFORMATION SEPARATOR ONE}";
my $SQLITE_TABLE     = qr/^"(\w+)"\."(\w+)"$/;

our @EXPORT = qw(check_deps check_schema create_dbh form_attr_string
                 dump_tables create_attribute_table insert_rows
                 check_sql dump_table);

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

    my $filename = $options{'filename'} || ':memory:';

    my $dbh = DBI->connect("dbi:SQLite:dbname=$filename", undef, undef, {
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

sub dump_table {
    my ( $dbh, $table_name ) = @_;

    my $sth = $dbh->prepare("SELECT * FROM $table_name");
    $sth->execute;

    my $rows = $sth->fetchall_arrayref;

    my @max_lengths = map { length() } @{ $sth->{'NAME'} };

    foreach my $row (@$rows) {
        @$row = map {
            defined() ? $_ : 'NULL'
        } @$row;

        foreach my $index (1 .. $#$row) {
            if(length($row->[$index]) > $max_lengths[$index]) {
                $max_lengths[$index] = length($row->[$index]);
            }
        }
    }

    my $format = join(' | ', map { '%' . $_ . 's' } @max_lengths) . "\n";

    diag(sprintf($format, @{ $sth->{'NAME'} }));
    foreach my $row (@$rows) {
        diag(sprintf($format, @$row));
    }
}

sub _is_virtual_table {
    my ( $dbh, $table_name ) = @_;

    my $database_name;

    ( $database_name, $table_name ) = $table_name =~ $SQLITE_TABLE;

    my $sth = $dbh->prepare(<<"END_SQL");
SELECT sql FROM $database_name.SQLITE_MASTER
WHERE type = 'table'
AND   name = ?
END_SQL

    $sth->execute($table_name);

    my ( $sql ) = $sth->fetchrow_array;

    return $sql =~ /create virtual table/i;
}

sub dump_tables {
    my ( $dbh ) = @_;

    my @tables = $dbh->tables(undef, undef, '%', 'TABLE');

    foreach my $table (@tables) {
        next if _is_virtual_table($dbh, $table);

        diag('');
        diag($table);
        diag('*' x length($table));
        diag('');

        dump_table $dbh, $table;
    }
}

sub create_attribute_table {
    my %options = @_;

    my $dbh  = $options{'dbh'};
    my $name = $options{'name'};

    $dbh->do(<<"END_SQL");
CREATE VIRTUAL TABLE $name USING attributes
END_SQL
}

sub stringify_rows {
    my ( $rows ) = @_;

    foreach my $row (@$rows) {
        foreach my $value (values %$row) {
            if(ref($value) eq 'HASH') {
                $value = form_attr_string(%$value);
            }
        }
    }
}

sub insert_rows {
    my ( $dbh, $table_name, @rows ) = @_;

    my $ok = 1;

    local $dbh->{'RaiseError'} = defined(wantarray) ? 0 : $dbh->{'RaiseError'};

    my @columns      = sort keys(%{ $rows[0] });
    my $columns      = join(', ', @columns);
    my $placeholders = join(', ', ('?') x @columns);

    my $sth = $dbh->prepare("INSERT INTO $table_name ($columns) VALUES ($placeholders)");

    return 0 unless $sth;

    stringify_rows(\@rows);
    foreach my $row (@rows) {
        $ok &&= $sth->execute(@{$row}{@columns});
    }

    return $ok;
}

sub make_row_sortable {
    my ( $row, @keys ) = @_;

    join($RECORD_SEPARATOR, map {
        join($RECORD_SEPARATOR, $_, $row->{$_})
    } @keys)
}

sub reorder_rows {
    my @rows = @_;

    my @keys = sort keys %{ $rows[0] };

    return sort {
        make_row_sortable($a, @keys) cmp make_row_sortable($b, @keys)
    } @rows;
}

sub check_sql {
    my %options = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my $dbh      = $options{'dbh'};
    my $sql      = $options{'sql'};
    my $ordered  = exists $options{'ordered'} ? $options{'ordered'} : 1;
    my $expected = $options{'rows'};

    local $dbh->{'RaiseError'} = 1;

    stringify_rows($expected);

    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @rows;

    while(my $row = $sth->fetchrow_hashref) {
        push @rows, $row;
    }

    unless($ordered) {
        @rows      = reorder_rows(@rows);
        @$expected = reorder_rows(@$expected);
    }

    is_deeply \@rows, $expected;
}

1;
