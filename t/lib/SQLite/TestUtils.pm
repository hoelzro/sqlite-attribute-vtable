package SQLite::TestUtils;

use strict;
use warnings;
use parent 'Exporter';

use Test::More;

our @EXPORT = qw(check_deps);

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

1;
