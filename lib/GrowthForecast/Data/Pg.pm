package GrowthForecast::Data::Pg;

use strict;
use warnings;
use base qw/GrowthForecast::Data/;
use Scope::Container::DBI;
use Log::Minimal;

sub new {
    my $class = shift;
    my $pgsql = shift;
    my $float_number = shift;
    my $disable_subtract = shift;

    return bless +{
        pgsql            => $pgsql,
        float_number     => $float_number,
        disable_subtract => $disable_subtract,
        for_update       => 1,
    }, $class;
}

sub number_type {
    my $self = shift;
    return $self->{'float_number'} ? 'DOUBLE PRECISION' : 'BIGINT';
}

sub complex_number_type {
    my $self = shift;
    return $self->{'float_number'} ? 'REAL' : 'INT';
}

sub on_connect {
    my $self = shift;
    return sub {
        my $dbh = shift;
        my $number_type = $self->number_type;
        my $complex_number_type = $self->complex_number_type;

        $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_tables
    WHERE  relname = 'graphs'
) THEN

    CREATE TABLE graphs (
        id           BIGSERIAL    PRIMARY KEY,
        service_name VARCHAR(255) NOT NULL,
        section_name VARCHAR(255) NOT NULL,
        graph_name   VARCHAR(255) NOT NULL,
        number       $number_type NOT NULL DEFAULT 0,
        mode         VARCHAR(255) NOT NULL DEFAULT 'gauge',
        description  VARCHAR(255) NOT NULL DEFAULT '',
        sort         INT          NOT NULL DEFAULT 0,
        gmode        VARCHAR(255) NOT NULL DEFAULT 'gauge',
        color        VARCHAR(255) NOT NULL DEFAULT '#00CC00',
        ulimit       $number_type NOT NULL DEFAULT 1000000000000000,
        llimit       $number_type NOT NULL DEFAULT 0,
        sulimit      $number_type NOT NULL DEFAULT 100000,
        sllimit      $number_type NOT NULL DEFAULT 0,
        type         VARCHAR(255) NOT NULL DEFAULT 'AREA',
        stype        VARCHAR(255) NOT NULL DEFAULT 'AREA',
        meta         TEXT,
        created_at   BIGINT       NOT NULL,
        updated_at   BIGINT       NOT NULL,
        timestamp    BIGINT       DEFAULT NULL,
        UNIQUE  (service_name, section_name, graph_name)
    );
END IF;

END\$\$;
EOF

        unless ( $self->{disable_subtract} ) {
            $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_tables
    WHERE  relname = 'prev_graphs'
) THEN

    CREATE TABLE prev_graphs (
        graph_id     BIGINT       NOT NULL,
        number       $number_type NOT NULL DEFAULT 0,
        subtract     $number_type,
        updated_at   BIGINT       NOT NULL,
        PRIMARY KEY  (graph_id)
    );
END IF;

END\$\$
EOF

            $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_tables
    WHERE  relname = 'prev_short_graphs'
) THEN

    CREATE TABLE prev_short_graphs (
        graph_id     BIGINT       NOT NULL,
        number       $number_type NOT NULL DEFAULT 0,
        subtract     $number_type,
        updated_at   BIGINT       NOT NULL,
        PRIMARY KEY  (graph_id)
    );
END IF;

END\$\$
EOF
        }

        $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_tables
    WHERE  relname = 'complex_graphs'
) THEN

    CREATE TABLE complex_graphs (
        id           BIGSERIAL    PRIMARY KEY,
        service_name VARCHAR(255) NOT NULL,
        section_name VARCHAR(255) NOT NULL,
        graph_name   VARCHAR(255) NOT NULL,
        number       $complex_number_type NOT NULL DEFAULT 0,
        description  VARCHAR(255) NOT NULL DEFAULT '',
        sort         INT          NOT NULL DEFAULT 0,
        meta         TEXT,
        created_at   BIGINT       NOT NULL,
        updated_at   BIGINT       NOT NULL,
        UNIQUE  (service_name, section_name, graph_name)
    );
END IF;

END\$\$
EOF

        $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_tables
    WHERE  relname = 'vrules'
) THEN

    CREATE TABLE vrules (
        id           BIGSERIAL    PRIMARY KEY,
        graph_path   VARCHAR(255) NOT NULL,
        time         BIGINT       NOT NULL,
        color        VARCHAR(255) NOT NULL DEFAULT '#FF0000',
        description  TEXT,
        dashes       VARCHAR(255) NOT NULL DEFAULT ''
    );
END IF;

END\$\$
EOF

        $dbh->do(<<EOF);
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_stat_user_indexes
    WHERE  indexrelname = 'time_graph_path'
) THEN

    CREATE INDEX time_graph_path ON vrules(time, graph_path);
END IF;

END\$\$
EOF

        {
            my $sth = $dbh->column_info(undef,undef,"vrules",undef);
            my $columns = $sth->fetchall_arrayref(+{ COLUMN_NAME => 1 });
            my %graphs_columns;
            $graphs_columns{$_->{COLUMN_NAME}} = 1 for @$columns;
            if ( ! exists $graphs_columns{dashes} ) {
                infof("add new column 'dashes'");
                $dbh->do(q{ALTER TABLE vrules ADD dashes VARCHAR(255) NOT NULL DEFAULT ''});
            }
        }

        # timestamp
        {
            my $sth = $dbh->column_info(undef,undef,"graphs",undef);
            my $columns = $sth->fetchall_arrayref(+{ COLUMN_NAME => 1 });
            my %graphs_columns;
            $graphs_columns{$_->{COLUMN_NAME}} = 1 for @$columns;
            if ( ! exists $graphs_columns{'"timestamp"'} ) {
                infof("add new column 'timestamp'");
                $dbh->do(q{ALTER TABLE graphs ADD timestamp BIGINT DEFAULT NULL});
            }
        }

        return;
    };
}

sub dbh {
    my $self = shift;
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    Scope::Container::DBI->connect(
        $self->{pgsql},
        $ENV{PGSQL_USER},
        $ENV{PGSQL_PASSWORD},
        {
            Callbacks => {
                connected => $self->on_connect,
            },
        }
    );
}

1;
