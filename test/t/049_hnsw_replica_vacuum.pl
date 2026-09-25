use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IPC::Run;

# Scans on a hot standby while the primary repeatedly deletes, vacuums and
# inserts. Vacuum's wait for in-flight scans (HNSW_SCAN_LOCK) is not
# WAL-logged, so standby scans can load elements that replay has just marked
# deleted.

my $dim = 100;
my $nrows = 1500;
my $batch = 300;
my $array_sql = join(",", ('random()') x $dim);

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', "autovacuum = off");
$node_primary->start;
$node_primary->backup('my_backup');

# Create streaming replica
my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->init_from_backup($node_primary, 'my_backup', has_streaming => 1);
# Delay replay instead of canceling conflicting standby queries
$node_replica->append_conf('postgresql.conf', "max_standby_streaming_delay = -1");
$node_replica->append_conf('postgresql.conf', "max_connections = 40");
$node_replica->start;

# Create table and index
$node_primary->safe_psql("postgres", "CREATE EXTENSION vector;");
$node_primary->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node_primary->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, $nrows) i;");
$node_primary->safe_psql("postgres",
	"CREATE INDEX ON tst USING hnsw (v vector_l2_ops) WITH (m = 4, ef_construction = 8);");
$node_primary->wait_for_catchup($node_replica, 'replay');

# Run scans on the standby in the background
my $script = $node_replica->basedir . '/select.sql';
PostgreSQL::Test::Utils::append_to_file($script,
	"SET hnsw.ef_search = 1000;\n"
	  . "SELECT i FROM tst ORDER BY v <-> (SELECT ARRAY[$array_sql]::vector) LIMIT 10;\n");
my ($stdout, $stderr) = ('', '');
my $h = IPC::Run::start(
	[
		'pgbench', '--no-vacuum', '--client=16', '--time=10',
		'--file=' . $script, '--host=' . $node_replica->host,
		'--port=' . $node_replica->port, 'postgres'
	],
	'<', \undef, '>', \$stdout, '2>', \$stderr);

# Delete, vacuum and reinsert a batch of rows at a time on the primary
my $cycles = 0;
while ($h->pumpable)
{
	my $start = ($cycles * $batch) % $nrows;
	my $end = $start + $batch;
	$node_primary->safe_psql("postgres",
		"DELETE FROM tst WHERE i >= $start AND i < $end;\n"
		  . "VACUUM tst;\n"
		  . "INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series($start, $end - 1) i;\n");
	$cycles++;
	$h->pump_nb;
}
$h->finish;

unlike($stderr, qr/cannot load deleted element/, "standby scans do not load deleted elements");
is($h->result(0), 0, "pgbench on standby succeeds");

done_testing();
