# RonDB Tools

This tool lets you deploy a RonDB cluster on AWS, monitor the cluster and run benchmarks.

## Simple usage

A minimal guide to get started.

1. The easiest way to get all necessary dependencies is to install `nix` and run `nix-shell`.
  You'll get a bash session with access to the dependencies, without modifying your system directories.
  Otherwise:
    * [Install terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
    * Install AWS CLI, python3, rsync and tmux.
      * On Debian/Ubuntu: `apt-get update && apt-get install -y awscli python3 rsync tmux`
      * On macOS: `brew install awscli python3 rsync tmux`
2. You need an AWS API key to create cloud resources.
  Run `aws configure` and enter it.
3. Run `./cluster_ctl deploy` to initialize and deploy the cluster.
   Go to the printed web address to monitor the cluster using grafana dashboards.
4. Run `./cluster_ctl bench_locust` to create test data and start locust.
  Go to the printed web address to access locust.
5. In the locust GUI, set the number of users to the same as the number of workers and press `START`.
  During the benchmark run, you can use both locust and grafana to gather statistics, from the client's and server's point of view, respectively.
6. Run `./cluster_ctl cleanup` to destroy the AWS resources and local files created.

## Manual

### Cluster configuration

A cluster can be (re)configured by changing the values in `config.py`.

Before running `./cluster_ctl deploy` (or `./cluster_ctl terraform`) and after running `./cluster_ctl cleanup`, terraform is not initialized and you may edit `config.py` freely.
Otherwise, follow these steps:
* If you intend to change region, run `./cluster_ctl cleanup`.
  This is because `region` cannot be updated for an existing cluster.
* Edit `config.py`.
* Apply the changes. The easiest way to do this is to run `./cluster_ctl deploy`.

  If the cluster already existed, it might be possible to apply the changes faster, but it's more complex:
  * If you have changed variables that affect AWS resources, you must run `./cluster_ctl terraform`.
    These variables include `num_azs`, `cpu_platform`, `*_instance_type`, `*_disk_size` and `*_count`.
  * Run `./cluster_ctl install` at least for affected nodes.
    This may include more nodes than you suppose.
    For example, changing `ndbmtd_count` will affect the sequence of node IDs assigned to `mysqld`, `rdrs` and `bench` nodes, and will also affect the `ndb_mgmd` config.
  * Run `./cluster_ctl start` at least for affected nodes.
* If all data nodes have been reinstalled or restarted, the test data table is lost.
  You'll have to repopulate using `./cluster_ctl bench_locust` or `./cluster_ctl populate`.
* If you have run `./cluster_ctl deploy` or `./cluster_ctl start` on `bench` nodes, locust will be stopped but not started.
  You'll have to use `./cluster_ctl bench_locust` or `./cluster_ctl start_locust` for that.

### `cluster_ctl` reference

Some sub-commands take a `NODES` argument.
It is a comma-separated list of node names and node types.
If given, the command will operate only on matching nodes, otherwise on all nodes.
Available node types are: `ndb_mgmd`, `ndbmtd`, `mysqld`, `rdrs`, `prometheus`, `grafana` and `bench`.

* `./cluster_ctl deploy [NODES]`
    A convenience command equivalent to `terraform`, `install` and `start`.

* `./cluster_ctl terraform`
    Configure, initialize and apply terraform as needed.
    In detail,
    * create or update `terraform.tfvars`.
    * call `terraform init` unless it is already initialized.
    * call `terraform apply`.

* `./cluster_ctl install [NODES]`
    Install necessary software and configuration files.
    Depending on the node type this may include RonDB, prometheus, grafana and locust.

* `./cluster_ctl start [NODES]`
    Start or restart services.
    (This will not start locust or the demo web UI, see `bench_locust` and `start_demo` respectively.)

* `./cluster_ctl stop [NODES]`
    Stop services.

* `./cluster_ctl open_tmux [NODES]`
    Create a tmux session with ssh connections opened to all applicable nodes.

    When attached to the tmux session, you can press `C-b n` to go to the next window, and `C-b d` to detach.

    If a session already exists, ignore the `NODES` argument and reattach to it.
    To connect to a different set of nodes, use `kill_tmux` first.

    See also the `./cluster_ctl ssh` command.

* `./cluster_ctl kill_tmux`
    Kill the tmux session created by open_tmux.

* `./cluster_ctl ssh NODE_NAME [COMMAND ...]`
    Connect to a node using ssh.
    Run `./cluster_ctl list` to see the options for `NODE_NAME`.

    See also the `./cluster_ctl open_tmux` command.

* `./cluster_ctl bench_locust [--cols COLUMNS] [--rows ROWS] [--types COLUMN_TYPES] [--workers WORKERS]`
    A convenience command equivalent to `populate` followed by `start_locust`.

* `./cluster_ctl populate [--cols COLUMNS] [--rows ROWS] [--types COLUMN_TYPES]`
    Create and populate a table with test data.
    If a table already exists, recreate it if any parameter has changed, otherwise do nothing.
    Parameters:
    * `COLUMNS`: Number of columns in table. Default 10.
    * `ROWS`: Number of rows in table. Default 100000.
    * `COLUMN_TYPES`: The columns types in table; one of `INT`, `VARCHAR`, `INT_AND_VARCHAR`. Default `INT`.

* `./cluster_ctl start_locust [--rows ROWS] [--workers WORKERS]`
    Start or restart locust services.
    You need to run `populate` first.
    Parameters:
    * `ROWS`: Number of rows in table. Default 100000.
            **Warning**: This must be set to the same value as the last call to `populate`.
            Prefer using `bench_locust`, which will do the right thing automatically.
    * `WORKERS`: Number of locust workers. Defaults to the maximum possible (one per available CPU).

* `./cluster_ctl stop_locust`
    Stop locust.

* `./cluster_ctl start_demo`
    Start the demo web UI that allows anonymous users to create databases, run benchmarks and monitor.

* `./cluster_ctl stop_demo`
    Stop the demo web UI.

* `./cluster_ctl list`
    Print node information, including node name, node type, public and private IP, NDB Node IDs, and running processes.

* `./cluster_ctl cleanup`
    This will
    * destroy the terraform cluster created by `./cluster_ctl terraform`
    * destroy the AWS SSH key created by `./cluster_ctl terraform`
    * remove the tmux session created by `./cluster_ctl open_tmux`
    * delete temporary files
