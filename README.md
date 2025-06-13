# RonDB Tools

This tool lets you deploy a RonDB clusters on AWS, monitor the cluster and run benchmarks.

## Simple usage

A minimal guide to get started.

1. Run `nix-shell` **or** install AWS CLI, terraform, python3 and rsync.
2. Configure your AWS API key
  ```
  aws configure
  ```
3. Initialize and deploy the cluster
  ```
  ./create_aws_ssh_key
  terraform init
  terraform apply
  ./cluster_ctl deploy
  ```
  Go to the printed web address to monitor the cluster using grafana dashboards.
4. Run `./cluster_ctl bench_locust` to create test data and start locust.
  Go to the printed web address to access locust.
5. In the locust GUI, set the number of users to the same as the number of workers and press `START`.
  During the benchmark run, you can use both locust and grafana to gather statistics, from the client's and server's point of view, respectively.
6. Run `./cleanup` to destroy the AWS resources and local files created.

## Manual

### Setup & deploy

1. The easiest way to get all necessary dependencies is to install `nix` and run `nix-shell`.
  You'll get a bash session with access to the dependencies, without modifying your system directories.
  Otherwise:
    * Install terraform, see https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
    * Install AWS CLI, python3, rsync and tmux.
      * On Debian/Ubuntu: `apt-get update && apt-get install -y awscli python3 rsync tmux`
      * On macOS: `brew install awscli python3 rsync tmux`
2. You need an AWS API key to create cloud resources.
  Run `aws configure` and enter it.
3. Edit `terraform.tfvars` to your liking.
  Refer to *Reconfiguring the cluster* if you want to change it later.
4. Run `./create_aws_ssh_key`.
  This will be used to access the VMs.
5. Run `terraform init` to initialize the terraform state files.
6. Run `terraform apply` to create the VMs and other AWS resources.
7. Run `./cluster_ctl deploy` to install and start RonDB services.

### Reconfigure a cluster

A cluster can be reconfigured by changing the values in `terraform.tfvars` and then update the cluster.
The necessary steps depend on what variables you've changed.
* The `region` cannot be updated for an existing cluster.
  If you want to use a different region, *first* run `./cleanup`, then start over at *Setup & deploy* above, step 3.
  The same goes for `key_name`, although there should be no reason to change that.
* If you have changed variables that affect AWS resources, you must run `terraform apply`.
  These include `num_azs`, `cpu_platform`, `*_instance_type` and `*_count`.
  If you are unsure, run `terraform apply` anyways and it will exit quickly in case there are no changes.
* Run `./cluster_ctl deploy` or `./cluster_ctl install`.
  Technically it is enough to do so only for the affected node types.
  However, this may include more types than you suppose.
  For example, changing the number of `ndbmtd` nodes will affect the sequence of node IDs assigned to `mysqld`, `rdrs` and `bench` nodes, which will also affect the `ndb_mgmd` config.
  It's easiest to run `./cluster_ctl deploy` without specifying a node type, to redeploy all.
* The test data table is not retained across data node reinstallations or restarts.
  You'll have to repopulate using `./cluster_ctl bench_locust` or `./cluster_ctl populate`.
* If you have run `./cluster_ctl deploy` or `./cluster_ctl start` on `bench` nodes, locust will be stopped but not started.
  You'll have to use `./cluster_ctl bench_locust` or `./cluster_ctl start_locust` for that.

### Cleanup

Cleanup is simple: just run `./cleanup`.
This will
* destroy the terraform cluster created by `terraform apply`
* delete the AWS SSH key created by `./create_aws_ssh_key`
* remove the tmux session created by `./cluster_ctl open_tmux`
* delete temporary files

### `cluster_ctl` reference

`cluster_ctl` lets you deploy, control, reconfigure and access a RonDB cluster.

Some sub-commands take a `NODE_TYPE` argument.
If given, the command will operate only on nodes of that node type, otherwise on all nodes.
Available node types are: `ndb_mgmd`, `ndbmtd`, `mysqld`, `rdrs`, `prometheus`, `grafana` and `bench`.

* `./cluster_ctl deploy [NODE_TYPE]`
    A convenience command equivalent to `install` followed by `start`.

* `./cluster_ctl install [NODE_TYPE]`
    Install necessary software and configuration files.
    Depending on the node type this may include RonDB, prometheus, grafana and locust.

* `./cluster_ctl start [NODE_TYPE]`
    Start or restart services.
    (This will not start locust, see `bench_locust`.)

* `./cluster_ctl stop [NODE_TYPE]`
    Stop services.

* `./cluster_ctl open_tmux [NODE_TYPE]`
    Create a tmux session with ssh connections opened to all applicable nodes.

    When attached to the tmux session, you can press `C-b n` to go to the next window, and `C-b d` to detach.

    If a session already exists, ignore the `NODE_TYPE` argument and reattach to it.
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
    * `COLUMNS`: Number of columns in table. Default 100.
    * `ROWS`: Number of rows in table. Default 100000.
    * `COLUMN_TYPES`: The columns types in table; one of `INT`, `VARCHAR`, `INT_AND_VARCHAR`. Default `INT`.

* `./cluster_ctl start_locust [--rows ROWS] [--workers WORKERS]\`
    Start or restart locust services.
    You need to run `populate` first.
    Parameters:
    * `ROWS`: Number of rows in table. Default 100000.
            **Warning**: This must be set to the same value as the last call to `populate`.
            Prefer using `bench_locust`, which will do the right thing automatically.
    * `WORKERS`: Number of locust workers. Defaults to the maximum possible (one per available CPU).

* `./cluster_ctl stop_locust`
    Stop locust.

* `./cluster_ctl list`
    Print node information, including node name, tyuupe, IPs and NDB Node IDs.
