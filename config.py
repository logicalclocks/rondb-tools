# This config file is a python script. Only the variable `config` in the end
# matters, the rest are for convenience.

# For RonDB, we need the RonDB version, glibc version and RDRS major version
latest_rondb = {
    "rondb_version": "24.10.10",
    "glibc_version": "2.28",
    "rdrs_major_version": 2,
}
latest_rondb_22 = {
    "rondb_version": "22.10.13",
    "glibc_version": "2.28",
    "rdrs_major_version": 1,
}

# Configure cluster size.
cluster_size_small = {
    # For ndbmtd, mysqld, rdrs and bench nodes, we can specify the number of
    # nodes to use.
    "ndbmtd_count": 1,
    "mysqld_count": 1,
    "rdrs_count": 1,
    "bench_count": 1,
    # We also need to specify the number of data node replicas.
    # Note that replicas * node groups = data nodes.
    # Therefore ndbmtd_count must be divisible by rondb_replicas.
    "rondb_replicas": 1,
}
cluster_size_medium = {
    "ndbmtd_count": 4,
    "mysqld_count": 2,
    "rdrs_count": 2,
    "bench_count": 10,
    "rondb_replicas": 2,
}

# Two deployment types are supported:
# 1) AWS EC2 VMs provisioned by Terraform
# 2) Static node configuration, e.g. dedicated machines or VMs

# For AWS: Configure disk sizes
disk_sizes_aws = {
    # Disk sizes in GiB
    "ndbmtd_disk_size": 200,
    "mysqld_disk_size": 60,
    "rdrs_disk_size": 60,
    "prometheus_disk_size": 120,
    "bench_disk_size": 60,
}

# For AWS: Configure CPU platform and node types. All nodes will use the same
# CPU platform, either arm64_v8 or x86_64.
arm_config_aws = {
    "cpu_platform": "arm64_v8",
    "ndb_mgmd_instance_type": "c8g.medium",
    "ndbmtd_instance_type": "c8g.2xlarge",
    "mysqld_instance_type": "c8g.medium",
    "rdrs_instance_type": "c8g.2xlarge",
    "prometheus_instance_type": "c8g.medium",
    "grafana_instance_type": "c8g.medium",
    "bench_instance_type": "c8g.2xlarge",
}
x86_config_aws = {
    "cpu_platform": "x86_64",
    "ndb_mgmd_instance_type": "t3.large",
    "ndbmtd_instance_type": "t3.2xlarge",
    "mysqld_instance_type": "t3.large",
    "rdrs_instance_type": "t3.2xlarge",
    "prometheus_instance_type": "t3.medium",
    "grafana_instance_type": "t3.medium",
    "bench_instance_type": "t3.2xlarge",
}

# For AWS: Tie together all necessary config variables except RonDB version.
aws_config = {
    "deployment_type": "aws",

    # AWS region
    "region": "eu-north-1",

    # The number of availability zones to use.
    # Numbers larger than 1 means multi-AZ environment and 1 means single-AZ.
    "num_azs": 1,

    # Cluster size
    **cluster_size_small,
    #**cluster_size_medium,

    # Node type configs.
    **arm_config_aws,
    #**x86_config_aws,

    # Disk sizes
    **disk_sizes_aws,
}

# An example static configuration, meaning the nodes are provided by you, not
# created by this tool.
static_config = {
    "deployment_type": "static",
    # A username with ssh and sudo access.
    "node_user": "vagrant",
    # "x86_64" or "arm64_v8"
    "cpu_platform": "x86_64",
    "rondb_replicas": 1,
    # The ssh key file used to access the nodes.
    "ssh_key_file": f"{__import__('os').environ['HOME']}/.ssh/id_static_rondb_cluster",
    # Fill out IPv4 addresses to all nodes. Public IPs are used to control the
    # nodes via ssh, and private IPs are used for internal communication. Public
    # and private addresses can be the same if desired.
    "ndb_mgmd_public_ips": [],
    "ndb_mgmd_private_ips": [],
    "ndbmtd_public_ips": [],
    "ndbmtd_private_ips": [],
    "mysqld_public_ips": [],
    "mysqld_private_ips": [],
    "rdrs_public_ips": [],
    "rdrs_private_ips": [],
    "prometheus_public_ips": [],
    "prometheus_private_ips": [],
    "grafana_public_ips": [],
    "grafana_private_ips": [],
    "bench_public_ips": [],
    "bench_private_ips": [],
    # Number of CPUs on the bench nodes.
    "bench_cpus_per_node": 12,
}

demo_rondb_com_config = {
    "deployment_type": "static",
    "node_user": "vagrant",
    "cpu_platform": "x86_64",
    "rondb_replicas": 1,
    "ssh_key_file": f"{__import__('os').environ['PWD']}/id_rondbdemo",
    "ndb_mgmd_public_ips": ['10.87.58.10'],
    "ndb_mgmd_private_ips": ['10.87.58.10'],
    "ndbmtd_public_ips": ['10.87.56.20', '10.87.57.21'],
    "ndbmtd_private_ips": ['10.87.56.20', '10.87.57.21'],
    "mysqld_public_ips": ['10.87.57.30'],
    "mysqld_private_ips": ['10.87.57.30'],
    "rdrs_public_ips": ['10.87.58.40'],
    "rdrs_private_ips": ['10.87.58.40'],
    "prometheus_public_ips": ['10.87.57.50'],
    "prometheus_private_ips": ['10.87.57.50'],
    "grafana_public_ips": ['10.87.57.60'],
    "grafana_private_ips": ['10.87.57.60'],
    "bench_public_ips": ['10.87.56.70'],
    "bench_private_ips": ['10.87.56.70'],
    "bench_cpus_per_node": 12,
}

config = {
    # RonDB version
    **latest_rondb,
    #**latest_rondb_22,

    # All other configuration, either AWS or static
    #**aws_config
    #**static_config
    **demo_rondb_com_config
}
