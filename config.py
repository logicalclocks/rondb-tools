
# This config file is a python script. Only the config variable in the end
# matters, the rest are for convenience.

# For RonDB, we need the RonDB version and the glibc version.
latest_rondb = {
    "glibc_version": "2.28",
    "rondb_version": "24.10.8",
}

cluster_size = {
    # The number of availability zones to use.
    # Numbers larger than 1 means multi-AZ environment and 1 means single-AZ.
    "num_azs": 1,
    # For ndbmtd, mysqld, rdrs and bench nodes, we can specify the number of VMs
    # to use.
    "ndbmtd_count": 1,
    "mysqld_count": 1,
    "rdrs_count": 1,
    "bench_count": 1,
    # We also need to specify the number of data node replicas.
    # Note that replicas * node groups = data nodes.
    # Therefore ndbmtd_count must be divisible by rondb_replicas.
    "rondb_replicas": 1,
    # Disk sizes in GiB
    "ndbmtd_disk_size": 200,
    "mysqld_disk_size": 60,
    "rdrs_disk_size": 60,
    "prometheus_disk_size": 120,
    "bench_disk_size": 60,
}

# CPU platform and node types. All nodes will use the same CPU platform, either
# arm64_v8 or x86_64.
arm_config = {
    "cpu_platform": "arm64_v8",
    "ndb_mgmd_instance_type": "c8g.medium",
    "ndbmtd_instance_type": "c8g.2xlarge",
    "mysqld_instance_type": "c8g.medium",
    "rdrs_instance_type": "c8g.2xlarge",
    "prometheus_instance_type": "c8g.medium",
    "grafana_instance_type": "c8g.medium",
    "bench_instance_type": "c8g.2xlarge",
}
x86_config = {
    "cpu_platform": "x86_64",
    "ndb_mgmd_instance_type": "t3.large",
    "ndbmtd_instance_type": "t3.2xlarge",
    "mysqld_instance_type": "t3.large",
    "rdrs_instance_type": "t3.2xlarge",
    "prometheus_instance_type": "t3.medium",
    "grafana_instance_type": "t3.medium",
    "bench_instance_type": "t3.2xlarge",
}

config= {

    # AWS region
    "region": "eu-north-1",

    # RonDB version
    **latest_rondb,

    # Cluster size.
    **cluster_size,

    # Node type configs.
    **arm_config,
    #**x86_config,
}
