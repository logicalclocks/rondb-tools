provider "aws" {
  region = var.region
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

data "aws_availability_zones" "available" {}

# Subnets
resource "aws_subnet" "subnet" {
  count             = var.num_azs
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "vm-subnet-${count.index}"
  }
}

# Get the list of subnet IDs
locals {
  selected_subnets = aws_subnet.subnet[*].id
}

resource "aws_route_table" "rt" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

# Associate each subnet with the public route table
resource "aws_route_table_association" "rt" {
  count          = length(local.selected_subnets)
  subnet_id      = local.selected_subnets[count.index]
  route_table_id = aws_route_table.rt.id
}

resource "aws_security_group" "rondb_bench" {
  name        = "rondb_bench"
  description = "Expose ssh, grafana and locust"
  vpc_id      = aws_vpc.main.id

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all internal traffic
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # Expose SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Expose nginx (for Grafana and Locust web UI)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  # 099720109477 is the OwnerId for Canonical, see
  # https://documentation.ubuntu.com/aws/aws-how-to/instances/find-ubuntu-images/
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-*-22.04-*-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
  filter {
    name   = "architecture"
    values = [
      var.cpu_platform == "arm64_v8" ? "arm64" : "x86_64"
    ]
  }
}

resource "aws_instance" "ndb_mgmd" {
  count                  = 1
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.ndb_mgmd_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  tags = {
    Name = "ndb_mgmd"
  }
}

resource "aws_instance" "ndbmtd" {
  count                  = var.ndbmtd_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.ndbmtd_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  root_block_device {
    volume_size = var.ndbmtd_disk_size
    volume_type = "gp3"
  }
  tags = {
    Name = "ndbmtd_${count.index + 1}"
  }
}

resource "aws_instance" "mysqld" {
  count                  = var.mysqld_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.mysqld_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  root_block_device {
    volume_size = var.mysqld_disk_size
    volume_type = "gp3"
  }
  tags = {
    Name = "mysqld_${count.index + 1}"
  }
}

resource "aws_instance" "rdrs" {
  count                  = var.rdrs_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.rdrs_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  root_block_device {
    volume_size = var.rdrs_disk_size
    volume_type = "gp3"
  }
  tags = {
    Name = "rdrs_${count.index + 1}"
  }
}

resource "aws_instance" "prometheus" {
  count                  = 1
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.prometheus_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  root_block_device {
    volume_size = var.prometheus_disk_size
    volume_type = "gp3"
  }
  tags = {
    Name = "prometheus"
  }
}

resource "aws_instance" "grafana" {
  count                  = 1
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.grafana_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  tags = {
    Name = "grafana"
  }
}

data "aws_ec2_instance_type" "bench_type" {
  instance_type = aws_instance.bench[0].instance_type
}

resource "aws_instance" "bench" {
  count                  = var.bench_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.bench_instance_type
  subnet_id              = local.selected_subnets[count.index % length(local.selected_subnets)]
  associate_public_ip_address = true
  vpc_security_group_ids = [aws_security_group.rondb_bench.id]
  key_name               = var.key_name
  root_block_device {
    volume_size = var.bench_disk_size
    volume_type = "gp3"
  }
  tags = {
    Name = "bench_${count.index + 1}"
  }
}

resource "aws_lb" "rdrs_nlb" {
  name               = "rdrs-lbs"
  internal           = true
  load_balancer_type = "network"
  subnets            = local.selected_subnets
}

resource "aws_lb_target_group" "rdrs_tg" {
  name        = "rdrs-targets"
  port        = 4406
  protocol    = "TCP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"
}

resource "aws_lb_target_group_attachment" "rdrs_tg_attachments" {
  for_each = { for idx, inst in aws_instance.rdrs : idx => inst }
  target_group_arn  = aws_lb_target_group.rdrs_tg.arn
  target_id        = each.value.private_ip
  port              = 4406
}

resource "aws_lb_listener" "rdrs_listener" {
  load_balancer_arn = aws_lb.rdrs_nlb.arn
  port              = 4406
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.rdrs_tg.arn
  }
}

resource "aws_lb" "rondis_nlb" {
  name               = "rondis-lbs"
  internal           = true
  load_balancer_type = "network"
  subnets            = local.selected_subnets
}

resource "aws_lb_target_group" "rondis_tg" {
  name        = "rondis-targets"
  port        = 6379
  protocol    = "TCP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"
}

resource "aws_lb_target_group_attachment" "rondis_tg_attachments" {
  for_each = { for idx, inst in aws_instance.rdrs : idx => inst }
  target_group_arn  = aws_lb_target_group.rondis_tg.arn
  target_id        = each.value.private_ip
  port              = 6379
}

resource "aws_lb_listener" "rondis_listener" {
  load_balancer_arn = aws_lb.rondis_nlb.arn
  port              = 6379
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.rondis_tg.arn
  }
}
