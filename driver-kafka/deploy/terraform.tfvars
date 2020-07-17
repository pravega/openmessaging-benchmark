public_key_path = "~/.ssh/kafka_aws.pub"
region          = "us-west-2"
ami             = "ami-9fa343e7" // RHEL-7.4

instance_types = {
  "kafka"     = "i3.4xlarge"
  "zookeeper" = "t3.small"
  "client"    = "c5.4xlarge"
}

num_instances = {
  "client"    = 2
  "kafka"     = 3
  "zookeeper" = 3
}
