public_key_path = "~/.ssh/kafka_aws.pub"
region          = "us-east-1"
ami             = "ami-c998b6b2" // RHEL-7.4

instance_types = {
  "kafka"     = "i3.4xlarge"
  "zookeeper" = "t2.small"
  "client"    = "c5.2xlarge"
}

num_instances = {
  "client"    = 4
  "kafka"     = 3
  "zookeeper" = 3
}
