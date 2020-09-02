public_key_path = "~/.ssh/pravega_aws.pub"
region          = "us-west-2"
ami             = "ami-9fa343e7" // RHEL-7.4 us-west-2

instance_types = {
  "controller"   = "i3en.2xlarge"
  "bookkeeper"   = "i3en.2xlarge"
  "zookeeper"    = "i3en.2xlarge"
  "client"       = "c5n.2xlarge"
  "metrics"      = "c5.2xlarge"
}

num_instances = {
  "controller"   = 1
  "bookkeeper"   = 3
  "zookeeper"    = 3
  "client"       = 2
  "metrics"      = 1
}
