# Getting Started

### Create Topics requried for this project

bin/kafka-topics.sh --create --topic sensor-in-topic --bootstrap-server a14b267fa546e46449f4603acb5ffed6-535234883.ap-south-1.elb.amazonaws.com:9094
bin/kafka-topics.sh --create --topic sensor-out-topic --bootstrap-server a14b267fa546e46449f4603acb5ffed6-535234883.ap-south-1.elb.amazonaws.com:9094

### Produce is available under com.somesh.kstream.producers package


