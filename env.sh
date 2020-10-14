export KAFKA_TOPIC="events"
export KAFKA_DEAD_LETTERS_TOPIC="events-shoreline-dl"
export KAFKA_VERSION="2.4.0"
export KAFKA_REQUIRE_SSL=true
export KAFKA_BROKERS="b-1.default-ops.gxm6gl.c4.kafka.us-west-2.amazonaws.com:9094,b-2.default-ops.gxm6gl.c4.kafka.us-west-2.amazonaws.com:9094,b-3.default-ops.gxm6gl.c4.kafka.us-west-2.amazonaws.com:9094"
export CLOUD_EVENTS_SOURCE="shoreline"
export KAFKA_CONSUMER_GROUP="shoreline" 

export SERVER_SECRET="This needs to be the same secret everywhere. YaHut75NsK1f9UKUXuWqxNN0RUwHFBCy"