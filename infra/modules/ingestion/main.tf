resource "aws_kinesis_stream" "sensor_stream" {
  name             = "${var.environment}-sensor-events-stream"
  shard_count      = var.shard_count
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Name        = "${var.environment}-sensor-events-stream"
    Environment = var.environment
  }
}
