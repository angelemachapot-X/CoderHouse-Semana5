output "kinesis_stream_name" {
  value = aws_kinesis_stream.sensor_stream.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.sensor_stream.arn
}
