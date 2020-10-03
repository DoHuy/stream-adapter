# webstream-adapter

This adapter can read any web stream or static file (audio, video, images, and text documents) accessible over the web. It supports any stream supported by FFMPEG, including HLS, MPEG-DASH, RTSP, MJPEG, and others.

## Requirements

Requires the following on your local machine to build and run locally:

- Go 1.9.2+
- make
- ffmpeg 3.4+ (`brew install ffmpeg` on Mac OS X)
- streamlink (`pip install streamlink` - learn more https://streamlink.github.io)

Or, to build and run as a Docker container, all you need is:

- make
- Docker

## Building and Running

Get the repo:

```
go get github.com/webstream-adapter
cd $GOPATH/src/github.com/webstream-adapter
```

To build, run:

```
make
```

Obtain a user token and set it as an environment variable. Also, set your Kafka broker addresses, Github user token, payload and an output topic, partition and prefix:

```
export API_TOKEN=XXXXXXXXXXXXXXXXXXXXXXXXX
export GITHUB_ACCESS_TOKEN=XXXXXXXXXXXXXXXXXXXXXXXXX
export KAFKA_BROKERS=kafka1:9092,kafka2:9092
export STREAM_OUTPUT_TOPIC=stream_1:0:1234abcd__
export PAYLOAD_JSON="{\"jobId\":\"job1\",\"taskId\":\"task1\",\"recordingId\":\"400002379\",\"taskPayload\":{\"mode\":\"ingest\",\"url\":\"https://s3.amazonaws.com/dev-chunk-cache-tmp/AC.mp4\",\"clusterId\":\"rt-deadbeef-0000-0001-0001-ba5eba111111\",\"valueSearch\":\"\",\"organizationId\":7682}}"
```

Running by docker container:

```
docker-compose up and stop with docker-compose down
```

Runing by binary:

Note: If your provided payload file contains the `token` field, webstream-adapter will use that token instead. This will be the case when running actual tasks in production.

Then, to run the engine, provide a payload using the `-payload` flag:

```
./webstream-adapter -payload sample/payload-video.json
```

There are several sample payloads included in the repo. Alternatively, you can specify payload contents directly using the `PAYLOAD_JSON` environment variable. Example:

```
PAYLOAD_JSON="{\"jobId\": \"job1\", \"taskId\": \"task1\", ...}" ./webstream-adapter
```

The provided config file, `config.json`, will be used by default. To use a custom configuration, create a new JSON config file and specify it using the `-config` flag:

```
./webstream-adapter -config my_config.json -payload payload.json
```

Or, set an environment variable:

```
export CONFIG_FILE=my_config.json
./webstream-adapter -payload payload.json
```

The following environment variables are available and will override any settings in the configuration file:

- ENGINE_ID: ID of this engine
- ENGINE_INSTANCE_ID: a unique uuid representing the current instance (included with each heartbeat)
- KAFKA_BROKERS: comma-separated list of kafka broker address (host:port)
- KAFKA_CHUNK_TOPIC: topic name for outgoing media chunk messages (default: chunk_all)
- KAFKA_HEARTBEAT_TOPIC: topic name for engine hearbeat messages (default: engine_status)
- API_BASE_URL: env-specific base URL for Veritone GraphQL API
- STREAM_OUTPUT_TOPIC: output topic, partition, and prefix to write the stream to, using the convention `topic:partition:prefix` (ex: `stream_1:0:1234abcd__`)

The URL to read from can be provided in any of the following ways:

- `url` field in the payload
- `url` field in the schema of a source, specified by `sourceId` in the payload
- `--url example.url.com/stream` command line flag

## Docker

To build the Docker container image for WebStream Adapter, use the following command:

```
make build-docker GITHUB_ACCESS_TOKEN=xxxxxxxxxxxxxxxxxxx
```

`GITHUB_ACCESS_TOKEN` is your GitHub personal access token. Set the `CONFIG_FILE` and `DEFAULT_PAYLOAD_FILE` environment variables with the command above if you would like to build the image with a different default config or payload file.

To run the container, following the steps:

1. Create a docker network

```
docker network create ExampleNetwork
```

2. Run Kafka/Zookeeper container

```
docker run -d --net ExampleNetwork -p 2181:2181 -p 9092:9092 --rm --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 --name kafka --hostname kafka spotify/kafka
```

3. Run the adapter container

```
export API_TOKEN=xxxxxxxxxxxxxxxxxxx
export STREAM_OUTPUT_TOPIC=xxxxxxxxxxxxxxxxxxx
docker run -t --net ExampleNetwork --rm --env API_TOKEN --env STREAM_OUTPUT_TOPIC webstream-adapter -payload payload.json
```

This will run webstream-adapter using the default payload file included in the container. To run it using a custom payload, set the `PAYLOAD_JSON` environment variable:

```
export PAYLOAD_JSON=xxxxxxxxxxxxxxxxxxx
docker run -t --net ExampleNetwork --rm --env API_TOKEN --env STREAM_OUTPUT_TOPIC --env PAYLOAD_JSON webstream-adapter
```

# Check job, tasks status

Login to CMS
```

Use query as example bellow to get job, task status, target, engine, payload, output, ...

```
{
  job(id:"19072916_W3TphRKNNY") {
    tasks {
      records {
        targetId
        engine {
          id
          name
        }
        status
        taskOutput
        taskPayload
        executionLocation{
          data
        }
      }
    }
  }
}
```

Query result example:

```
{
  "data": {
    "job": {
      "tasks": {
        "records": [
          {
            "targetId": "580392961",
            "engine": {
              "id": "ea0ada2a-7571-4aa5-9172-b5a7d989b041",
              "name": "Stream Ingestor"
            },
            "status": "failed",
            "taskOutput": {
              "info": "{\"assets\":{},\"chunks\":{},\"durationIngested\":\"0s\",\"ingestionTime\":\"17.804µs\"}",
              "error": "failed to parse QT atoms from stream: Invalid file format: Atom \"mfra\" at offset 0 reported a size of only 0 bytes",
              "uptime": "9m44.191s",
              "bytesRead": "1382635520",
              "heartbeats": "118",
              "bytesWritten": "0",
              "failureReason": "stream_read_error",
              "failureMessage": "failed to parse QT atoms from stream: Invalid file format: Atom \"mfra\" at offset 0 reported a size of only 0 bytes",
              "messagesWritten": "0",
              "engineInstanceId": "a63fa1ea-179d-4a5d-81ef-6143686ab336"
            },
            "taskPayload": {
              "comment": "stream ingestor",
              "organizationId": 7682,
              "extractFramesPerSec": 1,
              "generateMediaAssets": true,
              "outputChunkDuration": "5m",
              "chunkOverlapDuration": "0s"
            },
            "executionLocation": {
              "data": {
                "name": "EC2/SEM-PROD-RTea0ada2a-7571-4aa5-9172-b5a7d989b041-E4096-StreamIngestor_E/cc846cb3-af08-472e-99a3-b6575ea18fb1"
              }
            }
          },
          {
            "targetId": "580392961",
            "engine": {
              "id": "54525249-da68-4dbf-b6fe-aea9a1aefd4d",
              "name": "Speechmatics - English (Global) - V2F"
            },
            "status": "aborted",
            "taskOutput": {
              "info": "",
              "error": "",
              "chunkCount": "0",
              "errorCount": "0",
              "failureRate": "0.000",
              "pausedCount": "0",
              "ignoredCount": "0",
              "successCount": "0",
              "filteredCount": "0",
              "noStatusCount": "0",
              "conductorStats": "",
              "invalidStatusCount": "0"
            },
            "taskPayload": {
              "diarise": "true",
              "organizationId": 7682
            },
            "executionLocation": null
          },
          {
            "targetId": "580392961",
            "engine": {
              "id": "9e611ad7-2d3b-48f6-a51b-0a1ba40feab4",
              "name": "Webstream Adapter"
            },
            "status": "complete",
            "taskOutput": {
              "info": "",
              "error": "",
              "uptime": "9m42.704s",
              "bytesRead": "1382665507",
              "heartbeats": "117",
              "bytesWritten": "1382665507",
              "messagesWritten": "0",
              "engineInstanceId": "33e62e90-610d-48e4-8789-fb3bad589f0a"
            },
            "taskPayload": {
              "url": "https://s3.amazonaws.com/prod-api.veritone.com/7682/other/2019/6/2/_/TheTechGuy-TWiT.tv_20190715_ttg1609_h264m_1280x720_1872-17-38-741_ad8f82e2-506d-4178-a012-14e85f47214b.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQMR5VATUHU3MEGOA%2F20190716%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20190716T173538Z&X-Amz-Expires=86400&X-Amz-Signature=75acce8c171abb0f60dcad2def909221870d90d883c5259f3d0801ccf2e432ff&X-Amz-SignedHeaders=host",
              "tdoId": "580392961",
              "organizationId": 7682,
              "startTimeOverride": 1563297367
            },
            "executionLocation": {
              "data": {
                "name": "EC2/SEM-PROD-RT9e611ad7-2d3b-48f6-a51b-0a1ba40feab4-E256-WebstreamAdapter_E/cfff5ffa-1545-4657-a45f-52eab2da0277"
              }
            }
          }
        ]
      }
    }
  }
}
```

We can check log of stream engine by copy executionLocation data and search on cloud watch.

Open 

https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logs:

Then click on the link of the environment of the task.

Example:
```
/aws/ecs/prod-rt
/aws/ecs/stage-rt
/aws/ecs/dev-rt
```
After that paste executionLocation data to filter.


# Troubleshooting

FFMPEG run failed.

* Update Dockerfile to upgrade ffmpeg
* Build docker and try running by docker

Cannot process the URL

* Check URL to ensure it's valid

Other

* Contact Webstream-adapter owner
