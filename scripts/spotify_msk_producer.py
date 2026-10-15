sh-5.2$ cd ~/spotify-msk
sh-5.2$ source .venv/bin/activate
(.venv) sh-5.2$ pwd
/home/ssm-user/spotify-msk
(.venv) sh-5.2$ ls
spotify_msk_producer.py
(.venv) sh-5.2$ python spotify_msk_producer.py
Traceback (most recent call last):
  File "/home/ssm-user/spotify-msk/spotify_msk_producer.py", line 55, in <module>
    metadata = producer.send(TOPIC, event).get(timeout=30)
  File "/home/ssm-user/spotify-msk/.venv/lib64/python3.9/site-packages/kafka/producer/kafka.py", line 930, in send
    self._wait_on_metadata(topic, self.config['max_block_ms'])
  File "/home/ssm-user/spotify-msk/.venv/lib64/python3.9/site-packages/kafka/producer/kafka.py", line 1053, in _wait_on_metadata
    raise Errors.KafkaTimeoutError(
kafka.errors.KafkaTimeoutError: KafkaTimeoutError: Failed to update metadata after 60.0 secs.
(.venv) sh-5.2$ python - <<'PY'
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"
BOOTSTRAP = "boot-kvntzomh.c2.kafka-serverless.us-east-1.amazonaws.com:9098"

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

admin = KafkaAdminClient(
    bootstrap_servers=[BOOTSTRAP],
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(),
    request_timeout_ms=15000,
    api_version_auto_timeout_ms=15000,
)

try:
    result = admin.create_topics(
        [NewTopic(name="spotify-tracks", num_partitions=1, replication_factor=2)],
        validate_only=False,
    )
    print(result)
except Exception as e:
    print(type(e).__name__, str(e))
finally:
    admin.close()
PY
Traceback (most recent call last):
  File "<stdin>", line 13, in <module>
  File "/home/ssm-user/spotify-msk/.venv/lib64/python3.9/site-packages/kafka/admin/client.py", line 219, in __init__
    raise KafkaConfigurationError("Unrecognized configs: {}".format(extra_configs))
kafka.errors.KafkaConfigurationError: KafkaConfigurationError: Unrecognized configs: {'api_version_auto_timeout_ms'}
(.venv) sh-5.2$ python - <<'PY'
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"
BOOTSTRAP = "boot-kvntzomh.c2.kafka-serverless.us-east-1.amazonaws.com:9098"

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

admin = None

try:
    admin = KafkaAdminClient(
        bootstrap_servers=[BOOTSTRAP],
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        request_timeout_ms=15000,
    )

    result = admin.create_topics(
        [NewTopic(
            name="spotify-tracks",
            num_partitions=1,
            replication_factor=2
        )],
        validate_only=False,
    )
    print(result)

except Exception as e:
    print(type(e).__name__, str(e))

finally:
    if admin is not None:
        admin.close()
PY
{'topics': [{'name': 'spotify-tracks', 'topic_id': None, 'error_code': 0, 'error_message': None, 'topic_config_error_code': 29, 'num_partitions': -1, 'replication_factor': -1}]}
(.venv) sh-5.2$ python spotify_msk_producer.py
Sent: Blinding Lights | Partition: 0 | Offset: 0
Sent: Billie Jean | Partition: 0 | Offset: 1
Spotify events sent successfully.
(.venv) sh-5.2$ cd ~/spotify-msk
source .venv/bin/activate
(.venv) sh-5.2$ nano spotify_msk_consumer.py
(.venv) sh-5.2$ python spotify_msk_consumer.py
/home/ssm-user/spotify-msk/spotify_msk_consumer.py:21: DeprecationWarning: value_deserializer does not implement kafka.serializer.Deserializer
  consumer = KafkaConsumer(
Topic: spotify-tracks
Partition: 0
Offset: 0
Spotify event:
{
  "event_id": "56f8c773-4ffd-487d-93e6-504c4f9e86e1",
  "event_timestamp": "2026-10-14T20:33:48.354021+00:00",
  "track_name": "Blinding Lights",
  "artist_name": "The Weeknd",
  "genre": "Synth-pop",
  "popularity": 95,
  "duration_ms": 200040
}
--------------------------------------------------
Topic: spotify-tracks
Partition: 0
Offset: 1
Spotify event:
{
  "event_id": "355bcae7-5843-4148-bcf7-3ac55d50817c",
  "event_timestamp": "2026-10-14T20:33:48.562313+00:00",
  "track_name": "Billie Jean",
  "artist_name": "Michael Jackson",
  "genre": "Pop",
  "popularity": 85,
  "duration_ms": 294000
}
--------------------------------------------------
Consumer finished. Events read: 2
(.venv) sh-5.2$ cd ~/spotify-msk
(.venv) sh-5.2$ ls -l
total 8
-rw-r--r--. 1 ssm-user ssm-user 1278 Oct 14 20:36 spotify_msk_consumer.py
-rw-r--r--. 1 ssm-user ssm-user 1653 Oct 14 17:00 spotify_msk_producer.py
(.venv) sh-5.2$ cat spotify_msk_consumer.py

import json

from kafka import KafkaConsumer
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"
BOOTSTRAP_SERVERS = [
    "boot-kvntzomh.c2.kafka-serverless.us-east-1.amazonaws.com:9098"
]
TOPIC = "spotify-tracks"


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="spotify-consumer-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    consumer_timeout_ms=15000,
)

try:
    count = 0

    for message in consumer:
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print("Spotify event:")
        print(json.dumps(message.value, indent=2, ensure_ascii=False))
        print("-" * 50)
        count += 1

    print(f"Consumer finished. Events read: {count}")

finally:
    consumer.close()
(.venv) sh-5.2$ s
sh: s: command not found
(.venv) sh-5.2$ ls
spotify_msk_consumer.py  spotify_msk_producer.py
(.venv) sh-5.2$ cat spotify_msk_producer.py
import json
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"
BOOTSTRAP_SERVERS = [
    "boot-kvntzomh.c2.kafka-serverless.us-east-1.amazonaws.com:9098"
]
TOPIC = "spotify-tracks"


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

tracks = [
    {
        "track_name": "Blinding Lights",
        "artist_name": "The Weeknd",
        "genre": "Synth-pop",
        "popularity": 95,
        "duration_ms": 200040,
    },
    {
        "track_name": "Billie Jean",
        "artist_name": "Michael Jackson",
        "genre": "Pop",
        "popularity": 85,
        "duration_ms": 294000,
    },
]

try:
    for track in tracks:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            **track,
        }

        metadata = producer.send(TOPIC, event).get(timeout=30)

        print(
            f"Sent: {track['track_name']} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

    producer.flush()
    print("Spotify events sent successfully.")

finally:
    producer.close()