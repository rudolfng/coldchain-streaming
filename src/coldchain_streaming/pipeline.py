"""Pipeline Kafka-a-Kafka de Apache Beam, ejecutado por Flink."""

from __future__ import annotations

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from coldchain_streaming.config import Settings
from coldchain_streaming.transforms import (
    ParseAndValidate,
    build_metrics,
    dlq_to_kafka_record,
    metric_to_kafka_record,
)


def java_kafka_expansion_service(*extra_args: str):
    """Ejecuta los stages Java de KafkaIO como procesos dentro de los TaskManagers de Flink."""
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
            *extra_args,
        ]
    )


def pipeline_options(settings: Settings, *, job_name: str) -> PipelineOptions:
    return PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={settings.job_endpoint}",
            "--environment_type=PROCESS",
            '--environment_config={"command":"/opt/coldchain-lab/python-sdk/boot"}',
            "--streaming",
            f"--parallelism={settings.parallelism}",
            f"--job_name={job_name}",
            "--experiments=use_deprecated_read",
        ]
    )


def build_pipeline(
    pipeline: beam.Pipeline,
    settings: Settings,
    *,
    group_id: str,
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    # Lectura clásica (use_deprecated_read): con la lectura por SDF los checkpoints no se
    # completaban y la DLQ perdía su último lote (README sección 11).
    read_kwargs = {}
    if max_num_records is not None:
        read_kwargs["max_num_records"] = max_num_records
    if max_read_time is not None:
        read_kwargs["max_read_time"] = max_read_time

    raw = pipeline | "Read telemetry from Kafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[settings.topic_raw],
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=java_kafka_expansion_service("--experiments=use_deprecated_read"),
        **read_kwargs,
    )
    parsed = raw | "Parse and validate" >> beam.ParDo(ParseAndValidate()).with_outputs(
        ParseAndValidate.INVALID, main="valid"
    )
    metrics = build_metrics(parsed.valid, settings)
    (
        parsed.invalid
        # Sin este Reshuffle la rama inválida queda fusionada con la fuente no acotada (no
        # pasa por ningún GroupByKey, a diferencia de la válida): los mensajes llegan a
        # "Encode DLQ records" pero nunca se escriben en Kafka, sin ninguna excepción visible.
        | "Reshuffle before DLQ write" >> beam.Reshuffle()
        | "Encode DLQ records" >> beam.Map(dlq_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Write DLQ to Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.topic_dlq,
            expansion_service=java_kafka_expansion_service(),
        )
    )
    (
        metrics
        | "Encode metric records"
        >> beam.Map(metric_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Write metrics to Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.topic_metrics,
            expansion_service=java_kafka_expansion_service(),
        )
    )
    return metrics, parsed.invalid


def run(
    *,
    group_id: str = "beam-coldchain-metrics-v1",
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    settings = Settings.from_env()
    options = pipeline_options(settings, job_name=os.getenv("BEAM_JOB_NAME", "coldchain-metrics"))
    pipeline = beam.Pipeline(options=options)
    build_pipeline(
        pipeline,
        settings,
        group_id=group_id,
        max_num_records=max_num_records,
        max_read_time=max_read_time,
    )
    result = pipeline.run()
    result.wait_until_finish()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default="beam-coldchain-metrics-v1")
    parser.add_argument("--max-num-records", type=int)
    parser.add_argument("--max-read-time", type=int)
    args = parser.parse_args()
    result = run(
        group_id=args.group_id,
        max_num_records=args.max_num_records,
        max_read_time=args.max_read_time,
    )
    print(json.dumps({"state": str(result.state)}))


if __name__ == "__main__":
    main()
