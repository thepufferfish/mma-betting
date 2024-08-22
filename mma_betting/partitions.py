from dagster import DynamicPartitionsDefinition

ufc_events_partitions_def = DynamicPartitionsDefinition(name='ufc_events')
ufc_fights_partitions_def = DynamicPartitionsDefinition(name='ufc_fights')