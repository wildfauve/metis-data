import metis_data

def test_stream_writer_provides_trigger_condition_overriding_default():
    trigger_opt = {'processingTime': '1 minute'}
    writer = metis_data.SparkStreamingTableWriter(trigger_condition=trigger_opt)

    assert writer.trigger_condition == trigger_opt
    assert writer.write_trigger(None) == trigger_opt
    assert writer.write_trigger({"availableNow": True}) == {"availableNow": True}