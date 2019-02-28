package com.github.cjmatta.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class AddUuid<R extends ConnectRecord<R>> implements Transformation {

  public static final String OVERVIEW_DOC =
    "Insert a random UUID into a connect record";

  private interface ConfigName {
    String UUID_FIELD_NAME = "uuid.field.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.UUID_FIELD_NAME, ConfigDef.Type.STRING, "uuid", ConfigDef.Importance.HIGH,
      "Field name for UUID");

  private static final String PURPOSE = "adding UUID to record";

  private String fieldName;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(ConfigName.UUID_FIELD_NAME);
  }

  @Override
  public ConnectRecord apply(ConnectRecord connectRecord) {
    Schema currentSchema = connectRecord.valueSchema();
    final Struct currentValue = requireStruct(connectRecord.value(), PURPOSE);
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(currentSchema, SchemaBuilder.struct());

    for (Field field : currentSchema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(fieldName, Schema.STRING_SCHEMA);

    Schema newSchema = builder.build();

    final Struct updatedValue = new Struct(newSchema);

    for (Field field : currentSchema.schema().fields()) {
      updatedValue.put(field.name(), currentValue.get(field));
    }

    updatedValue.put(fieldName, UUID.randomUUID());

    return connectRecord.newRecord(
      connectRecord.topic(),
      connectRecord.kafkaPartition(),
      connectRecord.keySchema(),
      connectRecord.key(),
      newSchema,
      updatedValue,
      connectRecord.timestamp()
    );
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {

  }
}


