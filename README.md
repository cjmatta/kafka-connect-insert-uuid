Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

This SMT supports inserting a UUID into the record Key or Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`uuid.field.name`| Field name for UUID | String | `uuid` | High |

Example on how to add to your connector:
```
transforms=insertuuid
transforms.insertuuid.type=com.github.cjmatta.kafka.connect.smt.InsertUuid$Value
transforms.insertuuid.uuid.field.name="uuid"
```


ToDO
* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT