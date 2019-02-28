Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

add to your connector:
```
transforms=adduuid
transforms.adduuid.type=com.github.cjmatta.kafka.connect.smt.AddUuid
transforms.adduuid.uuid.field.name="uuid"
```
