package consumers;

import model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class EmployeeConsumers {

    public static void main(String[] args) throws Exception{

        String topicName = "Topic-A";
        String groupName = "Group-B";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "utility.EmployeeDeserializer");


        KafkaConsumer<String, Employee> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        while (true){
            ConsumerRecords<String, Employee> records = consumer.poll(100);
            for (ConsumerRecord<String, Employee> record : records){
                System.out.println(
                        "Employee Id= " + record.value().getEmployeeId() +
                                " Employee  Name = " + record.value().getEmployeeName() +
                                " Employee join Date = " + record.value().getEmployeeJoinDate().toString());
            }
        }
    }
}