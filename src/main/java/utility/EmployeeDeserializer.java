package utility;

import model.Employee;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class EmployeeDeserializer implements Deserializer<Employee> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Nothing to configure
    }

    @Override
    public Employee deserialize(String topic, byte[] data) {

        try {
            if (data == null){
                System.out.println("Null recieved at deserialize");
                return null;
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int id = buffer.getInt();

            int sizeOfEmployeeName = buffer.getInt();
            byte[] employeeNameBytes = new byte[sizeOfEmployeeName];
            buffer.get(employeeNameBytes);
            String deserializedName = new String(employeeNameBytes, encoding);

            int sizeOfEmployeeJoinDate = buffer.getInt();
            byte[] employeeJoinDateBytes = new byte[sizeOfEmployeeJoinDate];
            buffer.get(employeeJoinDateBytes);
            String dateString = new String(employeeJoinDateBytes,encoding);

            DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

            return new Employee(id,deserializedName,dateFormat.parse(dateString));

        } catch (Exception e) {
            throw new SerializationException("Error when deserialize byte[] to Employee");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
