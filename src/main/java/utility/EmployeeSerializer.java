package utility;

import model.Employee;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;


public class EmployeeSerializer implements Serializer<Employee> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Employee employee) {

        int sizeOfEmployeeName;
        int sizeOfEmployeeJoinDate;
        byte[] serializedEmployeeName;
        byte[] serializedEmployeeJoinDate;

        try {
            if (employee == null)
                return null;
            serializedEmployeeName = employee.getEmployeeName().getBytes(encoding);
            sizeOfEmployeeName = serializedEmployeeName.length;

            serializedEmployeeJoinDate = employee.getEmployeeJoinDate().toString().getBytes(encoding);
            sizeOfEmployeeJoinDate = serializedEmployeeJoinDate.length;

            ByteBuffer buffer = ByteBuffer.allocate(4+4+sizeOfEmployeeName+4+sizeOfEmployeeJoinDate);

            buffer.putInt(employee.getEmployeeId());
            buffer.putInt(sizeOfEmployeeName);
            buffer.put(serializedEmployeeName);
            buffer.putInt(sizeOfEmployeeJoinDate);
            buffer.put(serializedEmployeeJoinDate);


            return buffer.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Employee to byte[]");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}