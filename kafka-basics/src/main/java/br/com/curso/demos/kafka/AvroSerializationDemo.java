package br.com.curso.demos.kafka;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.DatumWriter;
public class AvroSerializationDemo {
    public static void main(String[] args) throws IOException {
        //Instantiating the Schema.Parser class.
        Schema schema = new Schema.Parser().parse(new File("/Users/robsvel/projetos/kafka-beginners-course/kafka-basics/src/main/resources/avro/emp.avsc"));

        //Instantiating the GenericRecord class.
        GenericRecord e1 = new GenericData.Record(schema);

        //Insert data according to schema
        e1.put("name", "ramu");
        e1.put("id", 001);
        e1.put("salary",30000);
        e1.put("age", 25);
        e1.put("address", "chenni");

        GenericRecord e2 = new GenericData.Record(schema);

        e2.put("name", "rahman");
        e2.put("id", 002);
        e2.put("salary", 35000);
        e2.put("age", 30);
        e2.put("address", "Delhi");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, new File("/Users/robsvel/projetos/kafka-beginners-course/kafka-basics/src/main/resources/mydata.txt"));

        dataFileWriter.append(e1);
        dataFileWriter.append(e2);
        dataFileWriter.close();

        System.out.println("data successfully serialized");
    }
}
