package com.demo.avro;

import com.demo.avro.gen.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class UserTest {

    private static final String SCHEMA_FILE = "src/main/resources/avro/user.avsc";
    private static final String DATA_FILE1 = "users1.avro";
    private static final String DATA_FILE2 = "users2.avro";

    @Test
    public void userTestWithGeneratedCode() throws IOException {
        //====不同的方式构造 user1, user2 和 user3 =====
        //默认构造器
        User user1 = new User();
        user1.setName("Xiaolong");
        user1.setFavoriteNumber(66);
        //favoriteColor不设置，因为它的类型是["string","null"]，可以为空

        //带参构造器
        User user2 = new User("Ben", 7, "red");

        //Builder模式构造
        User user3 = User.newBuilder()
                .setName("Doris")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        //====序列化 user1, user2 和 user3 到磁盘=====
        //DatumWriter将Java对象转换成in-memory的序列化格式，
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        //DataFileWriter把序列化的对象和它的schema写入指定的文件
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        File file = new File(DATA_FILE1);
        dataFileWriter.create(user1.getSchema(), file);
        //将user对象依次写入文件
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        //写完时，关闭文件
        dataFileWriter.close();

        //====反序列化 user1, user2 和 user3 =====
        // Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    @Test
    public void testUserWithoutGeneratedCode() throws IOException {
        //创建一个Schema对象，用于直接解析avro schema文件(*.avsc)
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));
        //====使用通用的类型：GenericRecord来构造合前面一样的 user1, user2 和 user3 =====
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Xiaolong");
        user1.put("favorite_number", 66);
        //favoriteColor不设置，因为它的类型是["string","null"]，可以为空

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "Doris");
        user3.put("favorite_number", null);
        user3.put("favorite_color", "blue");

        //====序列化 user1, user2 和 user3 到磁盘=====
        File file = new File(DATA_FILE2);
        //用schema来初始化writer
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        //初始化DataFileWriter，表示要向file写schema这种数据
        dataFileWriter.create(schema, file);

        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        //写完后关闭文件
        dataFileWriter.close();

        //====反序列化 user1, user2 和 user3 =====
        //用schema来初始化reader
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
