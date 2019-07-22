package com.example.demo;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateDirectory {
    //private static final String uri = "hdfs://localhost:9000/user/mustapha/";
    //private static final String directory = uri + "directory1" + File.separator + "directory2";

    private static final Configuration config = new Configuration();


    public static void createDirectory(String uri, String directory) throws IOException {

        String uri_ = "hdfs://localhost:9000/user/mustapha/"+ uri;
        String directory_ = "hdfs://localhost:9000/user/mustapha/"+ directory;;

        /* Get FileSystem object for given uri */
        FileSystem fs = FileSystem.get(URI.create(uri_), config);
        boolean isCreated = fs.mkdirs(new Path(directory_));

        if (isCreated) {
            System.out.println("Directory created");
        } else {
            System.out.println("Directory creation failed");
        }
    }

    /*public static void main(String args[]) throws IOException {
        createDirectory("","bb");
    }*/

}