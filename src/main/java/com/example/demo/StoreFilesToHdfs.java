package com.example.demo;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;

import java.net.URISyntaxException;

public class StoreFilesToHdfs {
    public static void main(String args[]) throws URISyntaxException, IOException {

        String src="ftp://echanges.dila.gouv.fr/";


        Configuration conf = new Configuration();

        conf.addResource(new Path("/home/mustapha/Bureau/PFE/hadoop-2.8.5/etc/hadoop/core-site.xml"));

        conf.addResource(new Path("/home/mustapha/Bureau/PFE/hadoop-2.8.5/etc/hadoop/hdfs-site.xml"));

        FTPFileSystem ftpfs = new FTPFileSystem();

        ftpfs.setConf(conf);

        ftpfs.initialize(new URI("ftp://anonymous:anonymous@echanges.dila.gouv.fr"), conf);

        // get an ftpClient object
        FTPClient ftpClient = new FTPClient();

        try {
            // pass directory path on server to connect
            ftpClient.connect("echanges.dila.gouv.fr");

            // pass username and password, returned true if authentication is
            // successful
            boolean login = ftpClient.login("anonymous", "anonymous");

            if (login) {
                System.out.println("Connection established...");

                // get all files from server and store them in an array of
                // FTPFiles
                FTPFile[] files = ftpClient.listFiles();
                //FTPFile[] files = ftpClient.listDirectories();

                for (FTPFile file : files) {
                    if (file.getType() == FTPFile.FILE_TYPE) {
                          FSDataInputStream fsdin = ftpfs.open(new Path(src+file.getName()), 1000);

                    FileSystem fileSystem=FileSystem.get(conf);

                    OutputStream outputStream=fileSystem.create(new Path(file.getName()));


                    IOUtils.copyBytes(fsdin, outputStream, conf, true);
                    }


                }

                // logout the user, returned true if logout successfully
                boolean logout = ftpClient.logout();
                if (logout) {
                    System.out.println("Connection close...");
                }
            } else {
                System.out.println("Connection fail...");
            }

        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
