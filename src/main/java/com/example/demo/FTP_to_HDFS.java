package com.example.demo;

import java.io.File;
import java.io.IOException;

import java.io.OutputStream;

import java.net.URI;

import java.net.URISyntaxException;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.ftp.FTPFileSystem;

import org.apache.hadoop.io.IOUtils;


class FTPtoHDFS

{
    public static void StoreFile(String parentDir, String currentFileName) throws IOException, URISyntaxException {

        String src="ftp://echanges.dila.gouv.fr/"+parentDir+ File.separator+currentFileName;

        Configuration conf = new Configuration();

        conf.addResource(new Path("/home/mustapha/Bureau/PFE/hadoop-2.8.5/etc/hadoop/core-site.xml"));

        conf.addResource(new Path("/home/mustapha/Bureau/PFE/hadoop-2.8.5/etc/hadoop/hdfs-site.xml"));


        FTPFileSystem ftpfs = new FTPFileSystem();

        ftpfs.setConf(conf);

        ftpfs.initialize(new URI("ftp://anonymous:anonymous@echanges.dila.gouv.fr"), conf);
        //ftpfs.initialize(new URI("ftp://username:password@host"), conf);

        FSDataInputStream fsdin = ftpfs.open(new Path(src), 1000);

        FileSystem fileSystem=FileSystem.get(conf);

        //OutputStream outputStream=fileSystem.create(new Path(args[0]));
        OutputStream outputStream=fileSystem.create(new Path("/user/mustapha/"+parentDir+File.separator+currentFileName));


        IOUtils.copyBytes(fsdin, outputStream, conf, true);

    }

    public static void main(String[] args) throws IOException, URISyntaxException
    {
        StoreFile("SARDE/DTD SARDE FREEMIUM", "SCHEMA_SARDE_1.0.zip");
    }
    
}
