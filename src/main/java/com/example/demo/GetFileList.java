package com.example.demo;

import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class GetFileList {
    public static void main(String args[]) {

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
                    /*if (file.getType() == FTPFile.FILE_TYPE) {
                        System.out.println("File Name: "
                                + file.getName()
                                + " File Size: "
                                + FileUtils.byteCountToDisplaySize(file
                                .getSize()));
                    }*/


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