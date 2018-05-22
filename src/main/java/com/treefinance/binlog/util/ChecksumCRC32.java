package com.treefinance.binlog.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * @author personalc
 */
public class ChecksumCRC32 {
    private static void doChecksum(String fileName) {
        try {
            CheckedInputStream cis = null;
            long fileSize = 0;
            try {
                // Computer CRC32 checksum
                cis = new CheckedInputStream(
                        new FileInputStream(fileName), new CRC32());
                fileSize = new File(fileName).length();
            } catch (FileNotFoundException e) {
                System.err.println("File not found.");
                System.exit(1);
            }
            byte[] buf = new byte[128];
            while (cis.read(buf) >= 0) {
            }
            long checksum = cis.getChecksum().getValue();
            System.out.println(checksum + "" + fileSize + "" + fileName);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Sole entry point to the class and application.
     *
     * @param args Array of String arguments.
     */
    public static void main(String[] args) {
        /*if (args.length != 1) {
            System.err.println("Usage: java ChecksumCRC32 filename");
        } else {
            doChecksum(args[0]);
        }*/
        String fileName = "copyFromLocal";
        doChecksum(fileName);
    }
}
