package bigfile;

import java.io.*;

public class SplitFileRunner implements Runnable{

    private int byteSize;

    private String splitFileName;

    private File originFile;

    private int startPos;

    public SplitFileRunner(int byteSize, String splitFileName, File originFile, int startPos) {
        this.byteSize = byteSize;
        this.splitFileName = splitFileName;
        this.originFile = originFile;
        this.startPos = startPos;

    }

    @Override
    public void run() {
        System.out.println("启动线程,fileName:"+ splitFileName);
        try(
                OutputStream outputStream = new FileOutputStream(splitFileName);
                RandomAccessFile randomAccessFile = new RandomAccessFile(originFile,"r")
            ){

            byte[] bytes = new byte[byteSize];
            randomAccessFile.seek(startPos);
            int read = randomAccessFile.read(bytes);
            outputStream.write(bytes,0,read);
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
