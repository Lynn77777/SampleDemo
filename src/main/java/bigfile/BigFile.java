package bigfile;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 参考
 * https://zhuanlan.zhihu.com/p/373367872
 * 1 通过先写小文件再合并成大文件的方式快速写入一亿的数据
 * 2 将大文件切割成20个小文件并排序 -- //todo
 * 3 将排序后的小文件再按照顺序合并成大文件，使大文件也有序 //todo
 */
public class BigFile {

    public static void main(String[] args) throws InterruptedException {
//        String path = "";
//        new BigFile().deleteFile("D:\\bigFile");
//        new BigFile().writeRandomNum();
        new BigFile().splitToSubFile("D:\\bigFile\\merge.txt");
//        System.exit(0);
    }


    /**
     * 将大文件中的随机数顺通过大文件分成小文件，小文件排序后，再逐个排序合并
     * @param path
     */
    public void splitToSubFile(String path) {

        List<String> splitFileNamelist = new ArrayList<>();
        splitToFiles(path, splitFileNamelist);
        sort(splitFileNamelist);
    }

    private void splitToFiles(String path, List<String> splitFileNamelist) {
        int segment = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(segment);
        File originFile = new File(path);
        int splitCount = (int)Math.ceil(originFile.length() / segment);
        for (int i = 0; i < 20; i++) {
            String partFileName = "D:\\bigFile\\split\\merge" + "." + i + ".part";
            splitFileNamelist.add(partFileName);
            executorService.execute(new SplitFileRunner(
                    splitCount
                    ,partFileName
                    ,originFile
                    ,i * splitCount
            ));
        }
    }

    public void sort(List<String> fileNameList) {

    }

    public void deleteFileOnFolderLevel(String pathName) {
        System.out.println("delete file begin");
        File file = new File(pathName);
        File[] files = file.listFiles();
        if (files != null) {
            for (File subFile : files) {
                if (subFile.isFile()) {
                    boolean delete = subFile.delete();
                    System.out.println("file "+subFile.getName()+" delete "+(delete?" success":" fail"));
                }
            }
        }
        System.out.println("delete file end");
    }
    public void deleteFileOnFileLevel(String pathName) {
        System.out.println("delete file begin");
        File file = new File(pathName);
        if (file.isFile()) {
            boolean delete = file.delete();
            System.out.println("file "+file.getName()+" delete "+(delete?" success":" fail"));
        }
        System.out.println("delete file end");
    }

    //通过多线程写多个小文件，再合并成一个大文件的方式，将一亿随机数写入一个大文件
    public void writeRandomNum() throws InterruptedException {
        System.out.println("writeRandomNum start");
        long writeRandomNumStartTime = System.currentTimeMillis();
        int total = 100000000;
        int segment = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(segment);
        AtomicInteger incr = new AtomicInteger(0);
        CountDownLatch downLatch = new CountDownLatch(segment);
        long startTimestamp = System.currentTimeMillis();
        List<File> fileList = new ArrayList<>();
        for (int i = 0; i < segment; i++) {
            executorService.execute(()->{
                RandomAccessFile randomAccessFile;
                FileChannel fileChannel = null;

                String fileName = "D:\\bigFile\\tmp_"+incr.getAndIncrement()+".txt";
                synchronized (fileList) {
                    //fileList本身不是线程安全的，在多线程里面会有并发问题
                    fileList.add(new File(fileName));
                }
                try {
                    randomAccessFile = new RandomAccessFile(fileName,"rw");
                    fileChannel = randomAccessFile.getChannel();
                    int offset = 0;
                    for (int j =0;j<total/segment/10000;j++) {
                        StringBuilder stringBuilder = new StringBuilder();
                        for (int i1 = 0; i1 < 10000; i1++) {
                            stringBuilder.append(new Random().nextInt() * total + "\n");
                        }
                        byte[] bytes = stringBuilder.toString().getBytes();
                        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, bytes.length);
                        mappedByteBuffer.put(bytes);
                        offset +=bytes.length;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    downLatch.countDown();
                    try {
                        fileChannel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        downLatch.await();
        System.out.println("write small file finish,cost="+(System.currentTimeMillis() - startTimestamp)/1000 + "s");

        long mergeStartTime = System.currentTimeMillis();
        merge(fileList,"D:\\bigFile\\merge.txt");
        System.out.println("file merge finish,cost="+(System.currentTimeMillis() - mergeStartTime)/1000 +"s");
//        long nums = getNums(new File("D:\\bigFile\\merge.txt"));
//        System.out.println("merge file nums="+nums);
        System.out.println("writeRandomNum end,cost="+(System.currentTimeMillis() - writeRandomNumStartTime)/1000 +"s");
    }

    private void merge(List<File> fileList, String mergePathFile) {
        System.out.println("merge start");
        File t = new File(mergePathFile);
        FileInputStream fileInputStream = null;
        FileChannel inChannel = null;

        FileOutputStream fileOutputStream = null;
        FileChannel outChannel = null;


        try {
            fileOutputStream = new FileOutputStream(t,true);
            outChannel = fileOutputStream.getChannel();

            long startIndex = 0;
            long totalNums = 0;
            for (File file : fileList) {
//                long subFileNums = getNums(file);
//                totalNums += subFileNums;
                System.out.println("merge "+file.getName());
                fileInputStream = new FileInputStream(file);
                inChannel = fileInputStream.getChannel();
                outChannel.transferFrom(inChannel,startIndex,file.length());
                startIndex +=file.length();
                System.out.println("merge "+file.getName()+" finish,current total length="+totalNums);
                fileInputStream.close();
                inChannel.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            try {
                fileOutputStream.close();
                outChannel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private long getNums(File file) {
        long lineNum = 0;
        try {
            FileReader fileReader = new FileReader(file);
            int i = 0;
            while((i = fileReader.read())!=-1) {
                Character c = (char)i;
                //将读出的字符转换为字符串
                String temp = c.toString();
                //判断字符串中有没有换行
                if(temp.contains("\n")) {
                    lineNum++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return lineNum;
    }


}
