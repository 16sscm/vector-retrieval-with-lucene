package com.hiretual.search.filterindex;


import java.util.Scanner;

//main class for test index,search api
public class Main {
    public static void main(String[] args) {
        try {

            Scanner s = new Scanner(System.in);
            System.out.println("input directory absolute path first:");
            String directory=s.nextLine();
//        String directory="/Users/gaoyongzhan/data/lucene";
            FilterIndex filterIndex = new FilterIndex(directory,1000000);
            System.out.println("select command:index|ann_bruteforce_index|search");
            boolean flag=true;
            while (flag){
                String command =s.nextLine();
                switch (command) {
                    case "index": {
                        filterIndex.index(Utils.genFakeDocuments(10000));
                        break;
                    }
                    case "search": {
                        filterIndex.search(Utils.genFakeQuery(),1000);
                        break;
                    }
                    case "ann_bruteforce_index":{

                        filterIndex.annBruteforceIndex(128,directory);
                        break;
                    }
                    case "exit" :{
                        flag=false;
                        break;
                    }
                    default: {
                        System.out.println("usage:select command:index|ann_bruteforce_index|search");
                    }
                }
            }
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

