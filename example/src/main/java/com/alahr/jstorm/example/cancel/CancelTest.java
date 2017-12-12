package com.alahr.jstorm.example.cancel;

public class CancelTest extends Thread {

    public CancelTest(){
        this("cancel-test");
    }

    public CancelTest(String name){
        super(name);
    }

    public static CancelTest ct = null;


    @Override
    public void run() {
        int i=0;
        System.out.println("run start...");
        while(true){
            System.out.print(i+++"\t");
            if(i%10==0){
                System.out.println("");
            }
            try{
                Thread.sleep(1000);
            }
            catch (InterruptedException e){
                System.out.println("\ninterrupt...");
                break;
            }
        }
        System.out.println("run end...");
    }

    public static void execute(String name, String type){
        if(null == ct){
            ct = new CancelTest(name);
        }
        System.out.println("\n"+ct.getName()+"\t"+name+",\t"+type);
        if("cancel".equals(type)){
            if(ct.isAlive()) ct.interrupt();
        }
        else if("start".equals(type)){
            ct.start();
        }
        else{
            System.out.println("type error");
        }
    }
}
