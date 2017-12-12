package com.alahr.jstorm.example.cancel;

public class ServiceThread {
    public static void main(String[] args){
        System.out.println("Service "+Thread.currentThread().getName());
        CancelTest.execute("hello", "start");
        try{
            Thread.sleep(25000);
        }
        catch (InterruptedException e){

        }
        CancelTest.execute("hello", "cancel");
    }
}
