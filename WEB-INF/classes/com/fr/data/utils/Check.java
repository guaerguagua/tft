package com.fr.data.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Check {

    public final static int BUSSNO   =1;
    public final static int TRANSCD  =2;
    public final static int PHONENO  =3;
    public final static int ACCTNO   =4;
    public final static int USERID   =5;
    public final static int INSMCHNTID    =6;


    private boolean res=true;

    private void  setRes(boolean b){
        this.res= b;
    }
    public boolean getRes(){
        return this.res;
    }
    public Check checkValue(int param,String value){
        if(this.res){
            this.res=checkInput(param,value);
        }

        return  this;
    }

    public static boolean checkNumStr(String str,int len){
        if(str.length()==0){
            return true;
        }
        String regx = String.format("^\\d{%d}$",len);
        Pattern p = Pattern.compile(regx);
        Matcher matcher=p.matcher(str);
        return matcher.matches();
    }
    public static boolean checkLists(String lists,int len){

        boolean res=false;
        if(lists.equals("")){
            return true;
        }
        String [] listCds=lists.split(",");
        System.out.println(listCds.length);
        if(listCds.length>0){
            for(int i=0;i<listCds.length;i++){
                res=checkNumStr(listCds[i],len);
                if(!res) return res;
                System.out.println(listCds[i]+"  "+ res);
            }

        }
        return res;
    }
    private boolean checkInput(int param,String value){
        boolean res=false;
        switch (param){
            case BUSSNO:
                res=checkNumStr(value,20);
                break;
            case TRANSCD:
                res=checkLists(value,4);
                break;
            case ACCTNO:
                res=checkNumStr(value,19);
                break;
            case USERID:
                res=checkNumStr(value,16);
                break;
            case PHONENO:
                res=checkNumStr(value,11);
                break;
            case INSMCHNTID:
                res=checkLists(value,15);
                break;

            default:
                res=false;
        }
        return res;
    }


}
