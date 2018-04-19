package com.fr.data.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Check {

    private boolean res=true;

    private void  setRes(boolean b){
        this.res= b;
    }
    public boolean getRes(){
        return this.res;
    }
    public Check checkValue(String param,String value){
        if(this.res){
            this.res=checkInput(param,value);
        }

        return  this;
    }

    public static boolean checkNumStr(String str,int len){
        String regx = String.format("^\\d{%d}$",len);
        Pattern p = Pattern.compile(regx);
        Matcher matcher=p.matcher(str);
        return matcher.matches();
    }
    public static boolean checkTransCd(String transCd){

        boolean res=false;
        if(transCd.equals("")){
            return true;
        }
        String [] list_transCds=transCd.split(",");
        System.out.println(list_transCds.length);
        if(list_transCds.length>0){
            for(int i=0;i<list_transCds.length;i++){
                res=checkNumStr(list_transCds[i],4);
                if(!res) return res;
                System.out.println(list_transCds[i]+"  "+ res);
            }

        }
        return res;
    }
    private boolean checkInput(String param,String value){
        boolean res=false;
        switch (param){
            case "bussNo":
                res=checkNumStr(value,20);
                break;
            case "transCd":
                res=checkTransCd(value);
                break;
            case "acctNo":
                res=checkNumStr(value,19);
                break;
            case "userId":
                res=checkNumStr(value,16);
                break;
            case "phoneNo":
                res=checkNumStr(value,11);
                break;

            default:
                res=false;
        }
        return res;
    }


}
