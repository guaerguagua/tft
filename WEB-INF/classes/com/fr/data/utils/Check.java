package com.fr.data.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Check {

    public final static int BUSS_NO_ID   =1;
    public final static int TRANS_CD_ID  =2;
    public final static int PHONE_NO_ID  =3;
    public final static int ACCT_NO_ID   =4;
    public final static int USER_ID_ID   =5;
    public final static int INS_MCHNT_ID   =6;
    public final static int INS_ACCT_NO_ID    =7;
    public final static int SETTLE_DT_ID    =8;

    private final static int LEN_BUSS_NO =20;
    private final static int LEN_TRANS_CD =4;
    private final static int LEN_PHONE_NO =11;
    private final static int LEN_ACCT_NO =19;
    private final static int LEN_USER_ID =16;
    private final static int LEN_INS_MCHNT =15;
    private final static int LEN_INS_ACCT_NO =6;
    private final static int LEN_SETTLE_DT =8;


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
    private boolean checkSettleDt(String settle){
        return checkNumStr(settle.replaceAll("-",""),LEN_SETTLE_DT);
    }
    private boolean checkInput(int param,String value){
        boolean res=false;
        switch (param){
            case BUSS_NO_ID:
                res=checkNumStr(value,LEN_BUSS_NO);
                break;
            case TRANS_CD_ID:
                res=checkLists(value,LEN_TRANS_CD);
                break;
            case ACCT_NO_ID:
                res=checkNumStr(value,LEN_ACCT_NO);
                break;
            case USER_ID_ID:
                res=checkNumStr(value,LEN_USER_ID);
                break;
            case PHONE_NO_ID:
                res=checkNumStr(value,LEN_PHONE_NO);
                break;
            case INS_MCHNT_ID:
                res=checkLists(value,LEN_INS_MCHNT);
                break;
            case INS_ACCT_NO_ID:
                res=checkLists(value,LEN_INS_ACCT_NO);
                break;
            case SETTLE_DT_ID:
                res=checkSettleDt(value);
                break;
            default:
                res=false;
        }
        return res;
    }


}
