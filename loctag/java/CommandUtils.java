package com.tuqu.sunny.utils;

import java.math.BigDecimal;

/**
 * Created by sunny on 17/6/27.
 */
public class CommandUtils {
    /**
     * 移除时间重复的数据
     * @param line
     * @return
     */
    static StringBuilder oldData = new StringBuilder();
    public static String removeRepeatDate(String line){
        String strdate = null;
        String olddate = null;
        if(oldData.length()!=0&&line!=null){
            strdate = line.split(",")[0];
            olddate = oldData.toString().split(",")[0];
            if(olddate.equals(strdate)){
                return null;
            }
            oldData.delete(0,oldData.length());
        }
        oldData.append(line);
        return line;
    }



    /**
     * 移除经纬度重复的数据
     * @param arr
     */
    static StringBuilder oldLngLat = new StringBuilder();
    public static String removeSameData(String line){

        Double lng = null;
        Double lat = null;
        Double olng =null;
        Double olat = null;
        BigDecimal blng = null;
        BigDecimal blat = null;
        BigDecimal bolng =null;
        BigDecimal bolat = null;
        String[] oldfields = null;
        String[] fields = null;
        if(line == null || line.contains("lat")){
            return line;
        }
        if(oldLngLat.length()!=0 ){
            fields = line.split(",");

            oldfields = oldLngLat.toString().split(",");
            if(fields.length<5 || oldfields.length<5){
                return null;
            }
            lat = Double.valueOf(fields[3].trim());
            lng = Double.valueOf(fields[4].trim());
            olat = Double.valueOf(oldfields[3].trim());
            olng = Double.valueOf(oldfields[4].trim());


            blat = BigDecimal.valueOf(lat).setScale(3,BigDecimal.ROUND_DOWN);
            blng = BigDecimal.valueOf(lng).setScale(3,BigDecimal.ROUND_DOWN);
            bolat = BigDecimal.valueOf(olat).setScale(3,BigDecimal.ROUND_DOWN);
            bolng = BigDecimal.valueOf(olng).setScale(3,BigDecimal.ROUND_DOWN);
            if (blat.equals(bolat) && blng.equals(bolng)){
                return null;
            }

            oldLngLat.delete(0,oldLngLat.length());
        }
        oldLngLat.append(line);
        return line;
    }

    /**
     * 把秒数格式化为时或分
     * @param second
     * @return
     */
    public static float secondTransform(float second){

        if(second == 0){
            return second;
        }

        float hour = second / 3600;
        hour = BigDecimal.valueOf(hour).setScale(2,BigDecimal.ROUND_DOWN).floatValue();
        return hour;
    }

}
