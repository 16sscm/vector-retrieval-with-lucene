package com.hiretual.search.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    private static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private static SimpleDateFormat msFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static SimpleDateFormat positionFormatter = new SimpleDateFormat("MMM yyyy");

    public static String formatCurrentTime() {
        return msFormatter.format(new Date());
    }

    public static int getMonthsFromDate(String fromDate, String endDate) {
        if (fromDate == null || endDate == null) {
            return 0;
        }
        Date fd;
        try {
            fd = positionFormatter.parse(fromDate);
        } catch(Exception e) {
            return 0;
        }

        Date ed;
        try {
            ed = positionFormatter.parse(endDate);
        } catch(Exception e) {
            ed = new Date();
        }
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(ed);
        c2.setTime(fd);
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int month2 = c2.get(Calendar.MONTH);

        int yearInterval = year1 - year2;
        if (month1 < month2) {
            yearInterval--;
        }

        int monthInterval = (month1 + 12) - month2;
        monthInterval %= 12;
        int monthsDiff = yearInterval * 12 + monthInterval + 1;
        return monthsDiff;
    }

    public static int getMonthsFromDate(String date) {
        if (date == null || date.length() == 0) {
            return 0;
        }

        return getMonthsFromDate(date, positionFormatter.format(new Date()));
    }

    public static int getTimestamp(Date date) {
        if (date != null) {
            return (int) (date.getTime() / 1000);
        }

        return 0;
    }

    public static String getTimestampStr(Date date) {
        return getTimestamp(date) + "";
    }

    public static String getBirthdayStr(Date date) {
        if (date.getTime() < System.currentTimeMillis()) {
            return dateFormatter.format(date);
        } else {
            return "";
        }
    }

    public static String getTodayDateStr() {
        return dateFormatter.format(new Date());
    }

    public static String getDateStr(Date date) {
        if (date == null) {
            return "";
        }
        return dateFormatter.format(date);
    }

    public static int getYear() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.YEAR);
    }

    public static int getMonth() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.MONTH) + 1;
    }

    public static int getDay() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.DAY_OF_MONTH);
    }
}