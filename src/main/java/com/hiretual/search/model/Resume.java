package com.hiretual.search.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class Resume {
    private static final Logger logger = LoggerFactory.getLogger(Resume.class);

    private String uid;
    // use for ranking
    private int availability;
    // always display the candidates whose personal email are stored first
    private boolean hasPersonalEmail;
    private String degree;
    private String yoe;
    // count in months
    private int yearsCurrentCompany;
    // count in months
    private int yearsCurrentRole;
    private boolean divWoman;
    private boolean divBlack;
    private boolean divHispanic;
    private boolean divVeteran;
    private boolean divNative;
    private boolean divAsian;
    private boolean needSponsorship;
    private String companyCurrent;
    private String companyIdCurrent;
    private Set<String> companiesPast;
    private Set<String> companyIdsPast;
    private Set<String> industries;
    private String locRaw;
    private String locContinent;
    private String locNation;
    private String locState;
    private String locCity;
    private float locLat;
    private float locLon;
    // user certifications, company name, position summary, position title,
    // education degree, education description, education majors, school name,
    // personal highlight, expertise and languages and skills for info_tech,
    // languages, normalized skills, title and issuer and description of patents,
    // title and description of projects, organization and description of publications
    private String compoundInfo;

    public Resume(String uid,
                  int availability,
                  boolean hasPersonalEmail,
                  String degree,
                  String yoe,
                  int yearsCurrentCompany,
                  int yearsCurrentRole,
                  boolean divWoman,
                  boolean divBlack,
                  boolean divHispanic,
                  boolean divVeteran,
                  boolean divNative,
                  boolean divAsian,
                  boolean needSponsorship,
                  String companyCurrent,
                  String companyIdCurrent,
                  Set<String> companiesPast,
                  Set<String> companyIdsPast,
                  Set<String> industries,
                  String locRaw,
                  String locContinent,
                  String locNation,
                  String locState,
                  String locCity,
                  float locLat,
                  float locLon,
                  String compoundInfo) {
        this.uid = uid;
        this.availability = availability;
        this.hasPersonalEmail = hasPersonalEmail;
        this.degree = degree;
        this.yoe = yoe;
        this.yearsCurrentCompany = yearsCurrentCompany;
        this.yearsCurrentRole = yearsCurrentRole;
        this.divWoman = divWoman;
        this.divBlack = divBlack;
        this.divHispanic = divHispanic;
        this.divVeteran = divVeteran;
        this.divNative = divNative;
        this.divAsian = divAsian;
        this.needSponsorship = needSponsorship;
        this.companyCurrent = companyCurrent;
        this.companyIdCurrent = companyIdCurrent;
        this.companiesPast = companiesPast;
        this.companyIdsPast = companyIdsPast;
        this.industries = industries;
        this.locRaw = locRaw;
        this.locContinent = locContinent;
        this.locNation = locNation;
        this.locState = locState;
        this.locCity = locCity;
        this.locLat = locLat;
        this.locLon = locLon;
        this.compoundInfo = compoundInfo;
    }

    public Resume(JsonNode jsonNode) {
        try {
            if (jsonNode.has("basic") && jsonNode.has("analytics")) {
                JsonNode basic = jsonNode.get("basic");
                if (basic.has("user_id")) {
                    this.uid = basic.get("user_id").asText();
                }
                String highlight = "";
                if (basic.has("highlight")) {
                    highlight = basic.get("highlight").isNull() ? "" : basic.get("highlight").asText();
                }
                this.hasPersonalEmail = true; //TODO: no data supported

                JsonNode analytics = jsonNode.get("analytics");
                if (analytics.has("availability")) {
                    this.availability = analytics.get("availability").isNull() ? 0 : analytics.get("availability").asInt();
                }
                StringBuilder educationInfo = new StringBuilder();
                if (analytics.has("education")) {
                    int i = 0;
                    for (JsonNode education : analytics.get("education")) {
                        if (i == 0) {
                            this.degree = education.get("education_degree_level").asText();
                        }

                        // //TODO: dedup work may be needed
                        educationInfo.append(education.get("education_school").asText()).append(',')
                                     .append(education.get("education_major").asText()).append(',')
                                     .append(education.get("education_degree").asText()).append(',')
                                     .append(education.get("education_degree_level").asText()).append(',')
                                     .append(education.get("education_description").asText()).append(',');
                        for (JsonNode normalizedMajor : education.get("normed_education_major")) {
                            educationInfo.append(normalizedMajor.asText()).append(',');
                        }
                        i++;
                    }
                }
                if (analytics.has("experience")) {
                    this.yoe = analytics.get("experience").isNull() ? null : analytics.get("experience").asText();
                }
                StringBuilder positionInfo = new StringBuilder();
                if (analytics.has("position")) {
                    this.companiesPast = new HashSet<>();
                    this.companyIdsPast = new HashSet<>();
                    for (JsonNode position : analytics.get("position")) {
                        if (position.get("position_iscurrent").asBoolean()) {
                            this.companyCurrent = position.get("position_company_name").asText();
                            this.companyIdCurrent = position.get("company_id").asText();
                            this.yearsCurrentRole = DateUtils.getMonthsFromDate(position.get("position_start_date").asText());
                        } else {
                            this.companiesPast.add(position.get("position_company_name").asText());
                            this.companyIdsPast.add(position.get("company_id").asText());
                        }

                        // //TODO: dedup work may be needed
                        positionInfo.append(position.get("position_title").asText()).append(',')
                                .append(position.get("position_company_name").asText()).append(',')
                                .append(position.get("position_summary").asText()).append(',');
                        for (JsonNode normalizedTitle : position.get("normed_position_title")) {
                            positionInfo.append(normalizedTitle.asText()).append(',');
                        }
                    }
                }

            }
        } catch (Exception e) {
            logger.error("fail to convert json to common parameter, input: " + jsonNode.toString(), e);
        }
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getAvailability() {
        return availability;
    }

    public void setAvailability(int availability) {
        this.availability = availability;
    }

    public boolean isHasPersonalEmail() {
        return hasPersonalEmail;
    }

    public void setHasPersonalEmail(boolean hasPersonalEmail) {
        this.hasPersonalEmail = hasPersonalEmail;
    }

    public String getDegree() {
        return degree;
    }

    public void setDegree(String degree) {
        this.degree = degree;
    }

    public String getYoe() {
        return yoe;
    }

    public void setYoe(String yoe) {
        this.yoe = yoe;
    }

    public int getYearsCurrentCompany() {
        return yearsCurrentCompany;
    }

    public void setYearsCurrentCompany(int yearsCurrentCompany) {
        this.yearsCurrentCompany = yearsCurrentCompany;
    }

    public int getYearsCurrentRole() {
        return yearsCurrentRole;
    }

    public void setYearsCurrentRole(int yearsCurrentRole) {
        this.yearsCurrentRole = yearsCurrentRole;
    }

    public boolean isDivWoman() {
        return divWoman;
    }

    public void setDivWoman(boolean divWoman) {
        this.divWoman = divWoman;
    }

    public boolean isDivBlack() {
        return divBlack;
    }

    public void setDivBlack(boolean divBlack) {
        this.divBlack = divBlack;
    }

    public boolean isDivHispanic() {
        return divHispanic;
    }

    public void setDivHispanic(boolean divHispanic) {
        this.divHispanic = divHispanic;
    }

    public boolean isDivVeteran() {
        return divVeteran;
    }

    public void setDivVeteran(boolean divVeteran) {
        this.divVeteran = divVeteran;
    }

    public boolean isDivNative() {
        return divNative;
    }

    public void setDivNative(boolean divNative) {
        this.divNative = divNative;
    }

    public boolean isDivAsian() {
        return divAsian;
    }

    public void setDivAsian(boolean divAsian) {
        this.divAsian = divAsian;
    }

    public boolean isNeedSponsorship() {
        return needSponsorship;
    }

    public void setNeedSponsorship(boolean needSponsorship) {
        this.needSponsorship = needSponsorship;
    }

    public String getCompanyCurrent() {
        return companyCurrent;
    }

    public void setCompanyCurrent(String companyCurrent) {
        this.companyCurrent = companyCurrent;
    }

    public String getCompanyIdCurrent() {
        return companyIdCurrent;
    }

    public void setCompanyIdCurrent(String companyIdCurrent) {
        this.companyIdCurrent = companyIdCurrent;
    }

    public Set<String> getCompaniesPast() {
        return companiesPast;
    }

    public void setCompaniesPast(Set<String> companiesPast) {
        this.companiesPast = companiesPast;
    }

    public Set<String> getCompanyIdsPast() {
        return companyIdsPast;
    }

    public void setCompanyIdsPast(Set<String> companyIdsPast) {
        this.companyIdsPast = companyIdsPast;
    }

    public Set<String> getIndustries() {
        return industries;
    }

    public void setIndustries(Set<String> industries) {
        this.industries = industries;
    }

    public String getLocRaw() {
        return locRaw;
    }

    public void setLocRaw(String locRaw) {
        this.locRaw = locRaw;
    }

    public String getLocContinent() {
        return locContinent;
    }

    public void setLocContinent(String locContinent) {
        this.locContinent = locContinent;
    }

    public String getLocNation() {
        return locNation;
    }

    public void setLocNation(String locNation) {
        this.locNation = locNation;
    }

    public String getLocState() {
        return locState;
    }

    public void setLocState(String locState) {
        this.locState = locState;
    }

    public String getLocCity() {
        return locCity;
    }

    public void setLocCity(String locCity) {
        this.locCity = locCity;
    }

    public float getLocLat() {
        return locLat;
    }

    public void setLocLat(float locLat) {
        this.locLat = locLat;
    }

    public float getLocLon() {
        return locLon;
    }

    public void setLocLon(float locLon) {
        this.locLon = locLon;
    }

    public String getCompoundInfo() {
        return compoundInfo;
    }

    public void setCompoundInfo(String compoundInfo) {
        this.compoundInfo = compoundInfo;
    }

    @Override
    public String toString() {
        return "uid:" + uid + "|availability:" + availability + "|hasPersonalEmail:" + hasPersonalEmail
                + "|degree:" + degree + "|yoe:" + yoe + "|yearsCurrentCompany:" + yearsCurrentCompany
                + "|yearsCurrentRole:" + yearsCurrentRole + "|divWoman:" + divWoman + "|divBlack:" + divBlack
                + "|divHispanic:" + divHispanic + "|divVeteran:" + divVeteran + "|divNative:" + divNative
                + "|divAsian:" + divAsian + "|needSponsorship:" + needSponsorship + "|companyCurrent:" + companyCurrent
                + "|companyIdCurrent:" + companyIdCurrent + "|companiesPast:" + String.join(",", companiesPast)
                + "|companyIdsPast:" + String.join(",", companyIdsPast) + "|industries:" + String.join(",", industries)
                + "|locRaw:" + locRaw + "|locContinent:" + locContinent + "|locNation:" + locNation
                + "|locState:" + locState + "|locCity:" + locCity + "|locLat:" + locLat + "|locLon:" + locLon
                + "|compoundInfo:" + compoundInfo;
    }

}
