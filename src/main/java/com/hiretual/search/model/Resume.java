package com.hiretual.search.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;

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
    private int monthsCurrentCompany;
    // count in months
    private int monthsCurrentRole;
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
    private double locLat;
    private double locLon;
    // user certifications, company name, position summary, position title,
    // education degree, education description, education majors, school name,
    // personal highlight, expertise and languages and skills for info_tech,
    // languages, normalized skills, title and issuer and description of patents,
    // title and description of projects, organization and description of publications
    private String compoundInfo;
    private float[] embedding;

    public Resume(String uid,
                  int availability,
                  boolean hasPersonalEmail,
                  String degree,
                  String yoe,
                  int monthsCurrentCompany,
                  int monthsCurrentRole,
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
                  double locLat,
                  double locLon,
                  String compoundInfo,
                  float[] embedding) {
        this.uid = uid;
        this.availability = availability;
        this.hasPersonalEmail = hasPersonalEmail;
        this.degree = degree;
        this.yoe = yoe;
        this.monthsCurrentCompany = monthsCurrentCompany;
        this.monthsCurrentRole = monthsCurrentRole;
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
        this.embedding = embedding;
    }

    public Resume(JsonNode jsonNode) {
        try {
            String highlight = "";
            StringBuilder skillInfo = new StringBuilder();
            StringBuilder itInfo = new StringBuilder();
            if (jsonNode.has("basic") && jsonNode.has("analytics")) {
                JsonNode basic = jsonNode.get("basic");
                if (basic.has("user_id")) {
                    this.uid = basic.get("user_id").asText();
                }
                if (basic.has("highlight")) {
                    highlight = basic.get("highlight").isNull() ? "" : basic.get("highlight").asText();
                }

                this.hasPersonalEmail = true; //TODO: no data support
                this.needSponsorship = false; //TODO: no data support
                JsonNode analytics = jsonNode.get("analytics");
                if (analytics.has("availability")) {
                    this.availability = analytics.get("availability").isNull() ? 0 : analytics.get("availability").asInt();
                }
                if (analytics.has("gender")) {
                    this.divWoman = "female".equals(analytics.get("gender").asText());
                } else {
                    this.divWoman = false;
                }
                if (analytics.has("race")) {
                    String race = analytics.get("race").asText();
                    if (race.indexOf("hispanic") > -1) {
                        this.divHispanic = true;
                    } else {
                        this.divHispanic = false;
                    }
                    if (race.indexOf("asian") > -1) {
                        this.divAsian = true;
                    } else {
                        this.divAsian = false;
                    }
                    if (race.indexOf("african") > -1 || race.indexOf("black") > -1) {
                        this.divBlack = true;
                    } else {
                        this.divBlack = false;
                    }
                    if (race.indexOf("native") > -1) {
                        this.divNative = true;
                    } else {
                        this.divNative = false;
                    }
                }
                if (analytics.has("veteran")) {
                    this.divVeteran = analytics.get("veteran").asBoolean();
                } else {
                    this.divVeteran = false;
                }
                if (analytics.has("education")) {
                    this.degree = analytics.get("education").asText();
                }
                if (analytics.has("experience")) {
                    this.yoe = analytics.get("experience").isNull() ? null : analytics.get("experience").asText();
                }
                if (analytics.has("skill")) {
                    for (JsonNode skill : analytics.get("skill")) {
                        skillInfo.append(skill.asText()).append(',');
                    }
                }
                if (analytics.has("industry")) {
                    this.industries = new HashSet<>();
                    for (JsonNode industry : analytics.get("industry")) {
                        this.industries.add(industry.asText());
                    }
                }
                if (analytics.has("norm_location")) {
                    JsonNode loc = analytics.get("norm_location");
                    this.locRaw = loc.get("location_fmt").isNull() ? "" : loc.get("location_fmt").asText();
                    this.locContinent = loc.get("continent").isNull() ? "" : loc.get("continent").asText();
                    this.locNation = loc.get("country").isNull() ? "" : loc.get("country").asText();
                    this.locState = loc.get("state").isNull() ? "" : loc.get("state").asText();
                    this.locCity = loc.get("city").isNull() ? "" : loc.get("city").asText();
                    this.locLat = loc.get("latitude").isNull() ? 0 : loc.get("latitude").asDouble();
                    this.locLon = loc.get("longitude").isNull() ? 0 : loc.get("longitude").asDouble();
                }
                if (analytics.has("it_analytics")) {
                    JsonNode it = analytics.get("it_analytics");
                    if (it.has("languages")) {
                        for (JsonNode language : it.get("languages")) {
                            itInfo.append(language.asText()).append(',');
                        }
                    }
                    if (it.has("expertise_scores")) {
                        Iterator<String> keys = it.get("expertise_scores").fieldNames();
                        while (keys.hasNext()) {
                            itInfo.append(keys.next()).append(',');
                        }
                    }
                    if (it.has("details")) {
                        JsonNode details = it.get("details");
                        if (details.has("github")) {
                            JsonNode github = details.get("github");
                            if (github.has("selected_repo")) {
                                for (JsonNode repo : github.get("selected_repo")) {
                                    if (repo.has("expertise")) {
                                        Iterator<String> expertises = repo.get("expertise").fieldNames();
                                        while (expertises.hasNext()) {
                                            itInfo.append(expertises.next()).append(',');
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            StringBuilder educationInfo = new StringBuilder();
            if (jsonNode.has("education")) {
                for (JsonNode education : jsonNode.get("education")) {
                    // //TODO: dedup work may be needed
                    educationInfo.append(education.get("education_school").asText()).append(',')
                            .append(education.get("education_major").asText()).append(',')
                            .append(education.get("education_degree").asText()).append(',')
                            .append(education.get("education_degree_level").asText()).append(',')
                            .append(education.get("education_description").asText()).append(',');
                    for (JsonNode normalizedMajor : education.get("normed_education_major")) {
                        educationInfo.append(normalizedMajor.asText()).append(',');
                    }
                }
            }
            StringBuilder positionInfo = new StringBuilder();
            if (jsonNode.has("position")) {
                this.companiesPast = new HashSet<>();
                this.companyIdsPast = new HashSet<>();
                Map<String, Integer> map = new HashMap<>();
                for (JsonNode position : jsonNode.get("position")) {
                    String companyName = position.get("position_company_name").asText().toLowerCase();
                    String companyId = position.get("company_id").asText();
                    if (position.get("position_iscurrent").asBoolean()) {
                        this.companyCurrent = companyName;
                        this.companyIdCurrent = companyId;
                        this.monthsCurrentRole = DateUtils.getMonthsFromDate(position.get("position_start_date").asText());
                    } else {
                        this.companiesPast.add(companyName);
                        this.companyIdsPast.add(companyId);
                    }

                    if (map.containsKey(companyName)) {
                        map.put(companyName, map.get(companyName)
                                + DateUtils.getMonthsFromDate(position.get("position_start_date").asText(),
                                position.get("position_end_date").asText()));
                    } else {
                        map.put(companyName, DateUtils.getMonthsFromDate(position.get("position_start_date").asText(),
                                position.get("position_end_date").asText()));
                    }

                    // //TODO: dedup work may be needed
                    positionInfo.append(position.get("position_title").asText()).append(',')
                            .append(position.get("position_company_name").asText()).append(',')
                            .append(position.get("position_summary").asText()).append(',');
                    for (JsonNode normalizedTitle : position.get("normed_position_title")) {
                        positionInfo.append(normalizedTitle.asText()).append(',');
                    }
                }
                if (!StringUtils.isEmpty(this.companyCurrent)) {
                    this.monthsCurrentCompany = map.get(this.companyCurrent);
                }
            }
            StringBuilder certInfo = new StringBuilder();
            if (jsonNode.has("certification")) {
                for (JsonNode certification : jsonNode.get("certification")) {
                    certInfo.append(certification.get("certification_name").asText()).append(',')
                            .append(certification.get("certification_authority").asText()).append(',');
                }
            }
            StringBuilder languageInfo = new StringBuilder();
            if (jsonNode.has("language")) {
                for (JsonNode language : jsonNode.get("language")) {
                    languageInfo.append(language.get("language_name").asText()).append(',');
                }
            }
            StringBuilder patentInfo = new StringBuilder();
            if (jsonNode.has("patent")) {
                for (JsonNode patent : jsonNode.get("patent")) {
                    patentInfo.append(patent.get("patent_title").asText()).append(',')
                              .append(patent.get("patent_issuer").asText()).append(',')
                              .append(patent.get("patent_description").asText()).append(',');
                }
            }
            StringBuilder projectInfo = new StringBuilder();
            if (jsonNode.has("project")) {
                for (JsonNode project : jsonNode.get("project")) {
                    projectInfo.append(project.get("project_title").asText()).append(',')
                               .append(project.get("project_summary").asText()).append(',');
                }
            }
            StringBuilder publicationInfo = new StringBuilder();
            if (jsonNode.has("publication")) {
                for (JsonNode publication : jsonNode.get("publication")) {
                    publicationInfo.append(publication.get("publication_title").asText()).append(',')
                                   .append(publication.get("publication_organization").asText()).append(',')
                                   .append(publication.get("publication_description").asText()).append(',');
                }
            }
            this.compoundInfo = highlight + positionInfo + educationInfo + certInfo + itInfo + languageInfo
                        + skillInfo + patentInfo + projectInfo + publicationInfo;
            if (jsonNode.has("embedding")) {
                this.embedding = new float[IndexBuildService.embeddingDimension];
                Iterator<JsonNode> arrayIterator = jsonNode.get("embedding").iterator();
                int i = 0;
                while(arrayIterator.hasNext() && i < this.embedding.length) {
                    this.embedding[i] = (float) arrayIterator.next().asDouble();
                    i++;
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

    public int getMonthsCurrentCompany() {
        return monthsCurrentCompany;
    }

    public void setMonthsCurrentCompany(int monthsCurrentCompany) {
        this.monthsCurrentCompany = monthsCurrentCompany;
    }

    public int getMonthsCurrentRole() {
        return monthsCurrentRole;
    }

    public void setMonthsCurrentRole(int monthsCurrentRole) {
        this.monthsCurrentRole = monthsCurrentRole;
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

    public double getLocLat() {
        return locLat;
    }

    public void setLocLat(double locLat) {
        this.locLat = locLat;
    }

    public double getLocLon() {
        return locLon;
    }

    public void setLocLon(double locLon) {
        this.locLon = locLon;
    }

    public String getCompoundInfo() {
        return compoundInfo;
    }

    public void setCompoundInfo(String compoundInfo) {
        this.compoundInfo = compoundInfo;
    }

    public float[] getEmbedding() {
        return embedding;
    }

    public void setEmbedding(float[] embedding) {
        this.embedding = embedding;
    }

    @Override
    public String toString() {
        return "uid:" + uid + "|availability:" + availability + "|hasPersonalEmail:" + hasPersonalEmail
                + "|degree:" + degree + "|yoe:" + yoe + "|monthsCurrentCompany:" + monthsCurrentCompany
                + "|monthsCurrentRole:" + monthsCurrentRole + "|divWoman:" + divWoman + "|divBlack:" + divBlack
                + "|divHispanic:" + divHispanic + "|divVeteran:" + divVeteran + "|divNative:" + divNative
                + "|divAsian:" + divAsian + "|needSponsorship:" + needSponsorship + "|companyCurrent:" + companyCurrent
                + "|companyIdCurrent:" + companyIdCurrent + "|companiesPast:" + String.join(",", companiesPast)
                + "|companyIdsPast:" + String.join(",", companyIdsPast) + "|industries:" + String.join(",", industries)
                + "|locRaw:" + locRaw + "|locContinent:" + locContinent + "|locNation:" + locNation
                + "|locState:" + locState + "|locCity:" + locCity + "|locLat:" + locLat + "|locLon:" + locLon
                + "|compoundInfo:" + compoundInfo;
    }

}
