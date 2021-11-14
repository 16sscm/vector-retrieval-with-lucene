package com.hiretual.search.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.constants.ResumeField;
import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.utils.DateUtils;
import com.hiretual.search.utils.JsonResumeParseUtils;
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
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.BASIC) && !JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.ANALYTICS)) {
                JsonNode basic = jsonNode.get(ResumeField.BASIC);
                JsonNode analytics = jsonNode.get(ResumeField.ANALYTICS);

                this.uid = JsonResumeParseUtils.getStringFieldFromJsonNode(basic, ResumeField.UID);

                highlight = JsonResumeParseUtils.getStringFieldFromJsonNode(basic, ResumeField.HIGHLIGHT, "");

                this.hasPersonalEmail = true; //TODO: no data support
                this.needSponsorship = false; //TODO: no data support

                this.availability = JsonResumeParseUtils.getIntFieldFromJsonNode(analytics, ResumeField.AVAILABILITY);

                this.divWoman = ResumeField.FEMALE.equals(JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.GENDER, ""));

                if (JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.RACE)) {
                    String race = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.RACE, "");
                    this.divHispanic = race.contains(ResumeField.HISPANIC);
                    this.divAsian = race.contains(ResumeField.ASIAN);
                    this.divBlack = race.contains(ResumeField.BLACK) || race.contains(ResumeField.AFRICAN);
                    this.divNative = race.contains(ResumeField.NATIVE);
                }

                this.divVeteran = JsonResumeParseUtils.getBoolFieldFromJsonNode(analytics, ResumeField.VETERAN);
                this.degree = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.EDUCATION);
                this.yoe = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.EXPERIENCE);

                if (JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.SKILL)) {
                    JsonNode skills = analytics.get(ResumeField.SKILL);
                    for (JsonNode skill : skills) {
                        skillInfo.append(skill.asText()).append(',');
                    }
                }

                if (JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.INDUSTRY)) {
                    this.industries = new HashSet<>();
                    JsonNode industries = analytics.get(ResumeField.INDUSTRY);
                    for (JsonNode industry : industries) {
                        this.industries.add(industry.asText());
                    }
                }

                if (JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.NORM_LOCATION)) {
                    JsonNode loc = analytics.get(ResumeField.NORM_LOCATION);
                    if (!loc.isEmpty()) {
                        this.locRaw = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.LOCATION_FMT, "");
                        this.locContinent = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.CONTINENT, "");
                        this.locNation = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.COUNTRY, "");
                        this.locState = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.STATE, "");
                        this.locCity = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.CITY, "");
                        this.locLat = JsonResumeParseUtils.getIntFieldFromJsonNode(loc, ResumeField.LATITUDE);
                        this.locLon = JsonResumeParseUtils.getIntFieldFromJsonNode(loc, ResumeField.LONGITUDE);
                    }
                }

                if (JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.IT_ANALYTICS)) {
                    JsonNode it = analytics.get(ResumeField.IT_ANALYTICS);
                    if (JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.LANGUAGE)) {
                        for (JsonNode language : it.get(ResumeField.LANGUAGE)) {
                            itInfo.append(language.asText()).append(',');
                        }
                    }
                    if (JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.EXPERTISE_SCORES)) {
                        Iterator<String> keys = it.get(ResumeField.EXPERTISE_SCORES).fieldNames();
                        while (keys.hasNext()) {
                            itInfo.append(keys.next()).append(',');
                        }
                    }
                    if (JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.DETAILS)) {
                        JsonNode details = it.get(ResumeField.DETAILS);
                        if (JsonResumeParseUtils.isJsonNodeNull(details, ResumeField.GITHUB)) {
                            JsonNode github = details.get(ResumeField.GITHUB);
                            if (JsonResumeParseUtils.isJsonNodeNull(github, ResumeField.SELECTED_REPO)) {
                                for (JsonNode repo : github.get(ResumeField.SELECTED_REPO)) {
                                    if (JsonResumeParseUtils.isJsonNodeNull(repo, ResumeField.EXPERTISE)) {
                                        Iterator<String> expertises = repo.get(ResumeField.EXPERTISE).fieldNames();
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
            if (JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.EDUCATION)) {
                for (JsonNode education : jsonNode.get(ResumeField.EDUCATION)) {
                    // //TODO: dedup work may be needed
                    educationInfo.append(education.get("education_school").asText()).append(',')
                            .append(education.get("education_major").asText()).append(',')
                            .append(education.get("education_degree").asText()).append(',')
                            .append(education.get("education_degree_level").asText()).append(',')
                            .append(education.get("education_description").asText()).append(',');
                    if (education.has("normed_education_major")) {
                        for (JsonNode normalizedMajor : education.get("normed_education_major")) {
                            educationInfo.append(normalizedMajor.asText()).append(',');
                        }
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
                    String companyId = position.has("company_id") ? position.get("company_id").asText() : "";
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
                    if (position.has("normed_position_title")) {
                        for (JsonNode normalizedTitle : position.get("normed_position_title")) {
                            positionInfo.append(normalizedTitle.asText()).append(',');
                        }
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
            } else {
                logger.warn("no embedding " + this.getUid());
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
