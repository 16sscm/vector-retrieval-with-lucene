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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Resume {
    private static final Logger logger = LoggerFactory.getLogger(Resume.class);

    private static final Pattern pattern = Pattern.compile("\\d{4}");

    private String uid;
    // use for ranking
    private int availability;
    // always display the candidates whose personal email are stored first
    private boolean hasPersonalEmail;
    private boolean hasContact;
    private boolean needSponsorship;
    private Set<String> eduDegrees;
    private Set<String> eduLevels;
    private Set<String> eduBusinessAdmLevels;
    private Set<String> eduMajors;
    private Set<String> eduSchoolNames;
    private Set<String> eduSchoolIds;
    private int itRankLevel;
    private String yoe;
    private String seniority;
    private Set<String> languages;
    private int eduGradYear;
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
    // company fields
    private String companyCurrent;
    private String companyIdCurrent;
    private Set<String> companySizeCurrent;
    private Set<String> companiesPast;
    private Set<String> companyIdsPast;
    private Set<String> industries;
    // for must_not filters
    private Set<String> titlesCurrent;
    private Set<String> normedTitlesCurrent;
    private Set<String> titlesPast;
    private Set<String> normedTitlesPast;
    private Set<String> normedSkills;
    private Set<String> reviewedSkills;
    // location
    private String locRaw;
    private String locFmt;
    private String locType;
    private String locContinent;
    private String locNation;
    private String locState;
    private String locCity;
    private double locLat;
    private double locLon;
    // user certifications, titles of current and past positions, personal highlight,
    // languages, reviewed skills, security_clearance(absence)
    private String titleSkill;
    // user certifications,
    // company names, summaries and titles of current and past positions,
    // degree, description, majors and school names of educations
    // personal highlight,
    // expertise, languages and skills for info_tech,
    // languages, normalized skills,
    // title, issuer and description of patents,
    // title and description of projects,
    // title, organization and description of publications
    private String compoundInfo;
    private float[] embedding;

    public Resume(String uid,
                  int availability,
                  boolean hasPersonalEmail,
                  boolean hasContact,
                  boolean needSponsorship,
                  Set<String> eduDegrees,
                  Set<String> eduLevels,
                  Set<String> eduBusinessAdmLevels,
                  Set<String> eduMajors,
                  Set<String> eduSchoolNames,
                  Set<String> eduSchoolIds,
                  int itRankLevel,
                  String yoe,
                  String seniority,
                  Set<String> languages,
                  int eduGradYear,
                  int monthsCurrentCompany,
                  int monthsCurrentRole,
                  boolean divWoman,
                  boolean divBlack,
                  boolean divHispanic,
                  boolean divVeteran,
                  boolean divNative,
                  boolean divAsian,
                  String companyCurrent,
                  String companyIdCurrent,
                  Set<String> companySizeCurrent,
                  Set<String> companiesPast,
                  Set<String> companyIdsPast,
                  Set<String> industries,
                  Set<String> titlesCurrent,
                  Set<String> normedTitlesCurrent,
                  Set<String> titlesPast,
                  Set<String> normedTitlesPast,
                  Set<String> normedSkills,
                  Set<String> reviewedSkills,
                  String locRaw,
                  String locFmt,
                  String locType,
                  String locContinent,
                  String locNation,
                  String locState,
                  String locCity,
                  double locLat,
                  double locLon,
                  String titleSkill,
                  String compoundInfo,
                  float[] embedding) {
        this.uid = uid;
        this.availability = availability;
        this.hasPersonalEmail = hasPersonalEmail;
        this.hasContact = hasContact;
        this.needSponsorship = needSponsorship;
        this.eduDegrees = eduDegrees;
        this.eduLevels = eduLevels;
        this.eduBusinessAdmLevels = eduBusinessAdmLevels;
        this.eduMajors = eduMajors;
        this.eduSchoolNames = eduSchoolNames;
        this.eduSchoolIds = eduSchoolIds;
        this.itRankLevel = itRankLevel;
        this.yoe = yoe;
        this.seniority = seniority;
        this.languages = languages;
        this.eduGradYear = eduGradYear;
        this.monthsCurrentCompany = monthsCurrentCompany;
        this.monthsCurrentRole = monthsCurrentRole;
        this.divWoman = divWoman;
        this.divBlack = divBlack;
        this.divHispanic = divHispanic;
        this.divVeteran = divVeteran;
        this.divNative = divNative;
        this.divAsian = divAsian;
        this.companyCurrent = companyCurrent;
        this.companyIdCurrent = companyIdCurrent;
        this.companySizeCurrent = companySizeCurrent;
        this.companiesPast = companiesPast;
        this.companyIdsPast = companyIdsPast;
        this.industries = industries;
        this.titlesCurrent = titlesCurrent;
        this.normedTitlesCurrent = normedTitlesCurrent;
        this.titlesPast = titlesPast;
        this.normedTitlesPast = normedTitlesPast;
        this.normedSkills = normedSkills;
        this.reviewedSkills = reviewedSkills;
        this.locRaw = locRaw;
        this.locFmt = locFmt;
        this.locType = locType;
        this.locContinent = locContinent;
        this.locNation = locNation;
        this.locState = locState;
        this.locCity = locCity;
        this.locLat = locLat;
        this.locLon = locLon;
        this.titleSkill = titleSkill;
        this.compoundInfo = compoundInfo;
        this.embedding = embedding;
    }

    public Resume(JsonNode jsonNode) {
        try {
            String highlight = "";
            String eduL = "";
            this.itRankLevel = -1;
            StringBuilder normedSkillInfo = new StringBuilder();
            StringBuilder reviewedSkillInfo = new StringBuilder();
            StringBuilder itInfo = new StringBuilder();
            StringBuilder clearanceInfo = new StringBuilder();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.BASIC) && !JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.ANALYTICS)) {
                JsonNode basic = jsonNode.get(ResumeField.BASIC);
                JsonNode analytics = jsonNode.get(ResumeField.ANALYTICS);

                this.uid = JsonResumeParseUtils.getStringFieldFromJsonNode(basic, ResumeField.UID);

                highlight = JsonResumeParseUtils.getStringFieldFromJsonNode(basic, ResumeField.HIGHLIGHT, "");

                this.hasPersonalEmail = JsonResumeParseUtils.getBoolFieldFromJsonNode(analytics, ResumeField.HAS_PERSONAL_EMAIL);
                this.hasContact = JsonResumeParseUtils.getBoolFieldFromJsonNode(analytics, ResumeField.HAS_CONTACT);
                this.needSponsorship = JsonResumeParseUtils.getBoolFieldFromJsonNode(analytics, ResumeField.NEED_VISA_SPONSORSHIP);

                this.availability = JsonResumeParseUtils.getIntFieldFromJsonNode(analytics, ResumeField.AVAILABILITY);
                this.yoe = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.EXPERIENCE, "");
                this.seniority = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.SENIORITY, "");
                eduL = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.EDUCATION, "");

                this.divWoman = ResumeField.FEMALE.equals(JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.GENDER, ""));
                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.RACE)) {
                    String race = JsonResumeParseUtils.getStringFieldFromJsonNode(analytics, ResumeField.RACE, "");
                    this.divHispanic = race.contains(ResumeField.HISPANIC);
                    this.divAsian = race.contains(ResumeField.ASIAN);
                    this.divBlack = race.contains(ResumeField.BLACK) || race.contains(ResumeField.AFRICAN);
                    this.divNative = race.contains(ResumeField.NATIVE);
                }
                this.divVeteran = JsonResumeParseUtils.getBoolFieldFromJsonNode(analytics, ResumeField.VETERAN);

                this.normedSkills = new HashSet<>();
                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.SKILL)) {
                    JsonNode skills = analytics.get(ResumeField.SKILL);
                    for (JsonNode skill : skills) {
                        this.normedSkills.add(skill.asText());
                        normedSkillInfo.append(skill.asText()).append(',');
                    }
                }
                // TODO: switch to real reviewed skills later, no data support
                this.reviewedSkills = new HashSet<>();
                if (!JsonResumeParseUtils.isJsonNodeNull(basic, ResumeField.SKILL)) {
                    JsonNode skills = basic.get(ResumeField.SKILL);
                    for (JsonNode skill : skills) {
                        this.reviewedSkills.add(skill.asText());
                        reviewedSkillInfo.append(skill.asText()).append(',');
                    }
                }

                this.industries = new HashSet<>();
                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.INDUSTRY)) {
                    JsonNode industries = analytics.get(ResumeField.INDUSTRY);
                    for (JsonNode industry : industries) {
                        this.industries.add(industry.asText());
                    }
                }

                this.companySizeCurrent = new HashSet<>();
                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.POSITION_CURRENT)) {
                    JsonNode currentPosition = analytics.get(ResumeField.POSITION_CURRENT);
                    if (!currentPosition.isEmpty() && !JsonResumeParseUtils.isJsonNodeNull(currentPosition, ResumeField.POSITION_COMPANY_SIZE)) {
                        for (JsonNode companySize : currentPosition.get(ResumeField.POSITION_COMPANY_SIZE)) {
                            this.companySizeCurrent.add(companySize.asText());
                        }
                    }
                }

                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.NORM_LOCATION)) {
                    JsonNode loc = analytics.get(ResumeField.NORM_LOCATION);
                    if (!loc.isEmpty()) {
                        this.locRaw = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.LOCATION_RAW, "");
                        this.locFmt = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.LOCATION_FMT, "");
                        this.locType = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.LOCATION_TYPE, "");
                        this.locContinent = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.CONTINENT, "");
                        this.locNation = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.COUNTRY, "");
                        this.locState = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.STATE, "");
                        this.locCity = JsonResumeParseUtils.getStringFieldFromJsonNode(loc, ResumeField.CITY, "");
                        this.locLat = JsonResumeParseUtils.getIntFieldFromJsonNode(loc, ResumeField.LATITUDE);
                        this.locLon = JsonResumeParseUtils.getIntFieldFromJsonNode(loc, ResumeField.LONGITUDE);
                    }
                }

                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.SECURITY_CLEARANCE)) {
                    JsonNode clearance = analytics.get(ResumeField.SECURITY_CLEARANCE);
                    Iterator<Map.Entry<String, JsonNode>> fields = clearance.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fields.next();
                        clearanceInfo.append(entry.getKey()).append(',');
                        for (JsonNode clearanceValue : entry.getValue()) {
                            clearanceInfo.append(clearanceValue.asText()).append(',');
                        }
                    }
                }

                if (!JsonResumeParseUtils.isJsonNodeNull(analytics, ResumeField.IT_ANALYTICS)) {
                    JsonNode it = analytics.get(ResumeField.IT_ANALYTICS);
                    if (it.hasNonNull(ResumeField.IT_ANALYTICS_RANK_LEVEL)) {
                        this.itRankLevel = JsonResumeParseUtils.getIntFieldFromJsonNode(it, ResumeField.IT_ANALYTICS_RANK_LEVEL);
                    }
                    if (!JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.LANGUAGE)) {
                        for (JsonNode language : it.get(ResumeField.LANGUAGE)) {
                            itInfo.append(language.asText()).append(',');
                        }
                    }
                    if (!JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.EXPERTISE_SCORES)) {
                        Iterator<String> keys = it.get(ResumeField.EXPERTISE_SCORES).fieldNames();
                        while (keys.hasNext()) {
                            itInfo.append(keys.next()).append(',');
                        }
                    }
                    if (!JsonResumeParseUtils.isJsonNodeNull(it, ResumeField.DETAILS)) {
                        JsonNode details = it.get(ResumeField.DETAILS);
                        if (!JsonResumeParseUtils.isJsonNodeNull(details, ResumeField.GITHUB)) {
                            JsonNode github = details.get(ResumeField.GITHUB);
                            if (!JsonResumeParseUtils.isJsonNodeNull(github, ResumeField.SELECTED_REPO)) {
                                for (JsonNode repo : github.get(ResumeField.SELECTED_REPO)) {
                                    if (!JsonResumeParseUtils.isJsonNodeNull(repo, ResumeField.EXPERTISE)) {
                                        Iterator<String> expertises = repo.get(ResumeField.EXPERTISE).fieldNames();
                                        while (expertises.hasNext()) {
                                            itInfo.append(expertises.next()).append(',');
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    this.itRankLevel = -1;
                }
            } else {
                logger.error("important fields missing: " + jsonNode.toString());
                return;
            }

            this.eduDegrees = new HashSet<>();
            this.eduLevels = new HashSet<>();
            this.eduBusinessAdmLevels = new HashSet<>(); // TODO: no data support
            this.eduMajors = new HashSet<>();
            this.eduSchoolNames = new HashSet<>();
            this.eduSchoolIds = new HashSet<>();
            Set<String> eduDescriptions = new HashSet<>();
            this.eduGradYear = -1;
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.EDUCATION)) {
                JsonNode educations = jsonNode.get(ResumeField.EDUCATION);
                if (!educations.isEmpty()) {
                    boolean isFirst = true;
                    for (JsonNode edu : educations) {
                        String degree = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_DEGREE, "");
                        if (!StringUtils.isEmpty(degree)) {
                            this.eduDegrees.add(degree);
                        }
                        String level = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_LEVEL, "");
                        if (!StringUtils.isEmpty(level)) {
                            this.eduLevels.add(level);
                        }
                        String major = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_MAJOR, "");
                        if (!StringUtils.isEmpty(major)) {
                            this.eduMajors.add(major);
                        }
                        String schoolName = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_SCHOOL_NAME, "");
                        if (!StringUtils.isEmpty(schoolName)) {
                            this.eduSchoolNames.add(schoolName);
                        }
                        String description = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_DESCRIPTION, "");
                        if (!StringUtils.isEmpty(description)) {
                            eduDescriptions.add(description);
                        }
                        if (edu.hasNonNull(ResumeField.EDUCATION_SCHOOL_ID))  {
                            for (JsonNode schoolId : edu.get(ResumeField.EDUCATION_SCHOOL_ID)) {
                                this.eduSchoolIds.add(schoolId.asText());
                            }
                        }
                        if (isFirst) {
                            String gradYear = JsonResumeParseUtils.getStringFieldFromJsonNode(edu, ResumeField.EDUCATION_GRAD_YEAR, "").trim();
                            if (!StringUtils.isEmpty(gradYear) && !gradYear.equals("present")) {
                                try {
                                    Matcher matcher = pattern.matcher(gradYear);
                                    if (matcher.find()) {
                                        this.eduGradYear = Integer.parseInt(matcher.group());
                                    }
                                } catch (Exception e) {
                                    logger.warn("fail to parse grad year to int|" + gradYear);
                                }
                            }
                        }
                        isFirst = false;
                    }
                }
            }
            if (!StringUtils.isEmpty(eduL)) {
                this.eduLevels.add(eduL);
            }

            String educationInfo = String.join(",", String.join(",", this.eduDegrees),
                    String.join(",", this.eduMajors), String.join(",", this.eduSchoolNames), String.join(",", eduDescriptions));

            this.companiesPast = new HashSet<>();
            this.companyIdsPast = new HashSet<>();
            this.titlesCurrent = new HashSet<>();
            this.normedTitlesCurrent = new HashSet<>();
            this.titlesPast = new HashSet<>();
            this.normedTitlesPast = new HashSet<>();
            Set<String> positionSummaries = new HashSet<>();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.POSITION)) {
                JsonNode positions = jsonNode.get(ResumeField.POSITION);
                if (!positions.isEmpty()) {
                    Map<String, Integer> map = new HashMap<>();
                    int k = 0;
                    int ccindex = Integer.MIN_VALUE;
                    for (JsonNode position : positions) {
                        String companyName = JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_COMPANY_NAME, "");
                        String companyId = JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_COMPANY_ID, "");
                        String summary = JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_SUMMARY, "");
                        String title = JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_TITLE, "");
                        Set<String> nTitles = new HashSet<>();
                        if (position.hasNonNull(ResumeField.POSITION_NORMED_TITLE)) {
                            for (JsonNode nTitle : position.get(ResumeField.POSITION_NORMED_TITLE)) {
                                nTitles.add(nTitle.asText());
                            }
                        }

                        Boolean isCurrent = JsonResumeParseUtils.getBoolFieldFromJsonNode(position, ResumeField.POSITION_IS_CURRENT);
                        if (isCurrent && StringUtils.isEmpty(this.companyCurrent)) {
                            this.companyCurrent = companyName;
                            this.companyIdCurrent = companyId;
                            if (!StringUtils.isEmpty(title)) {
                                this.titlesCurrent.add(title);
                            }
                            this.normedTitlesCurrent.addAll(nTitles);
                            this.monthsCurrentRole = DateUtils.getMonthsFromDate(JsonResumeParseUtils.getStringFieldFromJsonNode(position,
                                    ResumeField.POSITION_START_DATE, ""));
                            ccindex = k;
                        } else if (isCurrent) {
                            //TODO: some data seem wrong, a candidate has multiple current positions, should ask data team later
                            if (!StringUtils.isEmpty(companyName) && !companyName.equals(this.companyCurrent)) {
                                this.companiesPast.add(companyName);
                                if (!StringUtils.isEmpty(companyId)) {
                                    this.companyIdsPast.add(companyId);
                                }
                            }

                            if (!StringUtils.isEmpty(title)) {
                                this.titlesPast.add(title);
                            }
                            this.normedTitlesPast.addAll(nTitles);
                        } else {
                            if (!StringUtils.isEmpty(companyName)) {
                                if ((k - ccindex) == 1 && companyName.equals(this.companyCurrent)) {
                                    ccindex = k;
                                } else {
                                    this.companiesPast.add(companyName);
                                    if (!StringUtils.isEmpty(companyId)) {
                                        this.companyIdsPast.add(companyId);
                                    }
                                }
                            }

                            if (!StringUtils.isEmpty(title)) {
                                this.titlesPast.add(title);
                            }
                            this.normedTitlesPast.addAll(nTitles);
                        }

                        if (!StringUtils.isEmpty(summary)) {
                            positionSummaries.add(summary);
                        }

                        int offset = 0;
                        if (map.containsKey(companyName)) {
                            offset = map.get(companyName);
                        }
                        map.put(companyName, offset + DateUtils.getMonthsFromDate(
                                JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_START_DATE, ""),
                                JsonResumeParseUtils.getStringFieldFromJsonNode(position, ResumeField.POSITION_END_DATE, "")));
                        k++;
                    }
                    if (!StringUtils.isEmpty(this.companyCurrent) && map.containsKey(this.companyCurrent)) {
                        this.monthsCurrentCompany = map.get(this.companyCurrent);
                    }
                }
            }
            if (this.companyCurrent == null) {
                this.companyCurrent = "";
            }
            if (this.companyIdCurrent == null) {
                this.companyIdCurrent = "";
            }
            String positionInfo = String.join(",", this.companyCurrent, String.join(",", this.companiesPast),
                    String.join(",", positionSummaries), String.join(",", this.titlesCurrent), String.join(",", this.titlesPast));

            this.languages = new HashSet<>();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.LANGUAGE)) {
                for (JsonNode language : jsonNode.get(ResumeField.LANGUAGE)) {
                    String languageStr = JsonResumeParseUtils.getStringFieldFromJsonNode(language, ResumeField.LANGUAGE_NAME, "");
                    if (!StringUtils.isEmpty(languageStr)) {
                        this.languages.add(languageStr);
                    }
                }
            }

            StringBuilder certInfo = new StringBuilder();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.CERTIFICATION)) {
                for (JsonNode certification : jsonNode.get(ResumeField.CERTIFICATION)) {
                    certInfo.append(JsonResumeParseUtils.getStringFieldFromJsonNode(certification, ResumeField.CERTIFICATION_NAME, "")).append(',')
                            .append(JsonResumeParseUtils.getStringFieldFromJsonNode(certification, ResumeField.CERTIFICATION_AUTHORITY, "")).append(',');
                }
            }

            StringBuilder patentInfo = new StringBuilder();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.PATENT)) {
                for (JsonNode patent : jsonNode.get(ResumeField.PATENT)) {
                    patentInfo.append(JsonResumeParseUtils.getStringFieldFromJsonNode(patent, ResumeField.PATENT_TITLE, "")).append(',')
                              .append(JsonResumeParseUtils.getStringFieldFromJsonNode(patent, ResumeField.PATENT_ISSUER, "")).append(',')
                              .append(JsonResumeParseUtils.getStringFieldFromJsonNode(patent, ResumeField.PATENT_DESCRIPTION, "")).append(',');
                }
            }

            StringBuilder projectInfo = new StringBuilder();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.PROJECT)) {
                for (JsonNode project : jsonNode.get(ResumeField.PROJECT)) {
                    projectInfo.append(JsonResumeParseUtils.getStringFieldFromJsonNode(project, ResumeField.PROJECT_TITLE, "")).append(',')
                               .append(JsonResumeParseUtils.getStringFieldFromJsonNode(project, ResumeField.PROJECT_SUMMARY, "")).append(',');
                }
            }

            StringBuilder publicationInfo = new StringBuilder();
            if (!JsonResumeParseUtils.isJsonNodeNull(jsonNode, ResumeField.PUBLICATION)) {
                for (JsonNode publication : jsonNode.get(ResumeField.PUBLICATION)) {
                    publicationInfo.append(JsonResumeParseUtils.getStringFieldFromJsonNode(publication, ResumeField.PUBLICATION_TITLE, "")).append(',')
                                   .append(JsonResumeParseUtils.getStringFieldFromJsonNode(publication, ResumeField.PUBLICATION_ORGANIZATION, "")).append(',')
                                   .append(JsonResumeParseUtils.getStringFieldFromJsonNode(publication, ResumeField.PUBLICATION_DESCRIPTION, "")).append(',');
                }
            }

            this.titleSkill = String.join(",", certInfo, highlight, String.join(",", this.languages), clearanceInfo,
                    String.join(",", this.titlesCurrent), String.join(",", this.titlesPast), String.join(",", this.reviewedSkills));
            this.compoundInfo = String.join(",",certInfo, highlight, positionInfo, educationInfo, itInfo,
                    String.join(",", this.languages), patentInfo, projectInfo, publicationInfo, String.join(",", this.normedSkills));

            // if (jsonNode.has("embedding")) {
            //     this.embedding = new float[IndexBuildService.embeddingDimension];
            //     Iterator<JsonNode> arrayIterator = jsonNode.get("embedding").iterator();
            //     int i = 0;
            //     while(arrayIterator.hasNext() && i < this.embedding.length) {
            //         this.embedding[i] = (float) arrayIterator.next().asDouble();
            //         i++;
            //     }
            // } else {
            //     logger.warn("no embedding " + this.getUid());
            // }
        } catch (Exception e) {
            logger.error("fail to convert json to common parameter, input: " + jsonNode, e);
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

    public String getYoe() {
        return yoe;
    }

    public void setYoe(String yoe) {
        this.yoe = yoe;
    }

    public String getSeniority() {
        return seniority;
    }

    public void setSeniority(String seniority) {
        this.seniority = seniority;
    }

    public Set<String> getLanguages() {
        return languages;
    }

    public void setLanguages(Set<String> languages) {
        this.languages = languages;
    }

    public int getEduGradYear() {
        return eduGradYear;
    }

    public void setEduGradYear(int eduGradYear) {
        this.eduGradYear = eduGradYear;
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

    public boolean isHasContact() {
        return hasContact;
    }

    public void setHasContact(boolean hasContact) {
        this.hasContact = hasContact;
    }

    public Set<String> getEduDegrees() {
        return eduDegrees;
    }

    public void setEduDegrees(Set<String> eduDegrees) {
        this.eduDegrees = eduDegrees;
    }

    public Set<String> getEduLevels() {
        return eduLevels;
    }

    public void setEduLevels(Set<String> eduLevels) {
        this.eduLevels = eduLevels;
    }

    public Set<String> getEduBusinessAdmLevels() {
        return eduBusinessAdmLevels;
    }

    public void setEduBusinessAdmLevels(Set<String> eduBusinessAdmLevels) {
        this.eduBusinessAdmLevels = eduBusinessAdmLevels;
    }

    public Set<String> getEduMajors() {
        return eduMajors;
    }

    public void setEduMajors(Set<String> eduMajors) {
        this.eduMajors = eduMajors;
    }

    public Set<String> getEduSchoolNames() {
        return eduSchoolNames;
    }

    public void setEduSchoolNames(Set<String> eduSchoolNames) {
        this.eduSchoolNames = eduSchoolNames;
    }

    public Set<String> getEduSchoolIds() {
        return eduSchoolIds;
    }

    public void setEduSchoolIds(Set<String> eduSchoolIds) {
        this.eduSchoolIds = eduSchoolIds;
    }

    public int getItRankLevel() {
        return itRankLevel;
    }

    public void setItRankLevel(int itRankLevel) {
        this.itRankLevel = itRankLevel;
    }

    public Set<String> getCompanySizeCurrent() {
        return companySizeCurrent;
    }

    public void setCompanySizeCurrent(Set<String> companySizeCurrent) {
        this.companySizeCurrent = companySizeCurrent;
    }

    public Set<String> getTitlesCurrent() {
        return titlesCurrent;
    }

    public void setTitlesCurrent(Set<String> titlesCurrent) {
        this.titlesCurrent = titlesCurrent;
    }

    public Set<String> getNormedTitlesCurrent() {
        return normedTitlesCurrent;
    }

    public void setNormedTitlesCurrent(Set<String> normedTitlesCurrent) {
        this.normedTitlesCurrent = normedTitlesCurrent;
    }

    public Set<String> getTitlesPast() {
        return titlesPast;
    }

    public void setTitlesPast(Set<String> titlesPast) {
        this.titlesPast = titlesPast;
    }

    public Set<String> getNormedTitlesPast() {
        return normedTitlesPast;
    }

    public void setNormedTitlesPast(Set<String> normedTitlesPast) {
        this.normedTitlesPast = normedTitlesPast;
    }

    public Set<String> getNormedSkills() {
        return normedSkills;
    }

    public void setNormedSkills(Set<String> normedSkills) {
        this.normedSkills = normedSkills;
    }

    public Set<String> getReviewedSkills() {
        return reviewedSkills;
    }

    public void setReviewedSkills(Set<String> reviewedSkills) {
        this.reviewedSkills = reviewedSkills;
    }

    public String getLocFmt() {
        return locFmt;
    }

    public void setLocFmt(String locFmt) {
        this.locFmt = locFmt;
    }

    public String getLocType() {
        return locType;
    }

    public void setLocType(String locType) {
        this.locType = locType;
    }

    public String getTitleSkill() {
        return titleSkill;
    }

    public void setTitleSkill(String titleSkill) {
        this.titleSkill = titleSkill;
    }

    @Override
    public String toString() {
        return "uid:" + uid + "|availability:" + availability + "|hasPersonalEmail:" + hasPersonalEmail + "|hasContact:" + hasContact
                + "|needSponsorship:" + needSponsorship + "|eduDegrees:" + String.join(",", this.eduDegrees)
                + "|eduLevels:" + String.join(",", this.eduLevels) + "|eduBusinessAdmLevels:" + String.join(",", this.eduBusinessAdmLevels)
                + "|eduMajors:" + String.join(",", this.eduMajors) + "|eduSchoolNames:" + String.join(",", this.eduSchoolNames)
                + "|eduSchoolIds:" + String.join(",", this.eduSchoolIds) + "|itRankLevel:" + itRankLevel + "|yoe:" + yoe
                + "|seniority:" + seniority + "|languages:" + String.join(",", this.languages) + "|eduGradYear:" + eduGradYear
                + "|monthsCurrentCompany:" + monthsCurrentCompany + "|monthsCurrentRole:" + monthsCurrentRole + "|divWoman:" + divWoman
                + "|divBlack:" + divBlack + "|divHispanic:" + divHispanic + "|divAsian:" + divAsian + "|divNative:" + divNative + "|divVeteran:" + divVeteran
                + "|companyCurrent:" + companyCurrent + "|companyIdCurrent:" + companyIdCurrent + "|companySizeCurrent:" + companySizeCurrent
                + "|companiesPast:" + String.join(",", companiesPast) + "|companyIdsPast:" + String.join(",", companyIdsPast)
                + "|industries:" + String.join(",", industries) + "|titlesCurrent:" + String.join(",", titlesCurrent)
                + "|normedTitlesCurrent:" + String.join(",", normedTitlesCurrent) + "|titlesPast:" + String.join(",", titlesPast)
                + "|normedTitlesPast:" + String.join(",", normedTitlesPast) + "|normedSkills:" + String.join(",", normedSkills)
                + "|reviewedSkills:" + String.join(",", reviewedSkills) + "|locRaw:" + locRaw + "|locFmt:" + locFmt + "|locType:" + locType
                + "|locContinent:" + locContinent + "|locNation:" + locNation + "|locState:" + locState + "|locCity:" + locCity + "|locLat:"+ locLat
                + "|locLon:" + locLon + "|titleSkill:" + titleSkill + "|compoundInfo:" + compoundInfo;
    }

//    public static void main(String[] args) {
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            File file = new File("/Users/jetyang/resume_sample");
//            if (file.isDirectory()) {
//                File[] filelist = file.listFiles();
//                for (int i = 0; i < filelist.length; i++) {
//                    String fn = filelist[i].getAbsolutePath();
//                    if (fn.endsWith("json")) {
//                        logger.info(filelist[i].getAbsolutePath());
//                        long t1 = System.currentTimeMillis();
//                        JsonNode jsonArray = objectMapper.readTree(filelist[i]);
//                        long t2 = System.currentTimeMillis();
//                        logger.info("json array size: " + jsonArray.size() + "|time cost: " + (t2 - t1) + " ms");
//                        int k = 0;
//                        for (JsonNode node : jsonArray) {
//                            Resume resume = new Resume(node);
//                            k++;
//                            if (k % 10000 == 0) {
//                                logger.info(node.toString());
//                                logger.info(resume.toString());
//                            }
//                        }
//                        long t3 = System.currentTimeMillis();
//                        logger.info("time cost: " + (t3 - t1) + " ms");
//                    }
//                }
//            }
//        } catch (FileNotFoundException e) {
//            logger.error("can not open directory", e);
//        } catch (JsonProcessingException e) {
//            logger.error("can not parse json string", e);
//        } catch (IOException e) {
//            logger.error("can not read json", e);
//        }
//    }
}
