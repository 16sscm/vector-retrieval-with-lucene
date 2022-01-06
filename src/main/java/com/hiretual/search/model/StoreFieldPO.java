package com.hiretual.search.model;

import java.util.Set;

public class StoreFieldPO {
    private boolean hasContact;
    private int availability;
    private boolean hasPersonalEmail;
    private Set<String> reviewedSkills;
    private  Set<String> normedSkills;
    private Set<String> titlesCurrent;
    private Set<String> normedTitlesCurrent;
    private    Set<String> titlesPast;
    private   Set<String> eduSchoolNames;
    private String yoe;
    public StoreFieldPO() {
    }
    public String getYoe() {
        return yoe;
    }
    public void setYoe(String yoe) {
        this.yoe = yoe;
    }
    public boolean isHasContact() {
        return hasContact;
    }
    public void setHasContact(boolean hasContact) {
        this.hasContact = hasContact;
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
    public Set<String> getReviewedSkills() {
        return reviewedSkills;
    }
    public void setReviewedSkills(Set<String> reviewedSkills) {
        this.reviewedSkills = reviewedSkills;
    }
    public Set<String> getNormedSkills() {
        return normedSkills;
    }
    public void setNormedSkills(Set<String> normedSkills) {
        this.normedSkills = normedSkills;
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
    public Set<String> getEduSchoolNames() {
        return eduSchoolNames;
    }
    public void setEduSchoolNames(Set<String> eduSchoolNames) {
        this.eduSchoolNames = eduSchoolNames;
    }
}
