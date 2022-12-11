select sourcing_channel,count(Patientid) as TOTAL_CUSTOMER,sum(premium) AS TOTAL_REVENUE,
avg(premium) AS AVG_REVENUE,
avg(age_in_days/365) AS AVG_AGE_IN_YEARS 
from insurancedata 
group by sourcing_channel
order by AVG_REVENUE DESC;

select sourcing_channel,count(Patientid) as TOTAL_CUSTOMER,
max(application_underwriting_score) AS Max_application_underwriting_score,
avg(application_underwriting_score) AS AVG_application_underwriting_score 
from insurancedata 
group by sourcing_channel
order by AVG_application_underwriting_score DESC;
