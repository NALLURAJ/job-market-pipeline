create database if not exists job_market;
use job_market

DROP TABLE IF EXISTS job_skills;
DROP TABLE IF EXISTS job_postings;
DROP TABLE IF EXISTS companies;

create TABLE companies(
 company_id INT primary key AUTO_INCREMENT,
 company_name varchar(255),
 industry varchar(100),
 company_size varchar(50),
 headquarters_location varchar(255),
 created_at timestamp
);

CREATE table job_postings
(
job_id int primary key auto_increment,
company_id int,
foreign key (company_id) references companies(company_id),
title varchar(255),
description text,
location varchar(255),
work_model enum('remote', 'hybrid','onsite'),
job_type enum('full_time','part_time','contract','internship'),
experience_level varchar(50),
salary_min decimal(10,2),
salary_max decimal(10,2),
salary_currency varchar(3),
source_platform varchar(50),
source_url varchar(500),
posted_date date,
scraped_date timestamp,
is_active boolean,
created_at timestamp
);


create table job_skills (
skill_id int primary key auto_increment,
job_id int,
foreign key (job_id) references job_postings(job_id),
skill_name varchar(100),
skill_type enum('technical','soft','certification'),
created_at timestamp
);
ALTER TABLE job_postings MODIFY source_platform VARCHAR(255);

DELETE FROM job_postings WHERE job_id > 0;
DELETE FROM companies WHERE company_id > 0;





