from bs4 import BeautifulSoup
from selenium import webdriver
from linkedin_scraper import JobSearch, actions, Person
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv
import pandas as pd
import requests
import random
import time
import psycopg2
import json
import os

def get_url(country, job_title, start=0):
    template = 'https://www.linkedin.com/jobs/search/?f_AL=true&keywords={}&location={}&start={}'
    if start == 0:
        template = 'https://www.linkedin.com/jobs/search/?f_AL=true&keywords={}&location={}'
    job_title = job_title.replace(' ', '%20')
    country = country.replace(' ', '%20')
    return template.format(job_title, country, start)

def scroll_down():
    try:
        time.sleep(2)
        try:
            job_list_section = driver.find_element(By.XPATH, "/html/body/div[7]/div[3]/div[4]/div/div/main/div/div[2]/div[1]/div")
        except:
            job_list_section = driver.find_element(By.XPATH, "/html/body/div[6]/div[3]/div[4]/div/div/main/div/div[2]/div[1]/div")

        for _ in range(5):
            driver.execute_script("arguments[0].scrollTop += 500;", job_list_section)
            time.sleep(1)
    except Exception as e:
        print("Scrolling error:", e)

def scrape_linkedin_jobs(country, searched_phrase, max_pages=3):
    all_jobs = []
    all_pages = input("Do you want to scrape all pages? (y/n): ")

    for page in range(0, max_pages * 25, 25):
        print(max_pages)
        url = get_url(country, searched_phrase, page)
        print(f"Scraping: {url}")

        driver.get(url)
        time.sleep(3)

        button = driver.find_element(By.ID, "searchFilter_applyWithLinkedin")
        button.click()
        time.sleep(3)

        scroll_down()

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        if page == 0:
            results_text = soup.find("div", class_="jobs-search-results-list__subtitle")
            if results_text:
                job_count = results_text.get_text(strip=True)
                if job_count:
                    print(f"Total job results: {int(job_count.split()[0])}")
                    if all_pages == 'y':
                        max_pages = int(int(job_count.split()[0])/25)

        # Find job cards
        job_cards = soup.find_all("div", class_="job-card-container")
        for job in job_cards:
            job_area = job.find('a', class_="disabled ember-view job-card-container__link VqHKjOyVafyXWkAuDoXoeHlZcwCIjIyvRuDWBUe job-card-list__title--link")
            job_title = job_area.find('span').get_text(strip=True)
            location_area = job.find('ul', class_="job-card-container__metadata-wrapper")
            location = location_area.find('span').get_text(strip=True)
            company_area = job.find('div', class_="artdeco-entity-lockup__subtitle ember-view")
            company = company_area.find('span').get_text(strip=True)
            job_id = job.get("data-job-id")

            if job_id:
                job_data = {
                    "job_id": job_id,
                    "job_title": job_title,
                    "company": company,
                    "location": location
                }
                all_jobs.append(job_data)
                print(f"Scraped: {job_data}")

        time.sleep(random.uniform(3, 6))

    driver.quit()
    return all_jobs

def save_job_basic_info(jobs_data, source="LinkedIn"):
    """Insert job IDs and URLs into the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    base_url = "https://www.linkedin.com/jobs/view/"

    for job in jobs_data:
        job_id = job["job_id"]
        job_url = f"{base_url}{job_id}"
        job_title = job["job_title"]
        company = job["company"]
        location = job["location"]

        try:
            # Insert into job_postings (Ensures `job_id` is unique)
            cur.execute("""
                            INSERT INTO job_postings (job_id, title, company, location)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT job_postings_job_id_key
                            DO UPDATE 
                            SET title = EXCLUDED.title, company = EXCLUDED.company, location = EXCLUDED.location;
                        """, (job_id, job_title, company, location))

            # Insert into job_sources
            cur.execute("""
                            INSERT INTO job_sources (job_id, source, job_url, date_posted, is_active)
                            VALUES (%s, %s, %s, CURRENT_DATE, TRUE)
                            ON CONFLICT ON CONSTRAINT job_sources_job_id_key
                            DO NOTHING;
                        """, (job_id, source, job_url))
        except Exception as e:
            print(f"Error inserting job {job_id}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("Inserted job IDs into the database.")


if __name__ == '__main__':
    load_dotenv()
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")

    actions.login(driver, email, password)
    time.sleep(3)

    jobs_data = scrape_linkedin_jobs("Poland", "Data Analyst", max_pages=3)

    save_job_basic_info(jobs_data, "LinkedIn")
    print(f"Scraped {len(jobs_data)} job postings.")