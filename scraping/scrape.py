from bs4 import BeautifulSoup
from selenium import webdriver
from linkedin_scraper import JobSearch, actions, Person
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv
from selenium.webdriver.chrome.options import Options
from proxyscrape import create_collector, get_collector
import pandas as pd
import requests
import random
import re
import time
from datetime import datetime
import psycopg2
import json
import os

def get_proxy():
    """Fetch a new proxy from ProxyScrape"""
    proxy = collector.get_proxy()
    if proxy:
        return f"{proxy.host}:{proxy.port}"
    return None

def get_geo_id(driver, location):
    search_url = f"https://www.linkedin.com/jobs/search/"
    driver.get(search_url)
    time.sleep(3)
    location_box = driver.find_element(By.XPATH, '//*[@id="jobs-search-box-location-id-ember100"]')
    location_box.clear()
    location_box.send_keys(location)

    search_button = driver.find_element(By.XPATH, "/html/body/div[7]/header/div/div/div/div[2]/button[1]")
    search_button.click()

    time.sleep(3)

    current_url = driver.current_url
    match = re.search(r"geoId=(\d+)", current_url)
    if match:
        geo_id = match.group(1)
        print("For geoId: ", geo_id)
        return geo_id
    else:
        print("geoId not found for url: ", current_url)
        return None

def configure_driver():
    """Setup Selenium WebDriver with Proxy & Headers"""
    chrome_binary = "/usr/bin/google-chrome-stable"
    chrome_options = Options()
    chrome_options.binary_location = chrome_binary
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Reduce bot detection
    # chrome_options.add_argument("--headless")  # Optional: Run in headless mode
    chrome_options.add_argument(f"user-agent=Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0")

    proxy_address = get_proxy()
    if proxy_address:
        print(f"Using Proxy: {proxy_address}")
        chrome_options.add_argument(f'--proxy-server={proxy_address}')
    else:
        print("No proxies available. Running without proxy.")

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)
    return driver

def login_once_to_linkedin(driver):
    """Login to LinkedIn once per session"""
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    actions.login(driver, email, password)
    time.sleep(5)  # Allow time for login
    print("Logged in successfully!")

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

def scrape_linkedin_jobs(driver, location, searched_phrase):

    experience_levels = ["f_E=1", "f_E=2", "f_E=3", "f_E=4", "f_E=5", "f_E=6"]
    phrases = [
        "Internship", "Python", "Java", "C++", "Data Scientist", "Data Analyst",
        "Machine Learning", "AI Engineer", "Deep Learning", "Big Data Engineer",
        "Software Engineer", "Software Developer", "Application Developer",
        "Web Developer", "Frontend Developer", "UI Developer", "UX Designer",
        "React Developer", "Angular Developer", "Vue.js Developer",
        "Backend Developer", "Node.js Developer", "Django Developer",
        "Flask Developer", "Golang Developer", "Rust Developer",
        "Full Stack Developer", "DevOps", "Cloud Engineer", "Site Reliability Engineer",
        "AWS Engineer", "Azure Engineer", "Google Cloud Engineer",
        "Cybersecurity", "Ethical Hacker", "Penetration Tester", "SOC Analyst",
        "Security Engineer", "Cloud Security", "Database", "SQL Developer",
        "BI Developer", "Power BI Developer", "Tableau Developer",
        "Administrator", "Linux Administrator", "IT Technician", "Help Desk",
        "Technical Support", "IT Administrator", "Systems Engineer",
        "Network Engineer", "IT Specialist", "IT Support",
        "Business Analyst", "Data Engineer", "Quantitative Analyst", "Risk Analyst",
        "FinTech Engineer", "Blockchain Developer", "Quant Developer", "Crypto Developer"
    ]
    geo_id = get_geo_id(driver, location)

    def get_url(searched_phrase, exp_level=None, geo_id=None, start=0):
        if geo_id:
            template = 'https://www.linkedin.com/jobs/search/?geoId={}&keywords={}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(geo_id, searched_phrase.replace(' ', '%20'), exp_level, start)
        else:
            template = 'https://www.linkedin.com/jobs/search/?keywords={}&location={}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(searched_phrase.replace(' ', '%20'), location.replace(' ', '%20'), exp_level, start)

    all_jobs = []
    driver.get(get_url("", geo_id, 0))
    time.sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    results_text = soup.find("div", class_="jobs-search-results-list__subtitle")
    job_count = int(results_text.get_text(strip=True).split()[0].replace(',', '')) if results_text else 0
    max_pages = job_count // 25
    print(f"🔹 Total job results: {job_count} | Max Pages: {max_pages}")

    from_page = int(input("Enter the starting page number: "))
    num_pages = int(input("Enter the number of pages to scrape: "))
    start_offset = (from_page - 1) * 25
    end_offset = start_offset + (num_pages * 25)

    last_page_had_jobs = True

    for phrase in phrases:
        for exp_level in experience_levels:

            try:
                driver.get(get_url(searched_phrase, exp_level ,geo_id, 0))
                time.sleep(3)
            except Exception as e:
                print("Error loading LinkedIn:", e)
                driver.quit()
                return all_jobs

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            job_count = int(results_text.get_text(strip=True).split()[0].replace(',', '')) if results_text else 0
            if job_count == 0:
                print(f"🚫 No jobs found for {phrase} with {exp_level}. Skipping.")
                continue

            for page in range(start_offset, end_offset, 25):
                page_jobs = []

                try:
                    url = get_url(phrase, exp_level, geo_id, page)
                    driver.get(url)
                    time.sleep(3)
                    print(f"Scraping: {url}")
                except Exception as e:
                    print("Error loading LinkedIn:", e)
                    driver.quit()
                    return all_jobs

                scroll_down()

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')


                # job_cards = soup.find_all("div", class_="")
                job_cards = soup.find_all("div", attrs={"data-job-id": True})
                unknown_count = 0
                # job_cards = driver.find_elements(By.XPATH, "/html/body/div[7]/div[3]/div[4]/div/div/main/div/div[2]/div[1]/div/ul/li[1]/div/div")
                for job in job_cards:
                    try:
                        # Extract Job Title
                        job_title_tag = job.find("a", attrs={"aria-label": True})
                        job_title = job_title_tag.find("span", {"aria-hidden": "true"}).get_text(
                            strip=True) if job_title_tag else "Unknown"

                        # Extract Location
                        location_ul = job.find("ul", class_=lambda x: x and "metadata-wrapper" in x)
                        location_span = location_ul.find("span") if location_ul else None
                        location = location_span.get_text(strip=True) if location_span else "Unknown"


                        # Extract Company Name
                        company_div = job.find("div", class_=lambda x: x and "subtitle" in x)
                        company_span = company_div.find("span") if company_div else None
                        company_name = company_span.get_text(strip=True) if company_span else "Unknown"

                        job_id = job.get("data-job-id")

                        if job_id:
                            if job_title == "Unknown" and company_name == "Unknown":
                                unknown_count += 1
                            else:
                                job_data = {
                                    "job_id": job_id,
                                    "job_title": job_title,
                                    "company": company_name,
                                    "location": location
                                }
                                page_jobs.append(job_data)
                                print(f"Scraped: {job_data}")
                    except Exception as e:
                        print("Error scraping job:", e)

                time.sleep(random.uniform(3, 6))

                if len(page_jobs) == 0 or unknown_count > 1:
                    print(f"⚠️ No jobs found on page {page // 25 + 1}. Moving to next phrase...")
                    last_page_had_jobs = False
                    break

                if page_jobs:
                    save_job_basic_info(page_jobs, "LinkedIn")
                    all_jobs.extend(page_jobs)

            if not last_page_had_jobs:
                break

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


def scrape_from_url(url):
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    """Scrape job information from a LinkedIn job posting URL."""
    driver.get(url)
    time.sleep(3)


    try:
        button_notification = driver.find_element(By.XPATH, "/html/body/div[6]/div/div/section/button")
        button_notification.click()
        time.sleep(2)
    except:
        print("No notification button found.")

    try:
        button = driver.find_element(By.XPATH, "/html/body/main/section[1]/div/div/section[1]/div/div/section/button[1]")
        button.click()
        time.sleep(3)
    except:
        print("No 'Show more' button found.")

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    description = soup.select_one("div.show-more-less-html__markup")
    if description:
        text_parts = []

        for element in description.children:
            if element.name == "strong":
                text_parts.append(element.get_text(strip=True) + ":")
            else:
                text_parts.append(element.get_text(strip=True))


        full_text = " ".join(text_parts)
        try:
            cur.execute("""
                UPDATE job_postings
                SET description = %s
                WHERE job_id = (SELECT job_id FROM job_sources WHERE job_url = %s);
            """, (full_text, url))
            conn.commit()
            print("Successfully updated job description.")
        except Exception as e:
            print(f"Database error: {e}")

    else:
        print("Job description not found!")



if __name__ == '__main__':
    load_dotenv()
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    collector = create_collector('my-collector', 'https')
    driver = configure_driver()
    login_once_to_linkedin(driver)


    jobs_data = scrape_linkedin_jobs(driver, "Cracow, Małopolskie, Poland", "")


    save_job_basic_info(jobs_data, "LinkedIn")
    driver.quit()
    print(f"Scraped {len(jobs_data)} job postings.")

    # scrape_from_url("https://www.linkedin.com/jobs/view/4158353862")
