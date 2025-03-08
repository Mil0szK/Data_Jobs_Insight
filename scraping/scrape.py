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


def safe_get_url(driver, phrase, location, exp_level=None, geo_id=None, work_mode=None, start=0, retries=3, timeout=180):
    """Attempts to load a LinkedIn URL with retries in case of timeout or blockage"""

    def get_url(searched_phrase, location, exp_level=None, geo_id=None, work_mode=None, start=0):
        if geo_id:
            template = 'https://www.linkedin.com/jobs/search/?geoId={}&keywords={}&{}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(geo_id, searched_phrase.replace(' ', '%20'), exp_level or "", work_mode or "", start)
        else:
            template = 'https://www.linkedin.com/jobs/search/?keywords={}&location={}&{}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(searched_phrase.replace(' ', '%20'), location.replace(' ', '%20'), exp_level or "",
                                   work_mode or "", start)

    url = get_url(phrase, location=location, exp_level=exp_level, geo_id=geo_id, work_mode=work_mode, start=start)

    for attempt in range(retries):
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            time.sleep(random.uniform(3, 8))  # Random delay to avoid detection

            if "login" in driver.current_url or "captcha" in driver.page_source.lower():
                print("LinkedIn is blocking the bot! Trying to re-login...")
                login_once_to_linkedin(driver)
                time.sleep(5)

            print(f"Successfully loaded: {url}")
            return driver.page_source
        except Exception as e:
            print(f"Error loading {url}: {e}")
            time.sleep(5)

    print(f"Failed to load {url} after {retries} attempts.")
    return None

def login_once_to_linkedin(driver):
    """Login to LinkedIn once per session"""
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    actions.login(driver, email, password)
    time.sleep(5)  # Allow time for login
    print("Logged in successfully!")

def scroll_down(driver):
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

def extract_job_count(soup):
    """Extract the number of job listings from the search page."""
    results_text = soup.find("div", class_="jobs-search-results-list__subtitle")
    if results_text:
        try:
            return int(results_text.get_text(strip=True).split()[0].replace(',', ''))
        except ValueError:
            return 0
    return 0


def scrape_jobs_from_page(page_source):
    """Extract job listings from a LinkedIn job search results page."""
    soup = BeautifulSoup(page_source, 'html.parser')
    job_cards = soup.find_all("div", attrs={"data-job-id": True})

    scraped_jobs = []
    for job in job_cards:
        try:
            job_title_tag = job.find("a", attrs={"aria-label": True})
            job_title = job_title_tag.find("span", {"aria-hidden": "true"}).get_text(
                strip=True) if job_title_tag else "Unknown"

            location_ul = job.find("ul", class_=lambda x: x and "metadata-wrapper" in x)
            location_span = location_ul.find("span") if location_ul else None
            location = location_span.get_text(strip=True) if location_span else "Unknown"

            company_div = job.find("div", class_=lambda x: x and "subtitle" in x)
            company_span = company_div.find("span") if company_div else None
            company_name = company_span.get_text(strip=True) if company_span else "Unknown"

            job_id = job.get("data-job-id")

            if job_id and job_title != "Unknown" and company_name != "Unknown":
                job_data = {
                    "job_id": job_id,
                    "job_title": job_title,
                    "company": company_name,
                    "location": location
                }
                scraped_jobs.append(job_data)
                print(f"Scraped: {job_data}")
        except Exception as e:
            print("Error scraping job:", e)

    return scraped_jobs

def scrape_linkedin_jobs(location, searched_phrase):
    driver = configure_driver()
    login_once_to_linkedin(driver)

    experience_levels = ["f_E=1", "f_E=2", "f_E=3", "f_E=4", "f_E=5", "f_E=6"]
    work_modes = ["f_WT=1", "f_WT=3"] # without remote "f_WT=2"
    phrases = [
        "Internship", "Python", "SQL", "C%2B%2B", "C%23", "C", "Java", "JavaScript", "PHP", "Ruby", "Swift",
        "TypeScript", "Kotlin", "Go", "Rust", "Scala", "HTML", "CSS", "React", "Angular", "Vue", "Node.js",
        "Django", "Flask", "Spring", "Laravel", "Express", "Ruby on Rails", "ASP.NET", "jQuery", "Bootstrap",
        "Git", "Docker", "Kubernetes", "AWS", "Azure", "Google Cloud", "Linux", "Windows", "iOS", "Android",
    ]
    geo_id = get_geo_id(driver, location)

    all_jobs = []
    safe_get_url(driver, "", location=location,  geo_id=geo_id, start=0)
    time.sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    results_text = soup.find("div", class_="jobs-search-results-list__subtitle")
    job_count = int(results_text.get_text(strip=True).split()[0].replace(',', '')) if results_text else 0
    max_pages = job_count // 25
    print(f"🔹 Total job results: {job_count} | Max Pages: {max_pages}")


    for phrase in phrases:
        print(f"Searching jobs for: {phrase}")

        for work_mode in work_modes:
            print(f"Work Mode: {work_mode if work_mode else 'All'}")

            page_source = safe_get_url(driver, phrase, location=location, geo_id=geo_id, work_mode=work_mode, start=0)
            if not page_source:
                continue
            soup = BeautifulSoup(page_source, 'html.parser')
            job_count = extract_job_count(soup)

            if job_count == 0:
                print(f"❌ No jobs found for {phrase} in {location}. Skipping.")
                continue

            if job_count > 0 and job_count < 1000:
                print(f"{phrase} has <1000 jobs, scraping ALL without experience filters")
                experience_levels_to_use = [None]
            else:
                print(f"{phrase} has {job_count} jobs, applying experience level filters")
                experience_levels_to_use = experience_levels

            for exp_level in experience_levels_to_use:
                print(f"Searching: {phrase} ({exp_level if exp_level else 'All Levels'})")

                page_source = safe_get_url(driver=driver, phrase=phrase, location=location, exp_level=exp_level, geo_id=geo_id, work_mode=work_mode, start=0)
                if not page_source:
                    continue

                soup = BeautifulSoup(page_source, 'html.parser')
                job_count_final = extract_job_count(soup)
                if job_count_final == 0:
                    print(f"No jobs found for {phrase} in {location}. Skipping.")
                    continue

                for page in range(0, min(job_count, 1000), 25):  # LinkedIn limits results to 1000
                    page_jobs = []
                    print(f"Scraping page {page // 25 + 1}")

                    page_source = safe_get_url(driver, phrase, location, exp_level=exp_level, geo_id=geo_id,
                                               work_mode=work_mode, start=page)
                    if not page_source:
                        continue

                    scroll_down(driver)
                    page_source = driver.page_source

                    jobs = scrape_jobs_from_page(page_source)
                    all_jobs.extend(jobs)

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


def scrape_from_url(driver, url):
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

    # "Cracow" - already scraped
    cities = ["Szczecin", "Bydgoszcz", "Lublin", "Białsytok", "Łódź", "Poznan", "Rzeszów", "Wrocław", "Gdańsk", "Warsaw"]


    for city in cities:
        jobs_data = scrape_linkedin_jobs(city, "")
        save_job_basic_info(jobs_data, "LinkedIn")
        print(f"Scraped {len(jobs_data)} job postings.")
