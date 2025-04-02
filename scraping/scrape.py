from bs4 import BeautifulSoup
from selenium import webdriver
from linkedin_scraper import JobSearch, actions, Person
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from proxyscrape import create_collector, get_collector
import atexit
import pandas as pd
import requests
import random
import re
import time
from datetime import datetime
import psycopg2
import json
import os


driver = None

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


def get_free_proxies():
    url = "https://www.free-proxy-list.net/"
    response = requests.get(url)
    proxies = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_="table table-striped table-bordered")

        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            ip = columns[0].text.strip()
            port = columns[1].text.strip()
            is_https = columns[6].text.strip()
            if is_https == "yes":
                proxies.append(f"{ip}:{port}")

    return proxies

def check_proxy(proxy):
    """Test if a proxy is working by sending a request to a test website."""
    try:
        response = requests.get("https://www.linkedin.com", proxies={"https": f"http://{proxy}"}, timeout=5)
        return response.status_code == 200
    except:
        return False


def configure_driver():
    """Setup Selenium WebDriver with Proxy & Headers"""
    try:
        chrome_binary = "/usr/bin/google-chrome-stable"
        chrome_options = Options()
        chrome_options.binary_location = chrome_binary
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1600,1200")
        chrome_options.add_argument(
            f"user-agent=Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0")

        # Add this to make ChromeDriver more stable
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        proxy_address = get_proxy()
        if proxy_address:
            print(f"Using Proxy: {proxy_address}")
            chrome_options.add_argument(f'--proxy-server={proxy_address}')
        else:
            print("No proxies available. Running without proxy.")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(180)
        return driver
    except Exception as e:
        print(f"Error configuring driver: {e}")
        time.sleep(5)
        return configure_driver()

def is_driver_active(driver):
    """Check if the WebDriver session is still active"""
    try:
        # A simple command that should work if the driver is active
        driver.current_url
        return True
    except:
        return False


def safe_get_url(driver, phrase, location, exp_level=None, geo_id=None, work_mode=None, start=0, retries=3, timeout=80):
    """Attempts to load a LinkedIn URL with retries in case of timeout or blockage"""

    def get_url(searched_phrase, location, exp_level=None, geo_id=None, work_mode=None, start=0):
        if geo_id:
            # f_TPR=r2592000 - last month, f_TPR=r604800 - last week
            template = 'https://www.linkedin.com/jobs/search/?geoId={}&f_TPR=r604800&keywords={}&{}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(geo_id, searched_phrase.replace(' ', '%20'), exp_level or "", work_mode or "", start)
        else:
            template = 'https://www.linkedin.com/jobs/search/?keywords={}&f_TPR=r604800&location={}&{}&{}&origin=JOB_SEARCH_PAGE_JOB_FILTER&start={}'
            return template.format(searched_phrase.replace(' ', '%20'), location.replace(' ', '%20'), exp_level or "",
                                   work_mode or "", start)

    url = get_url(phrase, location=location, exp_level=exp_level, geo_id=geo_id, work_mode=work_mode, start=start)

    if driver is None:
        driver = configure_driver()

    for attempt in range(1, retries + 1):
        try:
            driver.set_page_load_timeout(timeout)
            print("dostaje url")
            driver.get(url)
            print("po dostaniu url")
            time.sleep(random.uniform(3, 8))

            if "login" in driver.current_url or "captcha" in driver.page_source.lower():
                print("LinkedIn is blocking the bot! Trying to re-login...")
                login_once_to_linkedin(driver)
                time.sleep(5)

            print(f"✅ Successfully loaded: {url}")
            return driver.page_source

        except Exception as e:
            print(f"[Attempt {attempt}] Error loading {url}: {e}")
            time.sleep(5)

            if attempt == retries:
                print("❌ Max retries reached. Restarting WebDriver...")

                try:
                    if driver:
                        driver.quit()
                    time.sleep(5)

                    driver = configure_driver()
                    login_once_to_linkedin(driver)
                    time.sleep(5)

                except Exception as restart_error:
                    print(f"Error restarting WebDriver: {restart_error}")

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
            print("Pierwsze job section")
        except:
            try:
                job_list_section = driver.find_element(By.XPATH, "/html/body/div[6]/div[3]/div[4]/div/div/main/div/div[2]/div[1]/div")
                print("Drugie job section")
            except:
                job_list_section = driver.find_element(By.XPATH, "/html/body/div[7]/div[3]/div[4]/div/div/main/div/div[2]/div[1]")
                print("Trzecie job section")
        for _ in range(5):
            try:
                driver.execute_script("arguments[0].scrollTop += 500;", job_list_section)
                time.sleep(1)
            except Exception as e:
                print("Scrolling error during script:", e)
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

def scrape_linkedin_jobs(location):
    driver = configure_driver()
    login_once_to_linkedin(driver)

    experience_levels = ["f_E=1", "f_E=2", "f_E=3", "f_E=4", "f_E=5", "f_E=6"]
    work_modes = ["f_WT=2"] # without "f_WT=1", "f_WT=3"
    # "Software Engineer", "Web Developer", "Frontend Developer", "Backend Developer",
    #                 "Internship", "Python", "SQL", "C%2B%2B", "C%23", "C", "Java", "JavaScript", "PHP", "Ruby", "Swift",
    #                 "TypeScript", "Kotlin", "Go", "Rust", "Scala", "HTML",
    phrases = [  "CSS", "React", "Angular", "Vue", "Node.js",
                "Django", "Flask", "Spring", "Laravel", "Express", "Ruby on Rails", "ASP.NET", "jQuery", "Bootstrap",
                "Git", "Docker", "Kubernetes", "AWS", "Azure", "Google Cloud", "Linux", "Windows", "iOS", "Android",
                "Data Analyst", "Data Scientist", "Machine Learning Engineer", "Data Engineer", "Excel", "Tableau", "Power BI",
                "Microsoft Office", "Documentation Writer", "Tester", "Quality Assurance", "QA", "Software Tester",
                "Manual Tester", "Software Developer",
    ]
    geo_id = get_geo_id(driver, location)

    all_jobs = []
    # page_source = safe_get_url(driver, "", location=location,  geo_id=geo_id, start=0)
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

                for page in range(0, min(job_count_final, 1000), 25):  # LinkedIn limits results to 1000
                    page_jobs = []
                    print(f"Scraping page {page // 25 + 1}")
                    print("getting safe url")
                    page_source = safe_get_url(driver=driver, phrase=phrase, location=location, exp_level=exp_level, geo_id=geo_id,
                                               work_mode=work_mode, start=page)
                    if not page_source:
                        continue

                    print("lest's scroll")
                    scroll_down(driver)
                    time.sleep(1)
                    page_source = driver.page_source
                    print("lest's scrape jobs from page")
                    jobs = scrape_jobs_from_page(page_source)

                    all_jobs.extend(jobs)
                    save_job_basic_info(jobs)

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
    try:
        driver.get(url)
        time.sleep(4)

        try:
            button_notification = driver.find_element(By.XPATH, "/html/body/div[6]/div/div/section/button")
            button_notification.click()
            time.sleep(2)
        except:
            print("No notification button found.")
            pass

        try:
            try:
                button = driver.find_element(By.XPATH, "/html/body/main/section[1]/div/div/section[1]/div/div/section/button[1]")
                button.click()

            except:
                button2 = driver.find_element(By.XPATH,
                                              "/html/body/main/section[1]/div/div/section[2]/div/div/section/button[1]")
                button2.click()
            time.sleep(3)
        except:
            print("No 'Show more' button found.")
            pass

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        description = soup.select_one("div.show-more-less-html__markup")
        description_criteria = soup.select_one("ul.description__job-criteria-list")

        if description:
            text_parts = []

            for element in description.children:
                if element.name == "strong":
                    text_parts.append(element.get_text(strip=True) + ":")
                else:
                    text_parts.append(element.get_text(strip=True))

            full_text = " ".join(text_parts)
            if description_criteria:
                description_criteria_text = " ".join(description_criteria.text.split()).strip()
                return full_text, description_criteria_text
            return full_text, None
        else:
            print("Job description not found!")
            return None, None
    except Exception as e:
        print("Error scraping job description:", e)
        return None, None


def get_job_urls_without_description():
    """Fetch all job URLs from the database"""
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    cur.execute("""
            SELECT js.job_url 
            FROM job_sources js
            INNER JOIN job_postings jp ON js.job_id = jp.job_id
            WHERE jp.description IS NULL AND js.is_active = TRUE;
        """)  # Only fetch jobs that don't have descriptions

    job_urls = cur.fetchall()

    cur.close()
    conn.close()

    return [url[0] for url in job_urls]

def update_job_descriptions(job_url, description):
    """Update job description in the database."""
    if description is None:
        print(f"Skipping update: No description found for {job_url}")
        return

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    try:
        cur.execute("""
                UPDATE job_postings
                SET description = %s
                WHERE job_id = (SELECT job_id FROM job_sources WHERE job_url = %s);
            """, (description, job_url))
        conn.commit()
        print(f"Successfully updated description for {job_url}")
    except Exception as e:
        print(f"Database error for {job_url}: {e}")
    finally:
        cur.close()
        conn.close()


def update_job_description_criteria(job_url, description_criteria):
    """Update job description in the database."""
    if description_criteria is None:
        print(f"Skipping update: No description found for {job_url}")
        return

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    try:
        cur.execute("""
                UPDATE job_postings
                SET description_criteria = %s
                WHERE job_id = (SELECT job_id FROM job_sources WHERE job_url = %s);
            """, (description_criteria, job_url))
        conn.commit()
        print(f"Successfully updated description criteria for {job_url}")
    except Exception as e:
        print(f"Database error for {job_url}: {e}")
    finally:
        cur.close()
        conn.close()


def scrape_all_jobs():
    job_urls = get_job_urls_without_description()[300:]
    print(f"Scraping {len(job_urls)} job descriptions...")
    driver = configure_driver()
    error_count = 0

    for url in job_urls:
        description, description_criteria = scrape_from_url(driver, url)

        if description is not None:
            update_job_descriptions(url, description)
            error_count = 0
        else:
            print(f"No description found for {url}")
            error_count += 1

        if description_criteria is not None:
            update_job_description_criteria(url, description_criteria)


        if error_count > 3:
            print("Too many errors. Exiting.")
            driver.quit()
            time.sleep(10)
            driver = configure_driver()
            consecutive_errors = 0

        time.sleep(2)
    driver.quit()
    print("Finished scraping job descriptions.")


def cleanup_processes():
    """Ensures ChromeDriver and Chrome are closed on script exit."""
    global driver
    if driver:
        try:
            driver.quit()
            print("WebDriver closed successfully.")
        except:
            print("WebDriver was already closed.")

    os.system("pkill -f chromedriver")
    os.system("pkill -f chrome")
    print("🧹 Cleaned up all ChromeDriver and Chrome processes.")


if __name__ == '__main__':
    atexit.register(cleanup_processes)
    load_dotenv()
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    collector = create_collector('my-collector', 'https')

    def te_scrapowania(driver):

        timeout = 80
        url = "https://www.linkedin.com/jobs/search/?currentJobId=4183948409&f_E=3&f_TPR=r604800&f_WT=2&geoId=105072130&keywords=Software%20Engineer&origin=JOB_SEARCH_PAGE_JOB_FILTER&start=25"

        if driver is None:
            driver = configure_driver()

        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            time.sleep(5)
            scroll_down(driver)
            page_source = driver.page_source
            jobs = scrape_jobs_from_page(page_source)
            print(jobs)
            time.sleep(random.uniform(3, 8))

            if "login" in driver.current_url or "captcha" in driver.page_source.lower():
                print("LinkedIn is blocking the bot! Trying to re-login...")
                login_once_to_linkedin(driver)
                time.sleep(5)

            if driver:
                driver.quit()
            time.sleep(5)
            driver = configure_driver()
            login_once_to_linkedin(driver)
            time.sleep(5)
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            time.sleep(5)
            scroll_down(driver)
            page_source = driver.page_source
            jobs = scrape_jobs_from_page(page_source)
            print(jobs)
            time.sleep(random.uniform(3, 8))



            if driver:
                driver.quit()
            time.sleep(5)
            driver = configure_driver()
            login_once_to_linkedin(driver)
            time.sleep(5)
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            time.sleep(5)
            scroll_down(driver)
            page_source = driver.page_source
            jobs = scrape_jobs_from_page(page_source)
            print(jobs)

            time.sleep(random.uniform(3, 8))

            print(f"Successfully loaded: {url}")
            return driver.page_source

        except Exception as e:
            print(f"[Attempt Error loading {url}: {e}")
            time.sleep(5)

        print(f"Failed to load {url} after 3 attempts.")
        return None

    # driver = configure_driver()
    # login_once_to_linkedin(driver)
    # te_scrapowania(driver)

    # "Cracow" - already scraped
    # cities = ["Cracow", "Szczecin", "Bydgoszcz", "Lublin", "Białsytok", "Łódź", "Poznan", "Rzeszów", "Wrocław", "Gdańsk", "Warsaw"]


    # jobs_data = scrape_linkedin_jobs("Poland")
    # save_job_basic_info(jobs_data, "LinkedIn")
    scrape_all_jobs()
    # driver = configure_driver()
    # scrape_from_url(driver=driver, url="https://www.linkedin.com/jobs/view/4144364769")

