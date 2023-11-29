from selenium import webdriver
import requests
import shutil
import time

def download_file(url, destination):
    # Start Chrome WebDriver
    driver = webdriver.Chrome()

    try:
        # Open the URL
        driver.get(url)

        # Wait for some time to allow the download to start (adjust as needed)
        time.sleep(5)

        # Get the download link
        download_link = driver.find_element_by_xpath('//a[contains(@href,"download")]')

        # Get the download URL
        download_url = download_link.get_attribute("href")

        # Use requests to download the file
        with requests.get(download_url, stream=True) as response:
            with open(destination, 'wb') as file:
                shutil.copyfileobj(response.raw, file)

        print(f"File downloaded to: {destination}")

    finally:
        # Close the WebDriver
        driver.quit()

if __name__ == "__main__":
    file_url = "https://pub.data.gov.bc.ca/datasets/cdfc2d7b-c046-4bf0-90ac-4897232619e1/prot_current_fire_polys.zip"
    destination_path = "prot_current_fire_polys.zip"

    download_file(file_url, destination_path)