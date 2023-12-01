import requests
import os

def download_file(url, local_filename):
    with requests.get(url, stream=True) as response:
        if response.status_code == 200:
            with open(local_filename, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {local_filename}")
        else:
            print(f"Failed to download: {url}")
            print(f"Status code: {response.status_code}")

if __name__ == "__main__":
    file_url = "https://pub.data.gov.bc.ca/datasets/cdfc2d7b-c046-4bf0-90ac-4897232619e1/prot_current_fire_polys.zip"
    destination_path = "./prot_current_fire_polys.zip"
    if os.path.exists("./prot_current_fire_polys.zip"):
        os.remove("./prot_current_fire_polys.zip")
    download_file(file_url, destination_path)
